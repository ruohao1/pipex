package frontier

import (
	"context"
	"fmt"
	"sync"
)

type MemoryStore[T any] struct {
	mu       sync.Mutex
	nextID   uint64
	inflight map[uint64]Entry[T]
	closed   bool

	pendingCh chan Entry[T]
	closedCh  chan struct{}
}

func NewMemoryStore[T any]() *MemoryStore[T] {
	return NewMemoryStoreWithCapacity[T](100)
}

func NewMemoryStoreWithCapacity[T any](pendingCapacity int) *MemoryStore[T] {
	if pendingCapacity <= 0 {
		pendingCapacity = 100
	}
	s := &MemoryStore[T]{
		nextID:    1,
		inflight:  make(map[uint64]Entry[T]),
		closed:    false,
		pendingCh: make(chan Entry[T], pendingCapacity),
		closedCh:  make(chan struct{}),
	}
	return s
}

func (s *MemoryStore[T]) Enqueue(stage string, item T, hops int) (id uint64, err error) {
	if stage == "" {
		return 0, fmt.Errorf("invalid stage name: %w", ErrInvalidStageName)
	}
	if hops < 0 {
		return 0, fmt.Errorf("invalid hops: %w", ErrInvalidHops)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrClosed
	}
	entry := Entry[T]{
		ID:      s.nextID,
		Stage:   stage,
		Input:   item,
		Hops:    hops,
		Attempt: 1,
	}

	select {
	case s.pendingCh <- entry:
		s.nextID++
		return entry.ID, nil
	default:
		return 0, ErrPendingQueueFull
	}
}

func (s *MemoryStore[T]) EnqueueWait(ctx context.Context, stage string, item T, hops int) (id uint64, err error) {
	if stage == "" {
		return 0, fmt.Errorf("invalid stage name: %w", ErrInvalidStageName)
	}
	if hops < 0 {
		return 0, fmt.Errorf("invalid hops: %w", ErrInvalidHops)
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, ErrClosed
	}
	id = s.nextID
	s.nextID++

	entry := Entry[T]{
		ID:      id,
		Stage:   stage,
		Input:   item,
		Hops:    hops,
		Attempt: 1,
	}
	s.mu.Unlock()

	select {
	case s.pendingCh <- entry:
		return id, nil
	case <-s.closedCh:
		return 0, ErrClosed
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (s *MemoryStore[T]) Reserve(ctx context.Context) (Entry[T], bool, error) {
	select {
	case <-s.closedCh:
		return Entry[T]{}, false, nil
	default:
	}

	select {
	case entry := <-s.pendingCh:
		s.mu.Lock()
		s.inflight[entry.ID] = entry
		s.mu.Unlock()
		return entry, true, nil
	case <-s.closedCh:
		return Entry[T]{}, false, nil
	case <-ctx.Done():
		return Entry[T]{}, false, ctx.Err()
	}
}

func (s *MemoryStore[T]) ReserveBatch(ctx context.Context, max int) ([]Entry[T], bool, error) {
	if max <= 0 {
		max = 1
	}
	first, ok, err := s.Reserve(ctx)
	if err != nil || !ok {
		return nil, ok, err
	}

	entries := make([]Entry[T], 0, max)
	entries = append(entries, first)
	for len(entries) < max {
		select {
		case entry := <-s.pendingCh:
			s.mu.Lock()
			s.inflight[entry.ID] = entry
			s.mu.Unlock()
			entries = append(entries, entry)
		default:
			return entries, true, nil
		}
	}
	return entries, true, nil
}

func (s *MemoryStore[T]) DrainPending() int {
	drained := 0
	for {
		select {
		case <-s.pendingCh:
			drained++
		default:
			return drained
		}
	}
}

func (s *MemoryStore[T]) Ack(id uint64) error {
	s.mu.Lock()
	if _, ok := s.inflight[id]; ok {
		delete(s.inflight, id)
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()
	return fmt.Errorf("%w: id=%d", ErrNotFound, id)
}

func (s *MemoryStore[T]) Retry(id uint64, cause error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	entry, ok := s.inflight[id]
	if !ok {
		return fmt.Errorf("%w: id=%d", ErrNotFound, id)
	}

	entry.Attempt++
	select {
	case s.pendingCh <- entry:
		delete(s.inflight, id)
		return nil
	default:
		return ErrPendingQueueFull
	}
}

func (s *MemoryStore[T]) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}

	s.closed = true
	close(s.closedCh)

	return nil
}
