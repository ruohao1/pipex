package frontier

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type MemoryStore[T any] struct {
	mu       sync.Mutex
	nextID   uint64
	inflight map[uint64]Entry[T]
	states   map[uint64]EntryState
	dropped  map[uint64]struct{}
	closed   bool

	pendingCount        atomic.Int64
	inflightCount       atomic.Int64
	ackedCount          atomic.Int64
	retriedCount        atomic.Int64
	droppedCount        atomic.Int64
	terminalFailedCount atomic.Int64
	canceledCount       atomic.Int64
	enqueueFullCount    atomic.Int64

	pendingCh chan Entry[T]
	closedCh  chan struct{}

	terminalHistory      []uint64
	terminalHistoryLimit int
}

// State returns the latest known lifecycle state for an entry id.
func (s *MemoryStore[T]) State(id uint64) (EntryState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.states[id]
	return st, ok
}

// StateCounts returns the number of entries currently tracked per state.
func (s *MemoryStore[T]) StateCounts() map[EntryState]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	counts := make(map[EntryState]int)
	for _, st := range s.states {
		counts[st]++
	}
	return counts
}

func (s *MemoryStore[T]) Stats() Stats {
	return Stats{
		Pending:           s.pendingCount.Load(),
		Inflight:          s.inflightCount.Load(),
		Acked:             s.ackedCount.Load(),
		Retried:           s.retriedCount.Load(),
		Dropped:           s.droppedCount.Load(),
		TerminalFailed:    s.terminalFailedCount.Load(),
		Canceled:          s.canceledCount.Load(),
		EnqueueFull:       s.enqueueFullCount.Load(),
		PendingQueueDepth: len(s.pendingCh),
	}
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
		states:    make(map[uint64]EntryState),
		dropped:   make(map[uint64]struct{}),
		closed:    false,
		pendingCh: make(chan Entry[T], pendingCapacity),
		closedCh:  make(chan struct{}),
		// Keep a bounded terminal-state history to avoid unbounded growth.
		terminalHistoryLimit: max(1024, pendingCapacity*4),
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
		State:   StatePending,
	}

	select {
	case s.pendingCh <- entry:
		s.nextID++
		s.states[entry.ID] = StatePending
		s.pendingCount.Add(1)
		return entry.ID, nil
	default:
		s.enqueueFullCount.Add(1)
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

	backoffTicker := time.NewTicker(250 * time.Microsecond)
	defer backoffTicker.Stop()
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return 0, ErrClosed
		}

		entry := Entry[T]{
			ID:      s.nextID,
			Stage:   stage,
			Input:   item,
			Hops:    hops,
			Attempt: 1,
			State:   StatePending,
		}
		select {
		case s.pendingCh <- entry:
			s.nextID++
			s.states[entry.ID] = StatePending
			s.pendingCount.Add(1)
			s.mu.Unlock()
			return entry.ID, nil
		default:
			s.enqueueFullCount.Add(1)
			s.mu.Unlock()
		}

		select {
		case <-s.closedCh:
			return 0, ErrClosed
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-backoffTicker.C:
		}
	}
}

func (s *MemoryStore[T]) Reserve(ctx context.Context) (Entry[T], bool, error) {
	select {
	case <-s.closedCh:
		return Entry[T]{}, false, nil
	default:
	}

	for {
		select {
		case entry := <-s.pendingCh:
			s.mu.Lock()
			if _, isDropped := s.dropped[entry.ID]; isDropped {
				delete(s.dropped, entry.ID)
				s.mu.Unlock()
				continue
			}
			st, ok := s.states[entry.ID]
			if !ok {
				s.mu.Unlock()
				return Entry[T]{}, false, fmt.Errorf("%w: id=%d", ErrNotFound, entry.ID)
			}
			if st != StatePending {
				s.mu.Unlock()
				return Entry[T]{}, false, fmt.Errorf("%w: id=%d, expected state=%d, actual state=%d", ErrInvalidStateTransition, entry.ID, StatePending, st)
			}
			s.inflight[entry.ID] = entry
			var err error
			if entry, err = s.transition(entry.ID, StatePending, StateReserved); err != nil {
				s.mu.Unlock()
				return Entry[T]{}, false, err
			}
			s.mu.Unlock()
			return entry, true, nil
		case <-s.closedCh:
			return Entry[T]{}, false, nil
		case <-ctx.Done():
			return Entry[T]{}, false, ctx.Err()
		}
	}
}

func (s *MemoryStore[T]) ReserveInto(ctx context.Context, dst []Entry[T], max int) ([]Entry[T], bool, error) {
	if max <= 0 {
		max = 1
	}

	entries := dst[:0]

	// Preserve Reserve semantics for the first item: block until work,
	// close, or context cancellation.
	first, ok, err := s.Reserve(ctx)
	if err != nil || !ok {
		return entries, ok, err
	}
	entries = append(entries, first)

	// Best-effort non-blocking drain for the rest of the batch.
	for len(entries) < max {
		select {
		case entry := <-s.pendingCh:
			s.mu.Lock()
			if _, isDropped := s.dropped[entry.ID]; isDropped {
				delete(s.dropped, entry.ID)
				s.mu.Unlock()
				continue
			}
			st, exists := s.states[entry.ID]
			if !exists {
				s.mu.Unlock()
				return entries, false, fmt.Errorf("%w: id=%d", ErrNotFound, entry.ID)
			}
			if st != StatePending {
				s.mu.Unlock()
				return entries, false, fmt.Errorf(
					"%w: id=%d, expected state=%d, actual state=%d",
					ErrInvalidStateTransition, entry.ID, StatePending, st,
				)
			}
			s.inflight[entry.ID] = entry
			var trErr error
			if entry, trErr = s.transition(entry.ID, StatePending, StateReserved); trErr != nil {
				s.mu.Unlock()
				return entries, false, trErr
			}
			s.mu.Unlock()
			entries = append(entries, entry)
		default:
			return entries, true, nil
		}
	}

	return entries, true, nil
}

func (s *MemoryStore[T]) ReserveBatch(ctx context.Context, max int) ([]Entry[T], bool, error) {
	return s.ReserveInto(ctx, nil, max)
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
		if _, err := s.transition(id, StateReserved, StateAcked); err != nil {
			s.mu.Unlock()
			return err
		}
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
	if entry.State != StateReserved {
		return fmt.Errorf("%w: id=%d, expected state=%d, actual state=%d", ErrInvalidStateTransition, id, StateReserved, entry.State)
	}

	entry.Attempt++
	entry.State = StatePending

	select {
	case s.pendingCh <- entry:
		delete(s.inflight, id)
		s.states[id] = StatePending
		s.retriedCount.Add(1)
		s.inflightCount.Add(-1)
		s.pendingCount.Add(1)
		return nil
	default:
		s.enqueueFullCount.Add(1)
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

func (s *MemoryStore[T]) transition(id uint64, from, to EntryState) (Entry[T], error) {
	entry, ok := s.inflight[id]
	if !ok {
		return Entry[T]{}, fmt.Errorf("%w: id=%d", ErrNotFound, id)
	}

	if entry.State != from {
		return Entry[T]{}, fmt.Errorf("%w: id=%d, expected state=%d, actual state=%d", ErrInvalidStateTransition, id, from, entry.State)
	}

	entry.State = to
	s.states[id] = to
	s.inflight[id] = entry
	switch {
	case from == StatePending && to == StateReserved:
		s.pendingCount.Add(-1)
		s.inflightCount.Add(1)
	case from == StateReserved && to == StateAcked:
		s.inflightCount.Add(-1)
		s.ackedCount.Add(1)
	case from == StateReserved && to == StateCanceled:
		s.inflightCount.Add(-1)
		s.canceledCount.Add(1)
	case from == StateReserved && to == StateTerminalFailed:
		s.inflightCount.Add(-1)
		s.terminalFailedCount.Add(1)
	}
	if to == StateAcked || to == StateCanceled || to == StateDropped || to == StateTerminalFailed {
		delete(s.inflight, id)
		s.recordTerminalStateLocked(id)
	}
	return entry, nil
}

func (s *MemoryStore[T]) MarkTerminalFailed(id uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.inflight[id]; ok {
		if _, err := s.transition(id, StateReserved, StateTerminalFailed); err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("%w: id=%d", ErrNotFound, id)
}

func (s *MemoryStore[T]) MarkCanceled(id uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.inflight[id]; ok {
		if _, err := s.transition(id, StateReserved, StateCanceled); err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("%w: id=%d", ErrNotFound, id)
}

func (s *MemoryStore[T]) MarkDropped(id uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if state, ok := s.states[id]; ok {
		if state != StatePending {
			return fmt.Errorf("%w: id=%d, expected state=%d, actual state=%d", ErrInvalidStateTransition, id, StatePending, state)
		}
		s.states[id] = StateDropped
		s.dropped[id] = struct{}{}
		s.pendingCount.Add(-1)
		s.droppedCount.Add(1)
		s.recordTerminalStateLocked(id)
		return nil
	}
	return fmt.Errorf("%w: id=%d", ErrNotFound, id)
}

func (s *MemoryStore[T]) recordTerminalStateLocked(id uint64) {
	s.terminalHistory = append(s.terminalHistory, id)
	for len(s.terminalHistory) > s.terminalHistoryLimit {
		evictID := s.terminalHistory[0]
		s.terminalHistory = s.terminalHistory[1:]
		if _, isDroppedPendingSkip := s.dropped[evictID]; isDroppedPendingSkip {
			continue
		}
		if _, inflight := s.inflight[evictID]; inflight {
			continue
		}
		delete(s.states, evictID)
	}
}
