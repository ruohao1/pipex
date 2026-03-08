package runtime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/ruohao1/pipex/internal/frontier"
)

func TestRunFrontierScheduler_ReserveDispatch(t *testing.T) {
	store := &reserveStore[int]{
		reserves: []reserveResult[int]{
			{entry: frontier.Entry[int]{ID: 1, Stage: "a", Input: 42}, ok: true},
			{ok: false},
		},
	}

	var dispatched atomic.Int64
	RunFrontierScheduler(FrontierSchedulerConfig[int]{
		Ctx:        context.Background(),
		BufferSize: 8,
		Store:      store,
		IsContextErr: func(err error) bool {
			return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
		},
		Dispatch: func(entry frontier.Entry[int]) {
			if entry.ID != 1 || entry.Input != 42 {
				t.Fatalf("unexpected entry: %+v", entry)
			}
			dispatched.Add(1)
		},
		OnError: func(err error) {
			t.Fatalf("unexpected error: %v", err)
		},
	})

	if got := dispatched.Load(); got != 1 {
		t.Fatalf("dispatch count = %d, want 1", got)
	}
}

func TestRunFrontierScheduler_BatchReserveErrorThenDispatch(t *testing.T) {
	store := &batchReserveStore[int]{
		batches: []batchResult[int]{
			{err: errors.New("boom")},
			{
				entries: []frontier.Entry[int]{
					{ID: 2, Stage: "b", Input: 7},
					{ID: 3, Stage: "b", Input: 8},
				},
				ok: true,
			},
			{ok: false},
		},
	}

	var (
		dispatched atomic.Int64
		errored    atomic.Int64
	)
	RunFrontierScheduler(FrontierSchedulerConfig[int]{
		Ctx:        context.Background(),
		BufferSize: 16,
		Store:      store,
		IsContextErr: func(err error) bool {
			return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
		},
		Dispatch: func(entry frontier.Entry[int]) {
			if entry.Stage != "b" {
				t.Fatalf("unexpected stage: %s", entry.Stage)
			}
			dispatched.Add(1)
		},
		OnError: func(err error) {
			errored.Add(1)
		},
	})

	if got := errored.Load(); got != 1 {
		t.Fatalf("error count = %d, want 1", got)
	}
	if got := dispatched.Load(); got != 2 {
		t.Fatalf("dispatch count = %d, want 2", got)
	}
}

type reserveResult[T any] struct {
	entry frontier.Entry[T]
	ok    bool
	err   error
}

type reserveStore[T any] struct {
	reserves []reserveResult[T]
	idx      int
}

func (s *reserveStore[T]) Enqueue(stage string, item T, hops int) (uint64, error) { return 0, nil }
func (s *reserveStore[T]) Ack(id uint64) error                                     { return nil }
func (s *reserveStore[T]) Retry(id uint64, cause error) error                      { return nil }
func (s *reserveStore[T]) Close() error                                            { return nil }
func (s *reserveStore[T]) Reserve(ctx context.Context) (frontier.Entry[T], bool, error) {
	if s.idx >= len(s.reserves) {
		return frontier.Entry[T]{}, false, nil
	}
	r := s.reserves[s.idx]
	s.idx++
	return r.entry, r.ok, r.err
}

type batchResult[T any] struct {
	entries []frontier.Entry[T]
	ok      bool
	err     error
}

type batchReserveStore[T any] struct {
	batches []batchResult[T]
	idx     int
}

func (s *batchReserveStore[T]) Enqueue(stage string, item T, hops int) (uint64, error) { return 0, nil }
func (s *batchReserveStore[T]) Ack(id uint64) error                                     { return nil }
func (s *batchReserveStore[T]) Retry(id uint64, cause error) error                      { return nil }
func (s *batchReserveStore[T]) Close() error                                            { return nil }
func (s *batchReserveStore[T]) Reserve(ctx context.Context) (frontier.Entry[T], bool, error) {
	return frontier.Entry[T]{}, false, nil
}
func (s *batchReserveStore[T]) ReserveInto(ctx context.Context, dst []frontier.Entry[T], max int) ([]frontier.Entry[T], bool, error) {
	if s.idx >= len(s.batches) {
		return dst[:0], false, nil
	}
	r := s.batches[s.idx]
	s.idx++
	if r.err != nil {
		return dst[:0], false, r.err
	}
	out := dst[:0]
	out = append(out, r.entries...)
	return out, r.ok, nil
}
