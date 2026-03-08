package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ruohao1/pipex/internal/frontier"
)

func TestEnqueueWithBackpressure_RetriesThenSucceeds(t *testing.T) {
	store := &enqueueStore[int]{
		enqueueErrs: []error{
			frontier.ErrPendingQueueFull,
			frontier.ErrPendingQueueFull,
			nil,
		},
		nextID: 42,
	}

	id, err := EnqueueWithBackpressure(context.Background(), store, "stage", 7, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != 42 {
		t.Fatalf("id = %d, want 42", id)
	}
	if store.enqueueCalls != 3 {
		t.Fatalf("enqueue calls = %d, want 3", store.enqueueCalls)
	}
}

func TestEnqueueWithBackpressure_NonRetriableError(t *testing.T) {
	want := errors.New("boom")
	store := &enqueueStore[int]{
		enqueueErrs: []error{want},
	}

	_, err := EnqueueWithBackpressure(context.Background(), store, "stage", 7, 0)
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
	if store.enqueueCalls != 1 {
		t.Fatalf("enqueue calls = %d, want 1", store.enqueueCalls)
	}
}

func TestEnqueueWithBackpressure_ContextCanceled(t *testing.T) {
	store := &enqueueStore[int]{
		enqueueErrs: []error{
			frontier.ErrPendingQueueFull,
			frontier.ErrPendingQueueFull,
			frontier.ErrPendingQueueFull,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(300 * time.Microsecond)
		cancel()
	}()

	_, err := EnqueueWithBackpressure(ctx, store, "stage", 7, 0)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

type enqueueStore[T any] struct {
	enqueueErrs  []error
	enqueueCalls int
	nextID       uint64
}

func (s *enqueueStore[T]) Enqueue(stage string, item T, hops int) (uint64, error) {
	s.enqueueCalls++
	idx := s.enqueueCalls - 1
	if idx < len(s.enqueueErrs) && s.enqueueErrs[idx] != nil {
		return 0, s.enqueueErrs[idx]
	}
	return s.nextID, nil
}

func (s *enqueueStore[T]) Reserve(ctx context.Context) (frontier.Entry[T], bool, error) {
	return frontier.Entry[T]{}, false, nil
}
func (s *enqueueStore[T]) Ack(id uint64) error                { return nil }
func (s *enqueueStore[T]) Retry(id uint64, cause error) error { return nil }
func (s *enqueueStore[T]) Close() error                       { return nil }
