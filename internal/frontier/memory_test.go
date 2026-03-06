package frontier

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemoryStoreReserveClosedReturnsNoWork(t *testing.T) {
	s := NewMemoryStore[int]()
	if err := s.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	e, ok, err := s.Reserve(context.Background())
	if err != nil {
		t.Fatalf("expected nil error on closed reserve, got %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false on closed reserve, got entry=%+v", e)
	}
}

func TestMemoryStoreAckAllowedAfterCloseForInflight(t *testing.T) {
	s := NewMemoryStore[int]()
	id, err := s.Enqueue("a", 1, 0)
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	_, ok, err := s.Reserve(context.Background())
	if err != nil || !ok {
		t.Fatalf("reserve failed: ok=%v err=%v", ok, err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	if err := s.Ack(id); err != nil {
		t.Fatalf("expected ack to succeed after close for inflight entry, got %v", err)
	}
}

func TestMemoryStoreRetryRejectedAfterClose(t *testing.T) {
	s := NewMemoryStore[int]()
	_, err := s.Enqueue("a", 1, 0)
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	e, ok, err := s.Reserve(context.Background())
	if err != nil || !ok {
		t.Fatalf("reserve failed: ok=%v err=%v", ok, err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	if err := s.Retry(e.ID, errors.New("boom")); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed from retry after close, got %v", err)
	}
}

func TestMemoryStoreEnqueueRejectedAfterClose(t *testing.T) {
	s := NewMemoryStore[int]()
	if err := s.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	if _, err := s.Enqueue("a", 1, 0); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed on enqueue after close, got %v", err)
	}
}

func TestMemoryStoreReserveHonorsContextCancellation(t *testing.T) {
	s := NewMemoryStore[int]()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, ok, err := s.Reserve(ctx)
	if ok {
		t.Fatal("expected ok=false when reserve context is canceled")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation error, got %v", err)
	}
}

func TestMemoryStoreRetryIncrementsAttemptWithoutChangingHops(t *testing.T) {
	s := NewMemoryStore[int]()
	_, err := s.Enqueue("a", 1, 3)
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	e, ok, err := s.Reserve(context.Background())
	if err != nil || !ok {
		t.Fatalf("reserve failed: ok=%v err=%v", ok, err)
	}
	if e.Attempt != 1 || e.Hops != 3 {
		t.Fatalf("unexpected initial entry state: %+v", e)
	}

	if err := s.Retry(e.ID, errors.New("retry")); err != nil {
		t.Fatalf("retry failed: %v", err)
	}

	e2, ok, err := s.Reserve(context.Background())
	if err != nil || !ok {
		t.Fatalf("reserve failed after retry: ok=%v err=%v", ok, err)
	}
	if e2.Attempt != 2 {
		t.Fatalf("expected attempt increment to 2, got %+v", e2)
	}
	if e2.Hops != 3 {
		t.Fatalf("expected hops unchanged at 3, got %+v", e2)
	}
}

func TestMemoryStoreAckRetryUnknownIDReturnNotFound(t *testing.T) {
	s := NewMemoryStore[int]()
	if err := s.Ack(999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound from Ack unknown id, got %v", err)
	}
	if err := s.Retry(999, errors.New("x")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound from Retry unknown id, got %v", err)
	}
}

func TestMemoryStoreEnqueueReturnsQueueFullWhenPendingBufferFull(t *testing.T) {
	s := NewMemoryStore[int]()
	for i := 0; i < 100; i++ {
		if _, err := s.Enqueue("a", i, 0); err != nil {
			t.Fatalf("enqueue %d failed unexpectedly: %v", i, err)
		}
	}
	if _, err := s.Enqueue("a", 101, 0); !errors.Is(err, ErrPendingQueueFull) {
		t.Fatalf("expected ErrPendingQueueFull when buffer is full, got %v", err)
	}
}

func TestMemoryStoreRetryReturnsQueueFullWhenPendingBufferFull(t *testing.T) {
	s := NewMemoryStore[int]()
	_, err := s.Enqueue("a", 1, 0)
	if err != nil {
		t.Fatalf("seed enqueue failed: %v", err)
	}
	e, ok, err := s.Reserve(context.Background())
	if err != nil || !ok {
		t.Fatalf("seed reserve failed: ok=%v err=%v", ok, err)
	}

	// Fill pending queue to capacity while one entry stays inflight.
	for i := 0; i < 100; i++ {
		if _, err := s.Enqueue("a", i, 0); err != nil {
			t.Fatalf("enqueue %d failed unexpectedly: %v", i, err)
		}
	}

	if err := s.Retry(e.ID, errors.New("retry")); !errors.Is(err, ErrPendingQueueFull) {
		t.Fatalf("expected ErrPendingQueueFull from Retry when pending buffer is full, got %v", err)
	}
}

func TestMemoryStoreWithCapacityRespectsPendingLimit(t *testing.T) {
	s := NewMemoryStoreWithCapacity[int](2)
	if _, err := s.Enqueue("a", 1, 0); err != nil {
		t.Fatalf("enqueue 1 failed: %v", err)
	}
	if _, err := s.Enqueue("a", 2, 0); err != nil {
		t.Fatalf("enqueue 2 failed: %v", err)
	}
	if _, err := s.Enqueue("a", 3, 0); !errors.Is(err, ErrPendingQueueFull) {
		t.Fatalf("expected ErrPendingQueueFull at configured capacity, got %v", err)
	}
}
