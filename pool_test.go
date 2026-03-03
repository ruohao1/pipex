package pipex

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewPoolValidation(t *testing.T) {
	if _, err := NewPool(PoolConfig{Workers: 0, Queue: 0}); !errors.Is(err, ErrInvalidWorkerCount) {
		t.Fatalf("expected ErrInvalidWorkerCount, got %v", err)
	}
	if _, err := NewPool(PoolConfig{Workers: 1, Queue: -1}); !errors.Is(err, ErrInvalidQueueSize) {
		t.Fatalf("expected ErrInvalidQueueSize, got %v", err)
	}
	if _, err := NewPool(PoolConfig{Workers: 2, Queue: 0}); err != nil {
		t.Fatalf("unexpected pool config error: %v", err)
	}
}

func TestPoolRunReportsJobErrorsAndCloses(t *testing.T) {
	p, err := NewPool(PoolConfig{Workers: 2, Queue: 10})
	if err != nil {
		t.Fatalf("unexpected NewPool error: %v", err)
	}

	jobs := make(chan Job)
	go func() {
		defer close(jobs)
		jobs <- Job{Name: "ok", Run: func(ctx context.Context) error { return nil }}
		jobs <- Job{Name: "nil"}
		jobs <- Job{Name: "err", Run: func(ctx context.Context) error { return errors.New("boom") }}
	}()

	errsCh := p.Run(context.Background(), jobs)

	var gotErrs []error
	for err := range errsCh {
		gotErrs = append(gotErrs, err)
	}

	if len(gotErrs) != 2 {
		t.Fatalf("expected 2 job errors, got %d (%v)", len(gotErrs), gotErrs)
	}

	foundNil := false
	foundBoom := false
	for _, e := range gotErrs {
		if errors.Is(e, ErrNilJobFunc) {
			foundNil = true
		}
		if e != nil && e.Error() == "boom" {
			foundBoom = true
		}
	}
	if !foundNil || !foundBoom {
		t.Fatalf("expected ErrNilJobFunc and boom in errors, got %v", gotErrs)
	}
}

func TestPoolRunRespectsWorkerLimit(t *testing.T) {
	const workers = 2
	const totalJobs = 8

	p, err := NewPool(PoolConfig{Workers: workers, Queue: totalJobs})
	if err != nil {
		t.Fatalf("unexpected NewPool error: %v", err)
	}

	var inFlight int64
	var maxInFlight int64

	jobs := make(chan Job)
	go func() {
		defer close(jobs)
		for i := 0; i < totalJobs; i++ {
			jobs <- Job{Name: "j", Run: func(ctx context.Context) error {
				cur := atomic.AddInt64(&inFlight, 1)
				for {
					prev := atomic.LoadInt64(&maxInFlight)
					if cur <= prev || atomic.CompareAndSwapInt64(&maxInFlight, prev, cur) {
						break
					}
				}
				time.Sleep(20 * time.Millisecond)
				atomic.AddInt64(&inFlight, -1)
				return nil
			}}
		}
	}()

	for range p.Run(context.Background(), jobs) {
		// no-op: successful jobs should produce no errors
	}

	if got := atomic.LoadInt64(&maxInFlight); got > workers {
		t.Fatalf("expected max concurrency <= %d, got %d", workers, got)
	}
}

func TestPoolRunContextCancelStopsWorkers(t *testing.T) {
	p, err := NewPool(PoolConfig{Workers: 2, Queue: 10})
	if err != nil {
		t.Fatalf("unexpected NewPool error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobs := make(chan Job)
	var executed int64
	var once sync.Once

	go func() {
		defer close(jobs)
		for i := 0; i < 20; i++ {
			jobs <- Job{Name: "block", Run: func(ctx context.Context) error {
				atomic.AddInt64(&executed, 1)
				once.Do(cancel)
				<-ctx.Done()
				return ctx.Err()
			}}
		}
	}()

	errCount := 0
	for range p.Run(ctx, jobs) {
		errCount++
	}

	if errCount == 0 {
		t.Fatal("expected at least one error after cancellation")
	}
	if got := atomic.LoadInt64(&executed); got > 4 {
		t.Fatalf("expected only a small number of jobs to start after cancellation, got %d", got)
	}
}
