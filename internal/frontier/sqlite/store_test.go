package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/ruohao1/pipex/internal/frontier"
)

func newTestStore(t *testing.T) *DurableStore[int] {
	t.Helper()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	s := NewDurableStore[int](db)
	if err := s.InitSchema(context.Background()); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	return s
}

func TestDurableStoreRequiresInitSchema(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	s := NewDurableStore[int](db)
	_, err = s.Enqueue(context.Background(), frontier.Entry[int]{
		Key: "k1", Stage: "a", Input: 1, Hops: 0, Attempt: 1,
	})
	if !errors.Is(err, ErrSchemaNotInitialized) {
		t.Fatalf("expected ErrSchemaNotInitialized, got %v", err)
	}
}

func TestDurableStoreEnqueueIdempotentAndReserveAckLease(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	created, err := s.Enqueue(ctx, frontier.Entry[int]{
		Key: "k1", Stage: "a", Input: 11, Hops: 0, Attempt: 1,
	})
	if err != nil || !created {
		t.Fatalf("first enqueue failed created=%v err=%v", created, err)
	}
	created, err = s.Enqueue(ctx, frontier.Entry[int]{
		Key: "k1", Stage: "a", Input: 99, Hops: 0, Attempt: 1,
	})
	if err != nil {
		t.Fatalf("second enqueue failed: %v", err)
	}
	if created {
		t.Fatal("expected second enqueue to be dedup no-op")
	}

	entries, leases, err := s.Reserve(ctx, 1, 5*time.Second)
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if len(entries) != 1 || len(leases) != 1 {
		t.Fatalf("unexpected reserve lengths entries=%d leases=%d", len(entries), len(leases))
	}
	if entries[0].Key != "k1" || entries[0].Input != 11 {
		t.Fatalf("unexpected reserved entry: %+v", entries[0])
	}

	if err := s.Ack(ctx, "k1", "wrong-lease"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for wrong lease, got %v", err)
	}
	if err := s.Ack(ctx, "k1", leases[0].ID); err != nil {
		t.Fatalf("ack failed: %v", err)
	}
}

func TestDurableStoreRetryRespectsNextVisibleAtAndIncrementsAttempt(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	base := time.UnixMilli(1_700_000_000_000)
	s.now = func() time.Time { return base }

	_, err := s.Enqueue(ctx, frontier.Entry[int]{
		Key: "k-delay", Stage: "a", Input: 7, Hops: 1, Attempt: 1,
	})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	entries, leases, err := s.Reserve(ctx, 1, 10*time.Second)
	if err != nil || len(entries) != 1 || len(leases) != 1 {
		t.Fatalf("reserve failed entries=%d leases=%d err=%v", len(entries), len(leases), err)
	}

	nextVisible := base.Add(30 * time.Second)
	if err := s.Retry(ctx, entries[0].Key, leases[0].ID, errors.New("retry"), nextVisible); err != nil {
		t.Fatalf("retry failed: %v", err)
	}

	entries, leases, err = s.Reserve(ctx, 1, 10*time.Second)
	if err != nil {
		t.Fatalf("reserve before visibility failed: %v", err)
	}
	if len(entries) != 0 || len(leases) != 0 {
		t.Fatalf("expected no visible entries yet, got entries=%d leases=%d", len(entries), len(leases))
	}

	s.now = func() time.Time { return nextVisible.Add(time.Millisecond) }
	entries, leases, err = s.Reserve(ctx, 1, 10*time.Second)
	if err != nil {
		t.Fatalf("reserve after visibility failed: %v", err)
	}
	if len(entries) != 1 || len(leases) != 1 {
		t.Fatalf("expected one entry after visibility, got entries=%d leases=%d", len(entries), len(leases))
	}
	if entries[0].Attempt != 2 {
		t.Fatalf("expected attempt incremented to 2, got %d", entries[0].Attempt)
	}
}

func TestDurableStoreRequeueExpiredHonorsLimit(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	base := time.UnixMilli(1_700_000_100_000)
	s.now = func() time.Time { return base }
	for i := 0; i < 3; i++ {
		_, err := s.Enqueue(ctx, frontier.Entry[int]{
			Key: "k" + string(rune('a'+i)), Stage: "a", Input: i, Hops: 0, Attempt: 1,
		})
		if err != nil {
			t.Fatalf("enqueue %d failed: %v", i, err)
		}
	}
	_, _, err := s.Reserve(ctx, 3, 5*time.Second)
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}

	n, err := s.RequeueExpired(ctx, base.Add(10*time.Second), 2)
	if err != nil {
		t.Fatalf("requeue expired first call failed: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected first requeue count 2, got %d", n)
	}

	n, err = s.RequeueExpired(ctx, base.Add(10*time.Second), 2)
	if err != nil {
		t.Fatalf("requeue expired second call failed: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected second requeue count 1, got %d", n)
	}
}

func TestDurableStoreRequeueExpired_IdempotentWithoutNewReserve(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	base := time.UnixMilli(1_700_000_200_000)
	s.now = func() time.Time { return base }

	_, err := s.Enqueue(ctx, frontier.Entry[int]{
		Key: "k-idem", Stage: "a", Input: 1, Hops: 0, Attempt: 1,
	})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	_, _, err = s.Reserve(ctx, 1, 5*time.Second)
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}

	afterExpiry := base.Add(10 * time.Second)
	n, err := s.RequeueExpired(ctx, afterExpiry, 10)
	if err != nil {
		t.Fatalf("first requeue failed: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected first requeue count 1, got %d", n)
	}

	n, err = s.RequeueExpired(ctx, afterExpiry, 10)
	if err != nil {
		t.Fatalf("second requeue failed: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected second requeue count 0, got %d", n)
	}
}

func TestDurableStoreRequeueExpired_OncePerLeaseCycle(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	base := time.UnixMilli(1_700_000_300_000)
	s.now = func() time.Time { return base }

	_, err := s.Enqueue(ctx, frontier.Entry[int]{
		Key: "k-cycle", Stage: "a", Input: 1, Hops: 0, Attempt: 1,
	})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	_, _, err = s.Reserve(ctx, 1, 5*time.Second)
	if err != nil {
		t.Fatalf("first reserve failed: %v", err)
	}

	firstExpiry := base.Add(10 * time.Second)
	n, err := s.RequeueExpired(ctx, firstExpiry, 10)
	if err != nil {
		t.Fatalf("first requeue failed: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected first requeue count 1, got %d", n)
	}

	// Reserve again to create a new lease cycle for the same entry.
	s.now = func() time.Time { return firstExpiry.Add(time.Millisecond) }
	entries, leases, err := s.Reserve(ctx, 1, 5*time.Second)
	if err != nil {
		t.Fatalf("second reserve failed: %v", err)
	}
	if len(entries) != 1 || len(leases) != 1 {
		t.Fatalf("expected one entry in second reserve, got entries=%d leases=%d", len(entries), len(leases))
	}

	secondExpiry := firstExpiry.Add(10 * time.Second)
	n, err = s.RequeueExpired(ctx, secondExpiry, 10)
	if err != nil {
		t.Fatalf("second cycle requeue failed: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected second cycle requeue count 1, got %d", n)
	}
}

func TestDurableStoreRequeueExpired_NoopBeforeLeaseExpiry(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	base := time.UnixMilli(1_700_000_400_000)
	s.now = func() time.Time { return base }

	_, err := s.Enqueue(ctx, frontier.Entry[int]{
		Key: "k-not-expired", Stage: "a", Input: 1, Hops: 0, Attempt: 1,
	})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	_, _, err = s.Reserve(ctx, 1, 30*time.Second)
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}

	n, err := s.RequeueExpired(ctx, base.Add(5*time.Second), 10)
	if err != nil {
		t.Fatalf("requeue before expiry failed: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected requeue count 0 before lease expiry, got %d", n)
	}
}
