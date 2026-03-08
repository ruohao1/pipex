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

func TestDurableStoreStatusSnapshot_Empty(t *testing.T) {
	s := newTestStore(t)

	base := time.UnixMilli(1_700_000_500_000)
	s.now = func() time.Time { return base }

	snap, err := s.StatusSnapshot(context.Background())
	if err != nil {
		t.Fatalf("status snapshot failed: %v", err)
	}
	if snap.Pending != 0 || snap.Reserved != 0 || snap.Acked != 0 || snap.TerminalFailed != 0 ||
		snap.Dropped != 0 || snap.Canceled != 0 || snap.RetriedEntries != 0 || snap.Total != 0 {
		t.Fatalf("unexpected non-zero snapshot: %+v", snap)
	}
	if !snap.At.Equal(base) {
		t.Fatalf("unexpected snapshot time: got=%v want=%v", snap.At, base)
	}
}

func TestDurableStoreStatusSnapshot_MixedStatesAndRetried(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	base := time.UnixMilli(1_700_000_600_000)
	s.now = func() time.Time { return base }

	// pending (attempt=1)
	if _, err := s.Enqueue(ctx, frontier.Entry[int]{
		Key: "k-pending", Stage: "a", Input: 1, Hops: 0, Attempt: 1,
	}); err != nil {
		t.Fatalf("enqueue pending failed: %v", err)
	}

	// pending retried (attempt=2)
	if _, err := s.Enqueue(ctx, frontier.Entry[int]{
		Key: "k-retried", Stage: "a", Input: 2, Hops: 0, Attempt: 2,
	}); err != nil {
		t.Fatalf("enqueue retried failed: %v", err)
	}

	// reserved -> acked
	if _, err := s.Enqueue(ctx, frontier.Entry[int]{
		Key: "k-acked", Stage: "a", Input: 3, Hops: 0, Attempt: 1,
	}); err != nil {
		t.Fatalf("enqueue acked failed: %v", err)
	}
	entries, leases, err := s.Reserve(ctx, 1, 10*time.Second)
	if err != nil || len(entries) != 1 || len(leases) != 1 {
		t.Fatalf("reserve for acked failed entries=%d leases=%d err=%v", len(entries), len(leases), err)
	}
	if entries[0].Key != "k-acked" {
		t.Fatalf("unexpected reserved key for acked flow: %s", entries[0].Key)
	}
	if err := s.Ack(ctx, entries[0].Key, leases[0].ID); err != nil {
		t.Fatalf("ack failed: %v", err)
	}

	// reserved -> terminal_failed
	if _, err := s.Enqueue(ctx, frontier.Entry[int]{
		Key: "k-failed", Stage: "a", Input: 4, Hops: 0, Attempt: 1,
	}); err != nil {
		t.Fatalf("enqueue failed-flow failed: %v", err)
	}
	entries, leases, err = s.Reserve(ctx, 1, 10*time.Second)
	if err != nil || len(entries) != 1 || len(leases) != 1 {
		t.Fatalf("reserve for failed flow failed entries=%d leases=%d err=%v", len(entries), len(leases), err)
	}
	if entries[0].Key != "k-failed" {
		t.Fatalf("unexpected reserved key for failed flow: %s", entries[0].Key)
	}
	if err := s.MarkTerminalFailed(ctx, entries[0].Key, leases[0].ID, errors.New("boom")); err != nil {
		t.Fatalf("mark terminal failed failed: %v", err)
	}

	// direct state setup for dropped/canceled coverage
	if _, err := s.Enqueue(ctx, frontier.Entry[int]{
		Key: "k-dropped", Stage: "a", Input: 5, Hops: 0, Attempt: 1,
	}); err != nil {
		t.Fatalf("enqueue dropped failed: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE frontier SET state='dropped' WHERE key='k-dropped';`); err != nil {
		t.Fatalf("set dropped failed: %v", err)
	}

	if _, err := s.Enqueue(ctx, frontier.Entry[int]{
		Key: "k-canceled", Stage: "a", Input: 6, Hops: 0, Attempt: 1,
	}); err != nil {
		t.Fatalf("enqueue canceled failed: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE frontier SET state='canceled' WHERE key='k-canceled';`); err != nil {
		t.Fatalf("set canceled failed: %v", err)
	}

	snap, err := s.StatusSnapshot(ctx)
	if err != nil {
		t.Fatalf("status snapshot failed: %v", err)
	}

	if snap.Pending != 2 {
		t.Fatalf("pending=%d want=2", snap.Pending)
	}
	if snap.Reserved != 0 {
		t.Fatalf("reserved=%d want=0", snap.Reserved)
	}
	if snap.Acked != 1 {
		t.Fatalf("acked=%d want=1", snap.Acked)
	}
	if snap.TerminalFailed != 1 {
		t.Fatalf("terminal_failed=%d want=1", snap.TerminalFailed)
	}
	if snap.Dropped != 1 {
		t.Fatalf("dropped=%d want=1", snap.Dropped)
	}
	if snap.Canceled != 1 {
		t.Fatalf("canceled=%d want=1", snap.Canceled)
	}
	if snap.RetriedEntries != 1 {
		t.Fatalf("retried_entries=%d want=1", snap.RetriedEntries)
	}
	if snap.Total != 6 {
		t.Fatalf("total=%d want=6", snap.Total)
	}
	if !snap.At.Equal(base) {
		t.Fatalf("unexpected snapshot time: got=%v want=%v", snap.At, base)
	}
}
