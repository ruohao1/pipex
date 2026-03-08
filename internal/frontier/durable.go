package frontier

import (
	"context"
	"time"
)

type Lease struct {
	Key   string
	ID    string
	Until time.Time
}

const (
	// DefaultDurableLeaseTTL is the default reservation lease window used by
	// durable frontier implementations when no explicit policy override exists.
	DefaultDurableLeaseTTL = 30 * time.Second

	// DefaultRequeueExpiredLimit bounds how many expired reservations are
	// requeued in one recovery pass.
	DefaultRequeueExpiredLimit = 4096
)

type DurableFrontierStore[T any] interface {
	Enqueue(ctx context.Context, e Entry[T]) (created bool, err error) // idempotent by Key
	Reserve(ctx context.Context, max int, leaseTTL time.Duration) ([]Entry[T], []Lease, error)
	Ack(ctx context.Context, key, leaseID string) error
	Retry(ctx context.Context, key, leaseID string, cause error, nextVisibleAt time.Time) error
	MarkTerminalFailed(ctx context.Context, key, leaseID string, cause error) error
	RequeueExpired(ctx context.Context, now time.Time, limit int) (int, error)
}

type DurableStatusSnapshot struct {
	Pending        int64
	Reserved       int64
	Acked          int64
	TerminalFailed int64
	Dropped        int64
	Canceled       int64
	// Optional/derived:
	RetriedEntries int64 // attempt > 1
	Total          int64
	At             time.Time
}

type DurableStatusProvider interface {
	StatusSnapshot(ctx context.Context) (DurableStatusSnapshot, error)
}
