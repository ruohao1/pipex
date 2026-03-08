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

type DurableFrontierStore[T any] interface {
	Enqueue(ctx context.Context, e Entry[T]) (created bool, err error) // idempotent by Key
	Reserve(ctx context.Context, max int, leaseTTL time.Duration) ([]Entry[T], []Lease, error)
	Ack(ctx context.Context, key, leaseID string) error
	Retry(ctx context.Context, key, leaseID string, cause error, nextVisibleAt time.Time) error
	MarkTerminalFailed(ctx context.Context, key, leaseID string, cause error) error
	RequeueExpired(ctx context.Context, now time.Time, limit int) (int, error)
}
