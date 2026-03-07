package frontier

import "context"

type Entry[T any] struct {
	ID      uint64
	Stage   string
	Input   T
	Hops    int
	Attempt int

	State EntryState
}

type EntryState int

const (
	StatePending EntryState = iota
	StateReserved
	StateRetried

	// Terminal states
	StateAcked
	StateCanceled
	StateDropped
	StateTerminalFailed
)



type Store[T any] interface {
	// Enqueue adds a new pending entry. Returns ErrClosed if the store has
	// already been closed.
	Enqueue(stage string, item T, hops int) (id uint64, err error)
	// Reserve returns ok=false, err=nil when the store is closed and should
	// no longer be consumed.
	Reserve(ctx context.Context) (entry Entry[T], ok bool, err error)
	// Ack marks an inflight entry as completed.
	Ack(id uint64) error
	// Retry requeues an inflight entry with incremented attempt count.
	Retry(id uint64, cause error) error
	// Close marks the store as closed; future Reserve calls should return
	// ok=false, err=nil.
	Close() error
}
