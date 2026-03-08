package frontier

import (
	"errors"
)

var (
	ErrInvalidStageName = errors.New("stage name cannot be empty")
	ErrInvalidHops      = errors.New("hops cannot be negative")
	ErrClosed           = errors.New("store is closed")
	ErrNotFound         = errors.New("entry not found")
	ErrPendingQueueFull = errors.New("pending queue is full")

	ErrInvalidStateTransition = errors.New("invalid state transition")
)
