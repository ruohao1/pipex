package runtime

import (
	"context"
	"errors"
	"time"

	"github.com/ruohao1/pipex/internal/frontier"
)

// EnqueueWithBackpressure retries enqueue while pending capacity is temporarily
// full, and stops on context cancellation or non-capacity errors.
func EnqueueWithBackpressure[T any](ctx context.Context, store frontier.Store[T], stage string, item T, hops int) (uint64, error) {
	var backoffTicker *time.Ticker
	stopBackoffTicker := func() {
		if backoffTicker != nil {
			backoffTicker.Stop()
		}
	}
	defer stopBackoffTicker()

	for {
		entryID, err := store.Enqueue(stage, item, hops)
		if err == nil {
			return entryID, nil
		}
		if !errors.Is(err, frontier.ErrPendingQueueFull) {
			return 0, err
		}
		if backoffTicker == nil {
			backoffTicker = time.NewTicker(250 * time.Microsecond)
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-backoffTicker.C:
		}
	}
}
