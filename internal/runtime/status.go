package runtime

import (
	"context"

	"github.com/ruohao1/pipex/internal/frontier"
)

// TryDurableStatusSnapshot fetches a durable status snapshot when the store
// implements frontier.DurableStatusProvider.
func TryDurableStatusSnapshot(ctx context.Context, store any) (frontier.DurableStatusSnapshot, bool, error) {
	provider, ok := store.(frontier.DurableStatusProvider)
	if !ok {
		return frontier.DurableStatusSnapshot{}, false, nil
	}
	snap, err := provider.StatusSnapshot(ctx)
	if err != nil {
		return frontier.DurableStatusSnapshot{}, true, err
	}
	return snap, true, nil
}

func DurableSnapshotToStats(s frontier.DurableStatusSnapshot) frontier.Stats {
	return frontier.Stats{
		Pending:        s.Pending,
		Inflight:       s.Reserved,
		Acked:          s.Acked,
		Retried:        s.RetriedEntries,
		Dropped:        s.Dropped,
		TerminalFailed: s.TerminalFailed,
		Canceled:       s.Canceled,
		// Durable snapshots currently do not carry queue depth / enqueue-full
		// counters in a portable way.
		PendingQueueDepth: 0,
		EnqueueFull:       0,
	}
}
