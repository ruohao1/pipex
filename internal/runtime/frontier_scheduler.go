package runtime

import (
	"context"
	"fmt"

	"github.com/ruohao1/pipex/internal/frontier"
)

type FrontierSchedulerConfig[T any] struct {
	Ctx          context.Context
	BufferSize   int
	Store        frontier.Store[T]
	IsContextErr func(error) bool
	Dispatch     func(entry frontier.Entry[T])
	OnError      func(error)
}

// RunFrontierScheduler reserves frontier entries and dispatches them until the
// store closes, context ends, or a terminal reserve condition is reached.
func RunFrontierScheduler[T any](cfg FrontierSchedulerConfig[T]) {
	batchSize := min(64, max(1, cfg.BufferSize))
	batchBuf := make([]frontier.Entry[T], 0, batchSize)

	type frontierBatchReserver interface {
		ReserveInto(ctx context.Context, dst []frontier.Entry[T], max int) ([]frontier.Entry[T], bool, error)
	}
	batchReserver, hasBatchReserver := cfg.Store.(frontierBatchReserver)

	for {
		if hasBatchReserver {
			batchBuf = batchBuf[:0]
			entries, ok, err := batchReserver.ReserveInto(cfg.Ctx, batchBuf, batchSize)
			if err != nil {
				if cfg.IsContextErr(err) {
					return
				}
				cfg.OnError(fmt.Errorf("frontier reserve batch: %w", err))
				continue
			}
			if !ok {
				return
			}
			for _, entry := range entries {
				cfg.Dispatch(entry)
			}
			continue
		}

		entry, ok, err := cfg.Store.Reserve(cfg.Ctx)
		if err != nil {
			if cfg.IsContextErr(err) {
				return
			}
			cfg.OnError(fmt.Errorf("frontier reserve: %w", err))
			continue
		}
		if !ok {
			return
		}
		cfg.Dispatch(entry)
	}
}
