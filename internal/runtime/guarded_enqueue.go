package runtime

import (
	"context"
	"sync"
	"time"

	"github.com/ruohao1/pipex/internal/frontier"
)

type FrontierBlockingEnqueuer[T any] interface {
	EnqueueWait(ctx context.Context, stage string, item T, hops int) (id uint64, err error)
}

type GuardedDedupRule[T any] struct {
	Name  string
	Scope string
	Key   func(T) string
}

type GuardedEnqueueConfig[T any] struct {
	Ctx                  context.Context
	RunCtx               context.Context
	RunID                string
	StageName            string
	Input                T
	Hops                 int
	DedupRules           []GuardedDedupRule[T]
	SeenMu               *sync.Mutex
	Seen                 map[string]struct{}
	CycleEnabled         bool
	CycleMaxHops         int
	CycleMaxJobs         int
	AcceptedJobs         *int
	ReservedJobs         *int
	FrontierMu           *sync.Mutex
	UseFrontier          bool
	FrontierStore        frontier.Store[T]
	BlockingEnqueuer     FrontierBlockingEnqueuer[T]
	HasBlockingEnqueuer  bool
	BlockingEnqueue      bool
	FrontierOutstanding  *sync.WaitGroup
	EnqueueDirect        func(stageName string, in T, hops int, frontierEntryID uint64, frontierEntryAttempt int) error
	BuildCycleMaxJobsErr func(maxJobs int) error
	OnDedupDrop          func(scope string, key string, item T, at time.Time)
	OnCycleHopLimitDrop  func(stage string, item T, hops int, maxHops int, at time.Time)
	OnCycleMaxJobsDrop   func(stage string, item T, acceptedJobs int, maxJobs int, at time.Time)
	OnFrontierEnqueue    func(entryID uint64, stage string, item T, hops int, at time.Time)
}

func ExecuteGuardedEnqueue[T any](cfg GuardedEnqueueConfig[T]) error {
	if cfg.Ctx.Err() != nil {
		return cfg.Ctx.Err()
	}

	dedupInsertedKeys := make([]string, 0, 4)
	var reservationTaken bool
	var key string
	rollbackDedup := func() {
		if len(dedupInsertedKeys) == 0 {
			return
		}
		cfg.SeenMu.Lock()
		for _, k := range dedupInsertedKeys {
			delete(cfg.Seen, k)
		}
		cfg.SeenMu.Unlock()
	}

	for _, rule := range cfg.DedupRules {
		dedupKeyValue, derr := SafeStringKey(rule.Key, cfg.Input)
		if derr != nil {
			rollbackDedup()
			return derr
		}
		key = cfg.StageName + "\x00" + dedupKeyValue
		dedupMapKey := cfg.StageName + "\x00" + rule.Name + "\x00" + dedupKeyValue
		if SeenContainsOrInsert(cfg.SeenMu, cfg.Seen, dedupMapKey) {
			if cfg.OnDedupDrop != nil {
				cfg.OnDedupDrop(rule.Scope, key, cfg.Input, time.Now())
			}
			return nil
		}
		dedupInsertedKeys = append(dedupInsertedKeys, dedupMapKey)
	}

	if cfg.CycleEnabled {
		at := time.Now()
		cfg.FrontierMu.Lock()
		switch EvaluateCycleGate(
			cfg.CycleEnabled,
			cfg.CycleMaxHops,
			cfg.CycleMaxJobs,
			cfg.Hops,
			*cfg.AcceptedJobs,
			*cfg.ReservedJobs,
		) {
		case CycleGateDropHopLimit:
			rollbackDedup()
			cfg.FrontierMu.Unlock()
			if cfg.OnCycleHopLimitDrop != nil {
				cfg.OnCycleHopLimitDrop(cfg.StageName, cfg.Input, cfg.Hops, cfg.CycleMaxHops, at)
			}
			return nil
		case CycleGateRejectMaxJobs:
			rollbackDedup()
			cfg.FrontierMu.Unlock()
			if cfg.OnCycleMaxJobsDrop != nil {
				cfg.OnCycleMaxJobsDrop(cfg.StageName, cfg.Input, *cfg.AcceptedJobs, cfg.CycleMaxJobs, at)
			}
			if cfg.BuildCycleMaxJobsErr != nil {
				return cfg.BuildCycleMaxJobsErr(cfg.CycleMaxJobs)
			}
			return nil
		}
		*cfg.ReservedJobs = *cfg.ReservedJobs + 1
		reservationTaken = true
		cfg.FrontierMu.Unlock()
	}

	select {
	case <-cfg.Ctx.Done():
		cfg.FrontierMu.Lock()
		if reservationTaken {
			*cfg.ReservedJobs = *cfg.ReservedJobs - 1
		}
		rollbackDedup()
		cfg.FrontierMu.Unlock()
		return cfg.Ctx.Err()
	default:
	}

	var (
		err     error
		entryID uint64
	)
	if cfg.UseFrontier {
		if cfg.BlockingEnqueue && cfg.HasBlockingEnqueuer {
			entryID, err = cfg.BlockingEnqueuer.EnqueueWait(cfg.RunCtx, cfg.StageName, cfg.Input, cfg.Hops)
		} else {
			entryID, err = EnqueueWithBackpressure(cfg.RunCtx, cfg.FrontierStore, cfg.StageName, cfg.Input, cfg.Hops)
		}
		if err == nil {
			cfg.FrontierOutstanding.Add(1)
			if cfg.OnFrontierEnqueue != nil {
				cfg.OnFrontierEnqueue(entryID, cfg.StageName, cfg.Input, cfg.Hops, time.Now())
			}
		}
	} else {
		err = cfg.EnqueueDirect(cfg.StageName, cfg.Input, cfg.Hops, 0, 0)
	}
	if err != nil {
		cfg.FrontierMu.Lock()
		if reservationTaken {
			*cfg.ReservedJobs = *cfg.ReservedJobs - 1
		}
		cfg.FrontierMu.Unlock()
		rollbackDedup()
		return err
	}

	cfg.FrontierMu.Lock()
	if reservationTaken {
		*cfg.ReservedJobs = *cfg.ReservedJobs - 1
	}
	*cfg.AcceptedJobs = *cfg.AcceptedJobs + 1
	cfg.FrontierMu.Unlock()
	return nil
}
