package pipex

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ruohao1/pipex/internal/frontier"
	iruntime "github.com/ruohao1/pipex/internal/runtime"
	"golang.org/x/time/rate"

	"github.com/ruohao1/pipex/internal/graph"
)

type Pipeline[T any] struct {
	stages map[string]Stage[T]
	edges  map[string][]string
	mu     sync.RWMutex
}

// NewPipeline creates a new instance of Pipeline.
func NewPipeline[T any]() *Pipeline[T] {
	return &Pipeline[T]{
		stages: make(map[string]Stage[T]),
		edges:  make(map[string][]string),
	}
}
func parseStageScope(s DedupScope) (stage string, ok bool) {
	const p = "stage:"
	v := string(s)
	if strings.HasPrefix(v, p) && len(v) > len(p) {
		return v[len(p):], true
	}
	return "", false
}

// Run executes the pipeline with the given seeds. The seeds map specifies the initial input items for each stage. The method returns a map of stage names to their output items, or an error if the pipeline configuration is invalid or if any stage processing fails.
func (p *Pipeline[T]) Run(ctx context.Context, seeds map[string][]T, opts ...Option[T]) (map[string][]T, error) {
	runOpts := defaultOptions[T]()
	for _, opt := range opts {
		opt(runOpts)
	}
	runID := iruntime.NewRunID()

	p.mu.RLock()
	stages := make(map[string]Stage[T], len(p.stages))
	maps.Copy(stages, p.stages)
	edges := make(map[string][]string, len(p.edges))
	for k, v := range p.edges {
		edges[k] = append([]string(nil), v...)
	}
	p.mu.RUnlock()

	runMeta := RunMeta{
		RunID:      runID,
		SeedStages: len(seeds),
		SeedItems: func() int {
			count := 0
			for _, items := range seeds {
				count += len(items)
			}
			return count
		}(),
		StageCount: len(stages),
		EdgeCount: func() int {
			count := 0
			for _, nextStages := range edges {
				count += len(nextStages)
			}
			return count
		}(),
		TriggerCount: len(runOpts.Triggers),
		SinkCount:    len(runOpts.Sinks),
		FailFast:     runOpts.FailFast,
		BufferSize:   runOpts.BufferSize,
	}
	iruntime.Call2(runOpts.Hooks.RunStart, ctx, runMeta)
	var retErr error
	defer func() {
		iruntime.Call3(runOpts.Hooks.RunEnd, ctx, runMeta, retErr)
	}()

	if runOpts.CycleMode.Enabled {
		if runOpts.CycleMode.MaxHops < -1 {
			retErr = ErrCycleModeInvalidMaxHops
			return nil, retErr
		}
		if runOpts.CycleMode.MaxJobs <= 0 {
			retErr = ErrCycleModeInvalidMaxJobs
			return nil, retErr
		}
	}

	if err := p.validateSnapshot(stages, edges, runOpts.CycleMode.Enabled); err != nil {
		retErr = err
		return nil, retErr
	}

	for seedStage := range seeds {
		if _, ok := stages[seedStage]; !ok {
			retErr = StageNotFound(seedStage)
			return nil, retErr
		}
	}
	for _, trigger := range runOpts.Triggers {
		if _, ok := stages[trigger.Stage()]; !ok {
			retErr = fmt.Errorf("trigger %s: %w", trigger.Name(), StageNotFound(trigger.Stage()))
			return nil, retErr
		}
	}
	for _, sink := range runOpts.Sinks {
		if _, ok := stages[sink.Stage()]; !ok {
			retErr = fmt.Errorf("sink %s: %w", sink.Name(), StageNotFound(sink.Stage()))
			return nil, retErr
		}
	}

	for stageName, count := range runOpts.StageWorkers {
		if _, ok := stages[stageName]; !ok {
			retErr = fmt.Errorf("stage workers config: %w", StageNotFound(stageName))
			return nil, retErr
		}
		if count <= 0 {
			retErr = fmt.Errorf("stage %s: invalid worker count %d: %w", stageName, count, ErrInvalidWorkerCount)
			return nil, retErr
		}
	}

	for stageName, rate := range runOpts.StageRateLimits {
		if _, ok := stages[stageName]; !ok {
			retErr = fmt.Errorf("stage rate limits config: %w", StageNotFound(stageName))
			return nil, retErr
		}
		if rate.RPS <= 0 {
			retErr = fmt.Errorf("stage %s: invalid RPS %f: %w", stageName, rate.RPS, ErrInvalidRPS)
			return nil, retErr
		}
		if rate.Burst < 1 {
			retErr = fmt.Errorf("stage %s: invalid burst %d: %w", stageName, rate.Burst, ErrInvalidBurst)
			return nil, retErr
		}
	}

	for stageName, policy := range runOpts.StagePolicies {
		if _, ok := stages[stageName]; !ok {
			retErr = fmt.Errorf("stage policies config: %w", StageNotFound(stageName))
			return nil, retErr
		}
		if policy.MaxAttempts < 1 {
			retErr = fmt.Errorf("invalid stage policy max attempts %d: %w", policy.MaxAttempts, ErrInvalidStagePolicyMaxAttempts)
			return nil, retErr
		}
		if policy.Backoff < 0 {
			retErr = fmt.Errorf("invalid stage policy backoff %s: %w", policy.Backoff, ErrInvalidStagePolicyBackoff)
			return nil, retErr
		}
		if policy.Timeout < 0 {
			retErr = fmt.Errorf("invalid stage policy timeout %s: %w", policy.Timeout, ErrInvalidStagePolicyTimeout)
			return nil, retErr
		}
	}

	if ctx.Err() != nil {
		retErr = ctx.Err()
		return nil, retErr
	}

	results := make(map[string][]T)
	var resultsMu sync.Mutex
	var tasksWG sync.WaitGroup
	var poolErrWG sync.WaitGroup
	var triggersWG sync.WaitGroup
	var sinksWG sync.WaitGroup

	jobsByStage := make(map[string]chan Job, len(stages))
	poolsByStage := make(map[string]*Pool, len(stages))
	sinksByStage := make(map[string][]chan T)
	dedupRulesByStage := make(map[string][]DedupRule[T])

	for _, rule := range runOpts.DedupRules {
		if rule.Name == "" {
			retErr = ErrInvalidDedupRuleName
			return nil, retErr
		}
		if rule.Key == nil {
			retErr = fmt.Errorf("dedup rule %s: %w", rule.Name, ErrInvalidDedupRuleKey)
			return nil, retErr
		}
		if rule.Scope == "" {
			retErr = fmt.Errorf("dedup rule %s: %w", rule.Name, ErrInvalidDedupRuleScope)
			return nil, retErr
		}
		stageScope, isStageScope := parseStageScope(rule.Scope)
		if !isStageScope && rule.Scope != DedupScopeGlobal {
			retErr = fmt.Errorf("dedup rule %s: invalid scope %s: %w", rule.Name, rule.Scope, ErrInvalidDedupRuleScope)
			return nil, retErr
		}

		if isStageScope {
			if _, ok := stages[stageScope]; !ok {
				retErr = fmt.Errorf("dedup rule %s: %w", rule.Name, StageNotFound(stageScope))
				return nil, retErr
			}
			dedupRulesByStage[stageScope] = append(dedupRulesByStage[stageScope], rule)
		}

		if !isStageScope || rule.Scope == DedupScopeGlobal {
			for stageName := range stages {
				dedupRulesByStage[stageName] = append(dedupRulesByStage[stageName], rule)
			}
		}
	}

	var errsMu sync.Mutex
	var runErrs []error
	var errOnce sync.Once
	var frontierMu sync.Mutex
	acceptedJobs := 0
	reservedJobs := 0
	seen := make(map[string]struct{})
	var seenMu sync.Mutex

	limiters := map[string]*rate.Limiter{}
	for stageName, cfg := range runOpts.StageRateLimits {
		limiters[stageName] = rate.NewLimiter(rate.Limit(cfg.RPS), cfg.Burst)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	runCtx := ctx

	isContextErr := func(err error) bool {
		return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
	}

	addErr := func(err error) {
		if err == nil {
			return
		}
		errsMu.Lock()
		runErrs = append(runErrs, err)
		errsMu.Unlock()
	}

	recordErr := func(err error) {
		if err == nil || isContextErr(err) {
			return
		}
		if runOpts.FailFast {
			errOnce.Do(func() {
				addErr(err)
				cancel()
			})
			return
		}
		addErr(err)
	}

	useFrontier := false
	fs := frontier.NewMemoryStore[T]()
	defer func() { _ = fs.Close() }()

	var enqueueGuarded func(stageName string, in T, hops int) error
	enqueueGuarded = func(stageName string, in T, hops int) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		dedupInsertedKeys := make([]string, 0, 4)
		var reservationTaken bool
		var key string
		rollbackDedup := func() {
			if len(dedupInsertedKeys) == 0 {
				return
			}
			seenMu.Lock()
			for _, k := range dedupInsertedKeys {
				delete(seen, k)
			}
			seenMu.Unlock()
		}

		if dedupRules, ok := dedupRulesByStage[stageName]; ok {
			for _, rule := range dedupRules {
				dedupKeyValue, derr := func() (key string, err error) {
					defer func() {
						if r := recover(); r != nil {
							err = fmt.Errorf("dedup key panic: %v", r)
						}
					}()
					return rule.Key(in), nil
				}()
				if derr != nil {
					rollbackDedup()
					return derr
				}
				key = stageName + "\x00" + dedupKeyValue
				dedupMapKey := stageName + "\x00" + rule.Name + "\x00" + dedupKeyValue
				seenMu.Lock()
				if _, exists := seen[dedupMapKey]; exists {
					seenMu.Unlock()
					iruntime.Call2(runOpts.Hooks.DedupDrop, runCtx, DedupDropEvent[T]{
						RunID: runID,
						Item:  in,
						Key:   key,
						At:    time.Now(),
						Scope: rule.Scope,
					})
					return nil
				}
				seen[dedupMapKey] = struct{}{}
				dedupInsertedKeys = append(dedupInsertedKeys, dedupMapKey)
				seenMu.Unlock()
			}
		}
		if runOpts.CycleMode.Enabled {
			at := time.Now()
			frontierMu.Lock()
			if runOpts.CycleMode.MaxHops >= 0 && hops > runOpts.CycleMode.MaxHops {
				rollbackDedup()
				frontierMu.Unlock()
				iruntime.Call2(runOpts.Hooks.CycleHopLimitDrop, runCtx, CycleHopLimitDropEvent[T]{
					RunID:   runID,
					Stage:   stageName,
					Item:    in,
					Hops:    hops,
					MaxHops: runOpts.CycleMode.MaxHops,
					At:      at,
				})

				return nil
			}
			if acceptedJobs+reservedJobs >= runOpts.CycleMode.MaxJobs {
				rollbackDedup()
				frontierMu.Unlock()
				iruntime.Call2(runOpts.Hooks.CycleMaxJobsExceeded, runCtx, CycleMaxJobsExceededEvent[T]{
					RunID:        runID,
					Stage:        stageName,
					Item:         in,
					AcceptedJobs: acceptedJobs,
					MaxJobs:      runOpts.CycleMode.MaxJobs,
					At:           at,
				})
				return CycleModeMaxJobsExceeded(runOpts.CycleMode.MaxJobs)
			}

			reservedJobs++
			reservationTaken = true
			frontierMu.Unlock()
		}

		select {
		case <-ctx.Done():
			frontierMu.Lock()
			if reservationTaken {
				reservedJobs--
			}
			rollbackDedup()
			frontierMu.Unlock()
			return ctx.Err()
		default:
		}

		_, err := fs.Enqueue(stageName, in, hops)
		if err != nil {
			frontierMu.Lock()
			if reservationTaken {
				reservedJobs--
			}
			frontierMu.Unlock()
			rollbackDedup()
			return err
		}

		frontierMu.Lock()
		if reservationTaken {
			reservedJobs--
		}
		acceptedJobs++
		frontierMu.Unlock()
		return nil
	}

	// Scheduler
	var shedulerWG sync.WaitGroup
	shedulerWG.Go(func() {
		if !useFrontier {
			return
		}

		for {
			entry, ok, err := fs.Reserve(ctx)
			if err != nil {
				recordErr(fmt.Errorf("frontier reserve: %w", err))
				continue
			}
			if !ok {
				break
			}
			stageJobs, ok := jobsByStage[entry.Stage]

			if !ok {
				recordErr(StageNotFound(entry.Stage))
				continue
			}

			job := Job{
				Name: fmt.Sprintf("%s_%v", entry.Stage, entry.Input),
				Run: func(_ context.Context) error {
					defer tasksWG.Done()
					if runCtx.Err() != nil {
						return runCtx.Err()
					}

					stage := stages[entry.Stage]

					if limiter, ok := limiters[entry.Stage]; ok {
						if err := limiter.Wait(runCtx); err != nil {
							return fmt.Errorf("stage %s: rate limit wait: %w", entry.Stage, err)
						}
					}

					policy, hasPolicy := runOpts.StagePolicies[entry.Stage]
					if !hasPolicy {
						policy = StagePolicy{MaxAttempts: 1}
					}
					if policy.MaxAttempts <= 0 {
						policy.MaxAttempts = 1
					}

					stageStart := time.Now()
					iruntime.Call2(runOpts.Hooks.StageStart, runCtx, StageStartEvent[T]{
						RunID:     runID,
						Stage:     entry.Stage,
						Input:     entry.Input,
						StartedAt: stageStart,
					})

					var (
						out []T
						err error
					)

					for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
						attemptCtx := runCtx
						var cancel context.CancelFunc = func() {}
						if policy.Timeout > 0 {
							attemptCtx, cancel = context.WithTimeout(runCtx, policy.Timeout)
						}

						attemptStart := time.Now()
						iruntime.Call2(runOpts.Hooks.StageAttemptStart, runCtx, StageAttemptStartEvent[T]{
							RunID:     runID,
							Stage:     entry.Stage,
							Input:     entry.Input,
							Attempt:   attempt,
							StartedAt: attemptStart,
						})
						out, err = stage.Process(attemptCtx, entry.Input)
						attemptFinish := time.Now()
						attemptDuration := attemptFinish.Sub(attemptStart)
						cancel()

						if err == nil {
							iruntime.Call2(runOpts.Hooks.StageFinish, runCtx, StageFinishEvent[T]{
								RunID:      runID,
								Stage:      entry.Stage,
								Input:      entry.Input,
								OutCount:   len(out),
								StartedAt:  stageStart,
								FinishedAt: attemptFinish,
								Duration:   attemptFinish.Sub(stageStart),
							})
							break
						}

						iruntime.Call2(runOpts.Hooks.StageAttemptError, runCtx, StageAttemptErrorEvent[T]{
							RunID:      runID,
							Stage:      entry.Stage,
							Input:      entry.Input,
							Attempt:    attempt,
							StartedAt:  attemptStart,
							FinishedAt: attemptFinish,
							Duration:   attemptDuration,
							Err:        err,
						})
						if errors.Is(attemptCtx.Err(), context.DeadlineExceeded) {
							iruntime.Call2(runOpts.Hooks.StageTimeout, runCtx, StageTimeoutEvent[T]{
								RunID:     runID,
								Stage:     entry.Stage,
								Input:     entry.Input,
								Attempt:   attempt,
								StartedAt: attemptStart,
								Timeout:   policy.Timeout,
								At:        attemptFinish,
							})
						}

						// stop immediately on run cancellation
						if runCtx.Err() != nil {
							if !isContextErr(err) {
								iruntime.Call2(runOpts.Hooks.StageError, runCtx, StageErrorEvent[T]{
									RunID:      runID,
									Stage:      entry.Stage,
									Input:      entry.Input,
									StartedAt:  stageStart,
									FinishedAt: attemptFinish,
									Duration:   attemptFinish.Sub(stageStart),
									Err:        err,
								})
								return fmt.Errorf("stage %s: %w", entry.Stage, err)
							}
							return runCtx.Err()
						}

						// no retries left
						if attempt == policy.MaxAttempts {
							iruntime.Call2(runOpts.Hooks.StageExhausted, runCtx, StageExhaustedEvent[T]{
								RunID:    runID,
								Stage:    entry.Stage,
								Input:    entry.Input,
								Attempts: attempt,
								Err:      err,
								At:       attemptFinish,
							})
							iruntime.Call2(runOpts.Hooks.StageError, runCtx, StageErrorEvent[T]{
								RunID:      runID,
								Stage:      entry.Stage,
								Input:      entry.Input,
								StartedAt:  stageStart,
								FinishedAt: attemptFinish,
								Duration:   attemptFinish.Sub(stageStart),
								Err:        err,
							})
							break
						}

						iruntime.Call2(runOpts.Hooks.StageRetry, runCtx, StageRetryEvent[T]{
							RunID:   runID,
							Stage:   entry.Stage,
							Input:   entry.Input,
							Attempt: attempt,
							Err:     err,
							Backoff: policy.Backoff,
							At:      attemptFinish,
						})

						// context-aware backoff
						if policy.Backoff > 0 {
							select {
							case <-runCtx.Done():
								return runCtx.Err()
							case <-time.After(policy.Backoff):
							}
						}
					}

					if err != nil {
						return fmt.Errorf("stage %s: %w", entry.Stage, err)
					}

					resultsMu.Lock()
					results[entry.Stage] = append(results[entry.Stage], out...)
					resultsMu.Unlock()

					for _, item := range out {
						for _, sinkCh := range sinksByStage[entry.Stage] {
							select {
							case <-runCtx.Done():
								return runCtx.Err()
							case sinkCh <- item:
							}
						}
					}

					for _, nextStage := range edges[entry.Stage] {
						for _, item := range out {
							err := enqueueGuarded(nextStage, item, entry.Hops+1)
							if err != nil {
								return fmt.Errorf("enqueue to %s: %w", nextStage, err)
							}
						}
					}

					if err := fs.Ack(entry.ID); err != nil {
						recordErr(fmt.Errorf("frontier ack: %w", err))
					}
					return nil
				},
			}

			tasksWG.Add(1)
			select {
			case <-ctx.Done():
				tasksWG.Done()
				return
			case stageJobs <- job:
				frontierMu.Lock()
				acceptedJobs++
				frontierMu.Unlock()
			}

		}
	})

	// Snapshot stage worker configuration and ensure all pools can be created
	// before starting any worker goroutine. This avoids partial startup leaks
	// if one stage has an invalid worker count at run time.
	for stageName, stage := range stages {
		pool, err := NewPool(PoolConfig{
			Workers: func() int {
				if count, ok := runOpts.StageWorkers[stageName]; ok {
					return count
				}
				return stage.Workers()
			}(),
			Queue:         runOpts.BufferSize,
			DrainOnCancel: true,
		})
		if err != nil {
			retErr = fmt.Errorf("stage %s: failed to create worker pool: %w", stageName, err)
			return nil, retErr
		}
		poolsByStage[stageName] = pool
	}

	for _, sink := range runOpts.Sinks {
		sinkCh := make(chan T, runOpts.BufferSize)
		sinksByStage[sink.Stage()] = append(sinksByStage[sink.Stage()], sinkCh)

		sinksWG.Add(1)
		go func(sink Sink[T], sinkCh chan T) {
			defer sinksWG.Done()
			disabled := false
			for {
				select {
				case <-ctx.Done():
					return
				case item, ok := <-sinkCh:
					if !ok {
						return
					}
					if disabled {
						continue
					}

					attempts := 0
					for {
						startTime := time.Now()
						iruntime.Call2(runOpts.Hooks.SinkConsumeStart, runCtx, SinkConsumeStartEvent[T]{
							RunID:     runID,
							Sink:      sink.Name(),
							Stage:     sink.Stage(),
							Item:      item,
							Attempt:   attempts + 1,
							StartedAt: startTime,
						})
						err := sink.Consume(ctx, item)
						finishTime := time.Now()
						duration := finishTime.Sub(startTime)

						if err == nil {
							iruntime.Call2(runOpts.Hooks.SinkConsumeSuccess, runCtx, SinkConsumeSuccessEvent[T]{
								RunID:      runID,
								Sink:       sink.Name(),
								Stage:      sink.Stage(),
								Item:       item,
								Attempt:    attempts + 1,
								StartedAt:  startTime,
								FinishedAt: finishTime,
								Duration:   duration,
							})
							break
						}
						attempts++

						if runOpts.SinkRetry.MaxRetries >= 0 && attempts > runOpts.SinkRetry.MaxRetries {
							iruntime.Call2(runOpts.Hooks.SinkExhausted, runCtx, SinkExhaustedEvent[T]{
								RunID:    runID,
								Sink:     sink.Name(),
								Stage:    sink.Stage(),
								Item:     item,
								Err:      err,
								Attempts: attempts,
								At:       time.Now(),
							})
							recordErr(fmt.Errorf("sink %s: retries exhausted: %w", sink.Name(), err))
							disabled = true
							break
						}

						if ctx.Err() != nil {
							recordErr(fmt.Errorf("sink %s: %w", sink.Name(), err))
							return
						}

						iruntime.Call2(runOpts.Hooks.SinkRetry, runCtx, SinkRetryEvent[T]{
							RunID:   runID,
							Sink:    sink.Name(),
							Stage:   sink.Stage(),
							Item:    item,
							Attempt: attempts,
							Err:     err,
							Backoff: runOpts.SinkRetry.Backoff,
							At:      time.Now(),
						})

						select {
						case <-ctx.Done():
							return
						case <-time.After(runOpts.SinkRetry.Backoff):
						}
					}
				}
			}
		}(sink, sinkCh)
	}

	for stageName := range stages {
		stageJobs := make(chan Job, runOpts.BufferSize)
		jobsByStage[stageName] = stageJobs

		pool := poolsByStage[stageName]

		poolErrWG.Add(1)
		// Stage pools drain queued jobs after cancellation so tasksWG can reach zero.
		poolErrCh := pool.Run(runCtx, stageJobs)
		go func(poolErrCh <-chan error) {
			defer poolErrWG.Done()
			for err := range poolErrCh {
				recordErr(err)
			}
		}(poolErrCh)
	}

	var enqueue func(stageName string, in T, hops int) error
	enqueue = func(stageName string, in T, hops int) error {
		stageJobs, ok := jobsByStage[stageName]
		if !ok {
			return StageNotFound(stageName)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		dedupInsertedKeys := make([]string, 0, 4)
		var reservationTaken bool
		var key string
		rollbackDedup := func() {
			if len(dedupInsertedKeys) == 0 {
				return
			}
			seenMu.Lock()
			for _, k := range dedupInsertedKeys {
				delete(seen, k)
			}
			seenMu.Unlock()
		}

		if dedupRules, ok := dedupRulesByStage[stageName]; ok {
			for _, rule := range dedupRules {
				dedupKeyValue, derr := func() (key string, err error) {
					defer func() {
						if r := recover(); r != nil {
							err = fmt.Errorf("dedup key panic: %v", r)
						}
					}()
					return rule.Key(in), nil
				}()
				if derr != nil {
					rollbackDedup()
					return derr
				}
				key = stageName + "\x00" + dedupKeyValue
				dedupMapKey := stageName + "\x00" + rule.Name + "\x00" + dedupKeyValue
				seenMu.Lock()
				if _, exists := seen[dedupMapKey]; exists {
					seenMu.Unlock()
					iruntime.Call2(runOpts.Hooks.DedupDrop, runCtx, DedupDropEvent[T]{
						RunID: runID,
						Item:  in,
						Key:   key,
						At:    time.Now(),
						Scope: rule.Scope,
					})
					return nil
				}
				seen[dedupMapKey] = struct{}{}
				dedupInsertedKeys = append(dedupInsertedKeys, dedupMapKey)
				seenMu.Unlock()
			}
		}
		if runOpts.CycleMode.Enabled {
			at := time.Now()
			frontierMu.Lock()
			if runOpts.CycleMode.MaxHops >= 0 && hops > runOpts.CycleMode.MaxHops {
				rollbackDedup()
				frontierMu.Unlock()
				iruntime.Call2(runOpts.Hooks.CycleHopLimitDrop, runCtx, CycleHopLimitDropEvent[T]{
					RunID:   runID,
					Stage:   stageName,
					Item:    in,
					Hops:    hops,
					MaxHops: runOpts.CycleMode.MaxHops,
					At:      at,
				})

				return nil
			}
			if acceptedJobs+reservedJobs >= runOpts.CycleMode.MaxJobs {
				rollbackDedup()
				frontierMu.Unlock()
				iruntime.Call2(runOpts.Hooks.CycleMaxJobsExceeded, runCtx, CycleMaxJobsExceededEvent[T]{
					RunID:        runID,
					Stage:        stageName,
					Item:         in,
					AcceptedJobs: acceptedJobs,
					MaxJobs:      runOpts.CycleMode.MaxJobs,
					At:           at,
				})
				return CycleModeMaxJobsExceeded(runOpts.CycleMode.MaxJobs)
			}

			reservedJobs++
			reservationTaken = true
			frontierMu.Unlock()
		}

		tasksWG.Add(1)
		job := Job{
			Name: fmt.Sprintf("%s_%v", stageName, in),
			Run: func(_ context.Context) error {
				defer tasksWG.Done()
				if runCtx.Err() != nil {
					return runCtx.Err()
				}

				stage := stages[stageName]

				if limiter, ok := limiters[stageName]; ok {
					if err := limiter.Wait(runCtx); err != nil {
						return fmt.Errorf("stage %s: rate limit wait: %w", stageName, err)
					}
				}

				policy, hasPolicy := runOpts.StagePolicies[stageName]
				if !hasPolicy {
					policy = StagePolicy{MaxAttempts: 1}
				}
				if policy.MaxAttempts <= 0 {
					policy.MaxAttempts = 1
				}

				stageStart := time.Now()
				iruntime.Call2(runOpts.Hooks.StageStart, runCtx, StageStartEvent[T]{
					RunID:     runID,
					Stage:     stageName,
					Input:     in,
					StartedAt: stageStart,
				})

				var (
					out []T
					err error
				)

				for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
					attemptCtx := runCtx
					var cancel context.CancelFunc = func() {}
					if policy.Timeout > 0 {
						attemptCtx, cancel = context.WithTimeout(runCtx, policy.Timeout)
					}

					attemptStart := time.Now()
					iruntime.Call2(runOpts.Hooks.StageAttemptStart, runCtx, StageAttemptStartEvent[T]{
						RunID:     runID,
						Stage:     stageName,
						Input:     in,
						Attempt:   attempt,
						StartedAt: attemptStart,
					})
					out, err = stage.Process(attemptCtx, in)
					attemptFinish := time.Now()
					attemptDuration := attemptFinish.Sub(attemptStart)
					cancel()

					if err == nil {
						iruntime.Call2(runOpts.Hooks.StageFinish, runCtx, StageFinishEvent[T]{
							RunID:      runID,
							Stage:      stageName,
							Input:      in,
							OutCount:   len(out),
							StartedAt:  stageStart,
							FinishedAt: attemptFinish,
							Duration:   attemptFinish.Sub(stageStart),
						})
						break
					}

					iruntime.Call2(runOpts.Hooks.StageAttemptError, runCtx, StageAttemptErrorEvent[T]{
						RunID:      runID,
						Stage:      stageName,
						Input:      in,
						Attempt:    attempt,
						StartedAt:  attemptStart,
						FinishedAt: attemptFinish,
						Duration:   attemptDuration,
						Err:        err,
					})
					if errors.Is(attemptCtx.Err(), context.DeadlineExceeded) {
						iruntime.Call2(runOpts.Hooks.StageTimeout, runCtx, StageTimeoutEvent[T]{
							RunID:     runID,
							Stage:     stageName,
							Input:     in,
							Attempt:   attempt,
							StartedAt: attemptStart,
							Timeout:   policy.Timeout,
							At:        attemptFinish,
						})
					}

					// stop immediately on run cancellation
					if runCtx.Err() != nil {
						if !isContextErr(err) {
							iruntime.Call2(runOpts.Hooks.StageError, runCtx, StageErrorEvent[T]{
								RunID:      runID,
								Stage:      stageName,
								Input:      in,
								StartedAt:  stageStart,
								FinishedAt: attemptFinish,
								Duration:   attemptFinish.Sub(stageStart),
								Err:        err,
							})
							return fmt.Errorf("stage %s: %w", stageName, err)
						}
						return runCtx.Err()
					}

					// no retries left
					if attempt == policy.MaxAttempts {
						iruntime.Call2(runOpts.Hooks.StageExhausted, runCtx, StageExhaustedEvent[T]{
							RunID:    runID,
							Stage:    stageName,
							Input:    in,
							Attempts: attempt,
							Err:      err,
							At:       attemptFinish,
						})
						iruntime.Call2(runOpts.Hooks.StageError, runCtx, StageErrorEvent[T]{
							RunID:      runID,
							Stage:      stageName,
							Input:      in,
							StartedAt:  stageStart,
							FinishedAt: attemptFinish,
							Duration:   attemptFinish.Sub(stageStart),
							Err:        err,
						})
						break
					}

					iruntime.Call2(runOpts.Hooks.StageRetry, runCtx, StageRetryEvent[T]{
						RunID:   runID,
						Stage:   stageName,
						Input:   in,
						Attempt: attempt,
						Err:     err,
						Backoff: policy.Backoff,
						At:      attemptFinish,
					})

					// context-aware backoff
					if policy.Backoff > 0 {
						select {
						case <-runCtx.Done():
							return runCtx.Err()
						case <-time.After(policy.Backoff):
						}
					}
				}

				if err != nil {
					return fmt.Errorf("stage %s: %w", stageName, err)
				}

				resultsMu.Lock()
				results[stageName] = append(results[stageName], out...)
				resultsMu.Unlock()

				for _, item := range out {
					for _, sinkCh := range sinksByStage[stageName] {
						select {
						case <-runCtx.Done():
							return runCtx.Err()
						case sinkCh <- item:
						}
					}
				}

				for _, nextStage := range edges[stageName] {
					for _, item := range out {
						if err := enqueue(nextStage, item, hops+1); err != nil {
							return fmt.Errorf("enqueue to %s: %w", nextStage, err)
						}
					}
				}

				return nil
			},
		}

		select {
		case <-ctx.Done():
			tasksWG.Done()
			frontierMu.Lock()
			if reservationTaken {
				reservedJobs--
			}
			rollbackDedup()
			frontierMu.Unlock()
			return ctx.Err()

		case stageJobs <- job:
			frontierMu.Lock()
			if reservationTaken {
				reservedJobs--
			}
			acceptedJobs++
			frontierMu.Unlock()
			return nil
		}

	}

	for stageName, inputs := range seeds {
		for _, input := range inputs {
			if err := enqueue(stageName, input, 0); err != nil {
				recordErr(fmt.Errorf("enqueue seed %s: %w", stageName, err))
			}
		}
	}

	for _, trigger := range runOpts.Triggers {
		triggersWG.Add(1)
		go func(trigger Trigger[T]) {
			defer triggersWG.Done()
			emit := func(item T) error {
				if err := enqueue(trigger.Stage(), item, 0); err != nil {
					return fmt.Errorf("trigger enqueue %s: %w", trigger.Name(), err)
				}
				return nil
			}
			startTime := time.Now()
			iruntime.Call2(runOpts.Hooks.TriggerStart, runCtx, TriggerStartEvent[T]{
				RunID:     runID,
				Trigger:   trigger.Name(),
				Stage:     trigger.Stage(),
				StartedAt: startTime,
			})
			err := trigger.Start(ctx, emit)
			finishTime := time.Now()
			duration := finishTime.Sub(startTime)
			if err != nil {
				iruntime.Call2(runOpts.Hooks.TriggerError, runCtx, TriggerErrorEvent[T]{
					RunID:      runID,
					Trigger:    trigger.Name(),
					Stage:      trigger.Stage(),
					StartedAt:  startTime,
					FinishedAt: finishTime,
					Duration:   duration,
					Err:        err,
				})
				recordErr(fmt.Errorf("trigger %s: %w", trigger.Name(), err))
				return
			}
			iruntime.Call2(runOpts.Hooks.TriggerEnd, runCtx, TriggerEndEvent[T]{
				RunID:      runID,
				Trigger:    trigger.Name(),
				Stage:      trigger.Stage(),
				StartedAt:  startTime,
				FinishedAt: finishTime,
				Duration:   duration,
			})
		}(trigger)
	}

	shedulerWG.Wait()
	triggersWG.Wait()
	tasksWG.Wait()
	for _, jobsCh := range jobsByStage {
		close(jobsCh)
	}
	poolErrWG.Wait()
	for _, sinkChs := range sinksByStage {
		for _, sinkCh := range sinkChs {
			close(sinkCh)
		}
	}
	sinksWG.Wait()

	errsMu.Lock()
	joinedErr := errors.Join(runErrs...)
	errsMu.Unlock()
	if joinedErr != nil {
		retErr = joinedErr
		if runOpts.ReturnPartialResults {
			return results, retErr
		}
		return nil, retErr
	}

	if err := ctx.Err(); err != nil {
		retErr = err
		if runOpts.ReturnPartialResults {
			return results, retErr
		}
		return nil, retErr
	}

	retErr = nil
	return results, retErr
}

// AddStage adds a new stage to the pipeline. It checks that the stage is not nil and that a stage with the same name does not already exist in the pipeline.
func (p *Pipeline[T]) AddStage(stage Stage[T]) error {
	// Lock the pipeline for writing to ensure thread safety when modifying the stages map.
	p.mu.Lock()
	defer p.mu.Unlock()

	if stage == nil {
		return ErrNilStage
	}
	if stage.Name() == "" {
		return ErrStageNameEmpty
	}
	if stage.Workers() <= 0 {
		return ErrStageInvalidWorkerCount(stage.Name(), stage.Workers())
	}
	if _, exists := p.stages[stage.Name()]; exists {
		return fmt.Errorf("%w: %s", ErrStageExists, stage.Name())
	}

	p.stages[stage.Name()] = stage
	return nil
}

// Connect creates a directed edge from the stage named "from" to the stage named "to". This indicates that the output of the "from" stage will be passed as input to the "to" stage. The method checks that "from" and "to" are distinct stages and that both stages exist.
func (p *Pipeline[T]) Connect(from, to string) error {
	// Lock the pipeline for writing to ensure thread safety when modifying the edges map.
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, okFrom := p.stages[from]; !okFrom {
		return StageNotFound(from)
	}
	if _, okTo := p.stages[to]; !okTo {
		return StageNotFound(to)
	}
	if slices.Contains(p.edges[from], to) {
		return fmt.Errorf("%w: %s -> %s", ErrEdgeExists, from, to)
	}

	p.edges[from] = append(p.edges[from], to)
	return nil
}

// Validate checks that the pipeline configuration is valid. It ensures that all stages referenced in the edges exist in the stages map. It also checks for cycles in the graph.
func (p *Pipeline[T]) Validate() error {
	// Lock the pipeline for reading to ensure thread safety when accessing the stages and edges maps.
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.validateSnapshot(p.stages, p.edges, false)
}

func (p *Pipeline[T]) validateSnapshot(stages map[string]Stage[T], edges map[string][]string, allowCycles bool) error {
	stageNames := make(map[string]struct{}, len(stages))
	for name := range stages {
		stageNames[name] = struct{}{}
	}

	return graph.ValidateSnapshotMapped(
		stageNames,
		edges,
		allowCycles,
		func() error { return ErrNoStages },
		StageNotFound,
		func() error { return ErrCycle },
	)
}
