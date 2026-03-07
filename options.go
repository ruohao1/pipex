package pipex

import (
	"maps"
	"time"
)

type RunOptions[T any] struct {
	BufferSize              int
	FailFast                bool
	UseFrontier             bool
	FrontierPendingCap      int
	FrontierBlockingEnqueue bool
	FrontierStatsInterval   time.Duration
	Triggers                []Trigger[T]
	Sinks                   []Sink[T]
	Hooks                   Hooks[T]
	CycleMode               CycleModeOptions[T]
	SinkRetry               SinkRetryPolicy
	ReturnPartialResults    bool
	StageWorkers            map[string]int
	StageRateLimits         map[string]RateLimit
	StagePolicies           map[string]StagePolicy
	DedupRules              []DedupRule[T]
}

type CycleModeOptions[T any] struct {
	Enabled bool
	MaxHops int // -1 means unlimited
	MaxJobs int // must be > 0 when enabled
}

type SinkRetryPolicy struct {
	MaxRetries int           // -1 means infinite
	Backoff    time.Duration // e.g. 10ms
}

type RateLimit struct {
	RPS   float64
	Burst int
}

type StagePolicy struct {
	MaxAttempts int
	Backoff     time.Duration
	Timeout     time.Duration
}

type DedupScope string

const (
	DedupScopeGlobal DedupScope = "global"
)

func DedupScopeStage(stageName string) DedupScope {
	return DedupScope("stage:" + stageName)
}

type DedupRule[T any] struct {
	Name  string
	Scope DedupScope
	Key   func(T) string
	// TTL   time.Duration
}

type Option[T any] func(*RunOptions[T])

func defaultOptions[T any]() *RunOptions[T] {
	return &RunOptions[T]{
		BufferSize:              1024,
		FailFast:                false,
		UseFrontier:             false,
		FrontierPendingCap:      0,
		FrontierBlockingEnqueue: false,
		FrontierStatsInterval:   0,
		Triggers:                []Trigger[T]{},
		Sinks:                   []Sink[T]{},
		Hooks:                   Hooks[T]{},
		CycleMode: CycleModeOptions[T]{
			Enabled: false,
			MaxHops: -1,
			MaxJobs: 0,
		},
		SinkRetry: SinkRetryPolicy{
			MaxRetries: 10,
			Backoff:    10 * time.Millisecond,
		},
		ReturnPartialResults: false,
		StageWorkers:         map[string]int{},
		StageRateLimits:      map[string]RateLimit{},
		StagePolicies:        map[string]StagePolicy{},
		DedupRules:           []DedupRule[T]{},
	}
}

func WithBufferSize[T any](size int) Option[T] {
	return func(opts *RunOptions[T]) {
		if size <= 0 {
			return
		}
		opts.BufferSize = size
	}
}

func WithFailFast[T any](failFast bool) Option[T] {
	return func(opts *RunOptions[T]) {
		opts.FailFast = failFast
	}
}

func WithFrontier[T any](enabled bool) Option[T] {
	return func(opts *RunOptions[T]) {
		opts.UseFrontier = enabled
	}
}

// WithFrontierPendingCapacity sets in-memory frontier pending queue capacity.
// Values <= 0 are ignored and keep automatic sizing.
func WithFrontierPendingCapacity[T any](n int) Option[T] {
	return func(opts *RunOptions[T]) {
		if n <= 0 {
			return
		}
		opts.FrontierPendingCap = n
	}
}

// WithFrontierBlockingEnqueue enables blocking, context-aware enqueue in frontier mode.
func WithFrontierBlockingEnqueue[T any](enabled bool) Option[T] {
	return func(opts *RunOptions[T]) {
		opts.FrontierBlockingEnqueue = enabled
	}
}

// WithFrontierStatsInterval configures sampled frontier stats emission interval.
// Values <= 0 disable sampled frontier stats.
func WithFrontierStatsInterval[T any](interval time.Duration) Option[T] {
	return func(opts *RunOptions[T]) {
		if interval <= 0 {
			opts.FrontierStatsInterval = 0
			return
		}
		opts.FrontierStatsInterval = interval
	}
}

func WithTriggers[T any](triggers ...Trigger[T]) Option[T] {
	return func(opts *RunOptions[T]) {
		opts.Triggers = append(opts.Triggers, triggers...)
	}
}

func WithSinks[T any](sinks ...Sink[T]) Option[T] {
	return func(opts *RunOptions[T]) {
		opts.Sinks = append(opts.Sinks, sinks...)
	}
}

func WithHooks[T any](hooks Hooks[T]) Option[T] {
	return func(opts *RunOptions[T]) {
		opts.Hooks = hooks
	}
}

// WithCycleMode configures cycle traversal guardrails.
func WithCycleMode[T any](maxHops, maxJobs int) Option[T] {
	return func(opts *RunOptions[T]) {
		opts.CycleMode = CycleModeOptions[T]{
			Enabled: true,
			MaxHops: maxHops,
			MaxJobs: maxJobs,
		}
	}
}

func WithSinkRetry[T any](maxRetries int, backoff time.Duration) Option[T] {
	return func(opts *RunOptions[T]) {
		if maxRetries < -1 {
			return
		}
		if backoff < 0 {
			return
		}
		if backoff == 0 {
			backoff = time.Millisecond
		}
		opts.SinkRetry = SinkRetryPolicy{
			MaxRetries: maxRetries,
			Backoff:    backoff,
		}
	}
}

func WithPartialResults[T any](v bool) Option[T] {
	return func(opts *RunOptions[T]) {
		opts.ReturnPartialResults = v
	}
}

func WithStageWorkers[T any](stageWorkers map[string]int) Option[T] {
	return func(opts *RunOptions[T]) {
		stageWorkersCopy := make(map[string]int, len(stageWorkers))
		maps.Copy(stageWorkersCopy, stageWorkers)
		opts.StageWorkers = stageWorkersCopy
	}
}

func WithStageRateLimits[T any](stageRateLimits map[string]RateLimit) Option[T] {
	return func(opts *RunOptions[T]) {
		stageRateLimitsCopy := make(map[string]RateLimit, len(stageRateLimits))
		maps.Copy(stageRateLimitsCopy, stageRateLimits)
		opts.StageRateLimits = stageRateLimitsCopy
	}
}

func WithStagePolicies[T any](stagePolicies map[string]StagePolicy) Option[T] {
	return func(opts *RunOptions[T]) {
		stagePoliciesCopy := make(map[string]StagePolicy, len(stagePolicies))
		maps.Copy(stagePoliciesCopy, stagePolicies)
		opts.StagePolicies = stagePoliciesCopy
	}
}

func WithDedupRules[T any](rules ...DedupRule[T]) Option[T] {
	return func(opts *RunOptions[T]) {
		opts.DedupRules = append(opts.DedupRules, rules...)
	}
}
