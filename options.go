package pipex

import (
	"maps"
	"time"
)

type RunOptions[T any] struct {
	BufferSize           int
	FailFast             bool
	Triggers             []Trigger[T]
	Sinks                []Sink[T]
	Hooks                Hooks[T]
	CycleMode            CycleModeOptions[T]
	SinkRetry            SinkRetryPolicy
	ReturnPartialResults bool
	StageWorkers         map[string]int
	StageRateLimits      map[string]RateLimit
	DedupRules           []DedupRule[T]
}

type CycleModeOptions[T any] struct {
	Enabled bool
	MaxHops int // -1 means unlimited
	MaxJobs int // must be > 0 when enabled
	// Deprecated: use WithDedupRules(...) for new dedup configuration.
	// This field remains supported for backward compatibility and is internally
	// translated into runtime dedup rules.
	DedupKey func(T) string // nil means dedup disabled
}

type SinkRetryPolicy struct {
	MaxRetries int           // -1 means infinite
	Backoff    time.Duration // e.g. 10ms
}

type RateLimit struct {
	RPS   float64
	Burst int
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
		BufferSize: 1024,
		FailFast:   false,
		Triggers:   []Trigger[T]{},
		Sinks:      []Sink[T]{},
		Hooks:      Hooks[T]{},
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
//
// Deprecated: the dedupKey parameter is deprecated; configure dedup using
// WithDedupRules(...) instead. dedupKey remains supported for backward
// compatibility and uses the same runtime dedup path internally.
func WithCycleMode[T any](maxHops, maxJobs int, dedupKey func(T) string) Option[T] {
	return func(opts *RunOptions[T]) {
		opts.CycleMode = CycleModeOptions[T]{
			Enabled:  true,
			MaxHops:  maxHops,
			MaxJobs:  maxJobs,
			DedupKey: dedupKey,
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

func WithDedupRules[T any](rules ...DedupRule[T]) Option[T] {
	return func(opts *RunOptions[T]) {
		opts.DedupRules = append(opts.DedupRules, rules...)
	}
}
