package pipex

import (
	"context"
	"time"
)

type RunMeta struct {
	RunID        string
	SeedStages   int
	SeedItems    int
	StageCount   int
	EdgeCount    int
	TriggerCount int
	SinkCount    int
	FailFast     bool
	BufferSize   int
}

type Hooks[T any] struct {
	RunStart func(ctx context.Context, meta RunMeta)
	RunEnd   func(ctx context.Context, meta RunMeta, err error)

	StageStart  func(ctx context.Context, e StageStartEvent[T])
	StageFinish func(ctx context.Context, e StageFinishEvent[T])
	StageError  func(ctx context.Context, e StageErrorEvent[T])

	TriggerStart func(ctx context.Context, e TriggerStartEvent[T])
	TriggerEnd   func(ctx context.Context, e TriggerEndEvent[T])
	TriggerError func(ctx context.Context, e TriggerErrorEvent[T])

	SinkConsumeStart   func(ctx context.Context, e SinkConsumeStartEvent[T])
	SinkConsumeSuccess func(ctx context.Context, e SinkConsumeSuccessEvent[T])
	SinkRetry          func(ctx context.Context, e SinkRetryEvent[T])
	SinkExhausted      func(ctx context.Context, e SinkExhaustedEvent[T])
	SinkError          func(ctx context.Context, e SinkErrorEvent[T])
}

func safeCall(fn func()) {
  	defer func() {
  		_ = recover()
  	}()
  	fn()
  }

func emitRunStart[T any](ctx context.Context, h Hooks[T], meta RunMeta) {
	if h.RunStart == nil {
		return
	}
	safeCall(func() { h.RunStart(ctx, meta) })
}

func emitRunEnd[T any](ctx context.Context, h Hooks[T], meta RunMeta, e error) {
	if h.RunEnd == nil {
		return
	}
	safeCall(func() { h.RunEnd(ctx, meta, e) })
}

// Stage events
type StageStartEvent[T any] struct {
	RunID     string
	Stage     string
	Input      T
	StartedAt time.Time
}

func emitStageStart[T any](ctx context.Context, h Hooks[T], e StageStartEvent[T]) {
	if h.StageStart == nil {
		return
	}
	safeCall(func() { h.StageStart(ctx, e) })
}

type StageFinishEvent[T any] struct {
	RunID      string
	Stage      string
	Input       T
	OutCount 	int
	StartedAt  time.Time
	FinishedAt time.Time
	Duration   time.Duration
}

func emitStageFinish[T any](ctx context.Context, h Hooks[T], e StageFinishEvent[T]) {
	if h.StageFinish == nil {
		return
	}
	safeCall(func() { h.StageFinish(ctx, e) })
}

type StageErrorEvent[T any] struct {
	RunID      string
	Stage      string
	Input       T
	StartedAt  time.Time
	FinishedAt time.Time
	Duration   time.Duration
	Err        error
}

func emitStageError[T any](ctx context.Context, h Hooks[T], e StageErrorEvent[T]) {
	if h.StageError == nil {
		return
	}
	safeCall(func() { h.StageError(ctx, e) })
}

// Trigger events

type TriggerStartEvent[T any] struct {
	RunID     string
	Trigger   string
	Stage     string
	StartedAt time.Time
}

func emitTriggerStart[T any](ctx context.Context, h Hooks[T], e TriggerStartEvent[T]) {
	if h.TriggerStart == nil {
		return
	}
	safeCall(func() { h.TriggerStart(ctx, e) })
}

type TriggerEndEvent[T any] struct {
	RunID      string
	Trigger    string
	Stage      string
	StartedAt  time.Time
	FinishedAt time.Time
	Duration   time.Duration
}

func emitTriggerEnd[T any](ctx context.Context, h Hooks[T], e TriggerEndEvent[T]) {
	if h.TriggerEnd == nil {
		return
	}
	safeCall(func() { h.TriggerEnd(ctx, e) })
}

type TriggerErrorEvent[T any] struct {
	RunID      string
	Trigger    string
	Stage      string
	Err        error
	StartedAt  time.Time
	FinishedAt time.Time
	Duration   time.Duration
}

func emitTriggerError[T any](ctx context.Context, h Hooks[T], e TriggerErrorEvent[T]) {
	if h.TriggerError == nil {
		return
	}
	safeCall(func() { h.TriggerError(ctx, e) })
}

// Sink events
type SinkConsumeStartEvent[T any] struct {
	RunID     string
	Sink      string
	Stage     string
	Item      T
	Attempt   int
	StartedAt time.Time
}

func emitSinkConsumeStart[T any](ctx context.Context, h Hooks[T], e SinkConsumeStartEvent[T]) {
	if h.SinkConsumeStart == nil {
		return
	}
	safeCall(func() { h.SinkConsumeStart(ctx, e) })
}

type SinkConsumeSuccessEvent[T any] struct {
	RunID      string
	Sink       string
	Stage      string
	Item       T
	Attempt    int
	StartedAt  time.Time
	FinishedAt time.Time
	Duration   time.Duration
}

func emitSinkConsumeSuccess[T any](ctx context.Context, h Hooks[T], e SinkConsumeSuccessEvent[T]) {
	if h.SinkConsumeSuccess == nil {
		return
	}
	safeCall(func() { h.SinkConsumeSuccess(ctx, e) })
}

type SinkRetryEvent[T any] struct {
	RunID   string
	Sink    string
	Stage   string
	Item    T
	Attempt int
	Err     error
	Backoff time.Duration
	At      time.Time
}

func emitSinkRetry[T any](ctx context.Context, h Hooks[T], e SinkRetryEvent[T]) {
	if h.SinkRetry == nil {
		return
	}
	safeCall(func() { h.SinkRetry(ctx, e) })
}

type SinkExhaustedEvent[T any] struct {
	RunID    string
	Sink     string
	Stage    string
	Item     T
	Attempts int
	Err      error
	At       time.Time
}

func emitSinkExhausted[T any](ctx context.Context, h Hooks[T], e SinkExhaustedEvent[T]) {
	if h.SinkExhausted == nil {
		return
	}
	safeCall(func() { h.SinkExhausted(ctx, e) })
}

type SinkErrorEvent[T any] struct {
	RunID   string
	Sink    string
	Stage   string
	Item    T
	Err     error
	Attempt int
	At      time.Time
}

func emitSinkError[T any](ctx context.Context, h Hooks[T], e SinkErrorEvent[T]) {
	if h.SinkError == nil {
		return
	}
	safeCall(func() { h.SinkError(ctx, e) })
}
