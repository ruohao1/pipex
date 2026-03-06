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
	// RunMeta intentionally excludes per-stage runtime internals such as
	// effective worker counts or WithStageWorkers override details.
}

type Hooks[T any] struct {
	RunStart func(ctx context.Context, meta RunMeta)
	RunEnd   func(ctx context.Context, meta RunMeta, err error)

	StageStart        func(ctx context.Context, e StageStartEvent[T])
	StageFinish       func(ctx context.Context, e StageFinishEvent[T])
	StageError        func(ctx context.Context, e StageErrorEvent[T])
	StageAttemptStart func(ctx context.Context, e StageAttemptStartEvent[T])
	StageAttemptError func(ctx context.Context, e StageAttemptErrorEvent[T])
	StageRetry        func(ctx context.Context, e StageRetryEvent[T])
	StageTimeout      func(ctx context.Context, e StageTimeoutEvent[T])
	StageExhausted    func(ctx context.Context, e StageExhaustedEvent[T])

	TriggerStart func(ctx context.Context, e TriggerStartEvent[T])
	TriggerEnd   func(ctx context.Context, e TriggerEndEvent[T])
	TriggerError func(ctx context.Context, e TriggerErrorEvent[T])

	SinkConsumeStart   func(ctx context.Context, e SinkConsumeStartEvent[T])
	SinkConsumeSuccess func(ctx context.Context, e SinkConsumeSuccessEvent[T])
	SinkRetry          func(ctx context.Context, e SinkRetryEvent[T])
	SinkExhausted      func(ctx context.Context, e SinkExhaustedEvent[T])

	CycleHopLimitDrop    func(ctx context.Context, e CycleHopLimitDropEvent[T])
	CycleMaxJobsExceeded func(ctx context.Context, e CycleMaxJobsExceededEvent[T])

	DedupDrop func(ctx context.Context, e DedupDropEvent[T])

	FrontierEnqueue func(ctx context.Context, e FrontierEnqueueEvent[T])
	FrontierReserve func(ctx context.Context, e FrontierReserveEvent[T])
	FrontierAck     func(ctx context.Context, e FrontierAckEvent[T])
	FrontierRetry   func(ctx context.Context, e FrontierRetryEvent[T])
}

// Stage events
type StageStartEvent[T any] struct {
	RunID     string
	Stage     string
	Input     T
	StartedAt time.Time
}

type StageFinishEvent[T any] struct {
	RunID      string
	Stage      string
	Input      T
	OutCount   int
	StartedAt  time.Time
	FinishedAt time.Time
	Duration   time.Duration
}

type StageErrorEvent[T any] struct {
	RunID      string
	Stage      string
	Input      T
	StartedAt  time.Time
	FinishedAt time.Time
	Duration   time.Duration
	Err        error
}

type StageAttemptStartEvent[T any] struct {
	RunID     string
	Stage     string
	Input     T
	Attempt   int
	StartedAt time.Time
}

type StageAttemptErrorEvent[T any] struct {
	RunID      string
	Stage      string
	Input      T
	Attempt    int
	StartedAt  time.Time
	FinishedAt time.Time
	Duration   time.Duration
	Err        error
}

type StageRetryEvent[T any] struct {
	RunID   string
	Stage   string
	Input   T
	Attempt int
	Err     error
	Backoff time.Duration
	At      time.Time
}

type StageTimeoutEvent[T any] struct {
	RunID     string
	Stage     string
	Input     T
	Attempt   int
	StartedAt time.Time
	Timeout   time.Duration
	At        time.Time
}

type StageExhaustedEvent[T any] struct {
	RunID    string
	Stage    string
	Input    T
	Attempts int
	Err      error
	At       time.Time
}

// Trigger events

type TriggerStartEvent[T any] struct {
	RunID     string
	Trigger   string
	Stage     string
	StartedAt time.Time
}

type TriggerEndEvent[T any] struct {
	RunID      string
	Trigger    string
	Stage      string
	StartedAt  time.Time
	FinishedAt time.Time
	Duration   time.Duration
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

// Sink events
type SinkConsumeStartEvent[T any] struct {
	RunID     string
	Sink      string
	Stage     string
	Item      T
	Attempt   int
	StartedAt time.Time
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

type SinkExhaustedEvent[T any] struct {
	RunID    string
	Sink     string
	Stage    string
	Item     T
	Attempts int
	Err      error
	At       time.Time
}

// CycleHopLimitDropEvent is emitted when an enqueue is dropped because hop
// count exceeded the configured cycle-mode MaxHops.
type CycleHopLimitDropEvent[T any] struct {
	RunID   string
	Stage   string
	Item    T
	Hops    int
	MaxHops int
	At      time.Time
}

// CycleMaxJobsExceededEvent is emitted when an enqueue is rejected because the
// configured cycle-mode MaxJobs budget is exhausted.
type CycleMaxJobsExceededEvent[T any] struct {
	RunID        string
	Stage        string
	Item         T
	AcceptedJobs int
	MaxJobs      int
	At           time.Time
}

// DedupDropEvent is emitted when an enqueue is dropped due to a deduplication rule, outside of cycle-mode.
type DedupDropEvent[T any] struct {
	RunID string
	Scope DedupScope
	Key   string
	Item  T
	At    time.Time
}

type FrontierEnqueueEvent[T any] struct {
	RunID   string
	EntryID uint64
	Stage   string
	Item    T
	Hops    int
	At      time.Time
}

type FrontierReserveEvent[T any] struct {
	RunID   string
	EntryID uint64
	Stage   string
	Input   T
	Hops    int
	Attempt int
	At      time.Time
}

type FrontierAckEvent[T any] struct {
	RunID   string
	EntryID uint64
	Stage   string
	Input   T
	Hops    int
	Attempt int
	At      time.Time
}

type FrontierRetryEvent[T any] struct {
	RunID   string
	EntryID uint64
	Stage   string
	Input   T
	Hops    int
	Attempt int
	Err     error
	At      time.Time
}
