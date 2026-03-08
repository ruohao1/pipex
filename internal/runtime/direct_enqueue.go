package runtime

import "context"

type DirectEnqueueConfig[J any] struct {
	Ctx                  context.Context
	StageName            string
	LookupQueue          func(stage string) (chan J, bool)
	BuildStageNotFound   func(stage string) error
	BeforeSchedule       func()
	OnCanceledAfterBuild func()
	BuildJob             func(stage string) J
}

// EnqueueDirect resolves the stage queue, builds the job, and enqueues it
// with context-aware cancellation behavior.
func EnqueueDirect[J any](cfg DirectEnqueueConfig[J]) error {
	stageQueue, ok := cfg.LookupQueue(cfg.StageName)
	if !ok {
		return cfg.BuildStageNotFound(cfg.StageName)
	}
	if cfg.Ctx.Err() != nil {
		return cfg.Ctx.Err()
	}

	if cfg.BeforeSchedule != nil {
		cfg.BeforeSchedule()
	}
	job := cfg.BuildJob(cfg.StageName)

	select {
	case <-cfg.Ctx.Done():
		if cfg.OnCanceledAfterBuild != nil {
			cfg.OnCanceledAfterBuild()
		}
		return cfg.Ctx.Err()
	case stageQueue <- job:
		return nil
	}
}
