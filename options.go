package pipex

import "time"

type RunOptions[T any] struct {
	BufferSize           int
	FailFast             bool
	Triggers             []Trigger[T]
	Sinks                []Sink[T]
	Hooks								Hooks[T]
	SinkRetry            SinkRetryPolicy
	ReturnPartialResults bool
}

type SinkRetryPolicy struct {
	MaxRetries int           // -1 means infinite
	Backoff    time.Duration // e.g. 10ms
}

type Option[T any] func(*RunOptions[T])

func defaultOptions[T any]() *RunOptions[T] {
	return &RunOptions[T]{
		BufferSize: 1024,
		FailFast:   false,
		Triggers:   []Trigger[T]{},
		Sinks:      []Sink[T]{},
		SinkRetry: SinkRetryPolicy{
			MaxRetries: 10,
			Backoff:    10 * time.Millisecond,
		},
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
		if len(triggers) == 0 {
			return
		}
		opts.Triggers = append(opts.Triggers, triggers...)
	}
}

func WithSinks[T any](sinks ...Sink[T]) Option[T] {
	return func(opts *RunOptions[T]) {
		if len(sinks) == 0 {
			return
		}
		opts.Sinks = append(opts.Sinks, sinks...)
	}
}

func WithHooks[T any](hooks Hooks[T]) Option[T] {
	return func(opts *RunOptions[T]) {
		opts.Hooks = hooks
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
