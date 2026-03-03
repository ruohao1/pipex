package pipex

type RunOptions[T any] struct {
  	BufferSize int
  	FailFast   bool
  	Triggers   []Trigger[T]
		ReturnPartialResults bool
  }

type Option[T any] func(*RunOptions[T])

func defaultOptions[T any]() *RunOptions[T] {
	return &RunOptions[T]{
		BufferSize: 1024,
		FailFast:   false,
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

func WithPartialResults[T any](v bool) Option[T] {
  	return func(opts *RunOptions[T]) {
  		opts.ReturnPartialResults = v
  	}
  }
