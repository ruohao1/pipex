package pipex

type RunOptions struct {
	BufferSize int 
	FailFast bool
}

type Option func(*RunOptions)

func defaultOptions() *RunOptions {
	return &RunOptions{
		BufferSize: 1024,
		FailFast:   false,
	}
}

func WithBufferSize(size int) Option {
	return func(opts *RunOptions) {
		if size <= 0 {
			return
		}
		opts.BufferSize = size
	}
}

func WithFailFast(failFast bool) Option {
	return func(opts *RunOptions) {
		opts.FailFast = failFast
	}
}


