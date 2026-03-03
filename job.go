package pipex

import "context"

type JobFunc func(context.Context) error

type Job struct {
	Name string
	Run  JobFunc
}

func (j Job) Exec(ctx context.Context) error {
	if j.Run == nil {
		return ErrNilJobFunc
	}
	return j.Run(ctx)
}
