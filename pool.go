package pipex

import (
	"context"
	"sync"
)

type PoolConfig struct {
	Workers int
	Queue   int
}
type Pool struct{ cfg PoolConfig }

func NewPool(cfg PoolConfig) (*Pool, error) {
	if cfg.Workers <= 0 {
		return nil, ErrInvalidWorkerCount
	}
	if cfg.Queue < 0 {
		return nil, ErrInvalidQueueSize
	}

	return &Pool{cfg: cfg}, nil
}

func (p *Pool) Run(ctx context.Context, jobs <-chan Job) <-chan error {
	errs := make(chan error, p.cfg.Queue)

	var wg sync.WaitGroup
	wg.Add(p.cfg.Workers)

	for i := 0; i < p.cfg.Workers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case job, ok := <-jobs:
					if !ok {
						return
					}
					if err := job.Exec(ctx); err != nil {
						// If context is already canceled, prefer best-effort reporting of
						// the current job error before worker exit.
						if ctx.Err() != nil {
							select {
							case errs <- err:
							default:
							}
							return
						}
						select {
						case errs <- err:
						case <-ctx.Done():
							return
						}
					}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(errs)
	}()

	return errs
}
