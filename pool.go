package pipex

import (
	"context"
	"sync"
)

type PoolConfig struct {
	Workers int
	Queue   int
	// DrainOnCancel keeps workers consuming queued jobs after ctx cancellation
	// until jobs channel is closed. This is useful for callers that enqueue
	// jobs tracked by a waitgroup and must guarantee every queued job runs its
	// cleanup path.
	DrainOnCancel bool
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
			draining := false
			for {
				if draining {
					job, ok := <-jobs
					if !ok {
						return
					}
					if err := job.Exec(ctx); err != nil {
						// Context is canceled in drain mode; report best-effort and continue.
						select {
						case errs <- err:
						default:
						}
					}
					continue
				}

				select {
				case <-ctx.Done():
					if p.cfg.DrainOnCancel {
						draining = true
						continue
					}
					return
				case job, ok := <-jobs:
					if !ok {
						return
					}
					if err := job.Exec(ctx); err != nil {
						// If context is already canceled, prefer best-effort reporting of
						// the current job error before worker exit.
						if ctx.Err() != nil {
							if p.cfg.DrainOnCancel {
								select {
								case errs <- err:
								default:
								}
								draining = true
								continue
							}
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
