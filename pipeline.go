package pipex

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"
)

type Pipeline[T any] struct {
	stages map[string]Stage[T]
	edges  map[string][]string
	mu     sync.RWMutex
}

// NewPipeline creates a new instance of Pipeline.
func NewPipeline[T any]() *Pipeline[T] {
	return &Pipeline[T]{
		stages: make(map[string]Stage[T]),
		edges:  make(map[string][]string),
	}
}

// Run executes the pipeline with the given seeds. The seeds map specifies the initial input items for each stage. The method returns a map of stage names to their output items, or an error if the pipeline configuration is invalid or if any stage processing fails.
func (p *Pipeline[T]) Run(ctx context.Context, seeds map[string][]T, opts ...Option[T]) (map[string][]T, error) {
	runOpts := defaultOptions[T]()
	for _, opt := range opts {
		opt(runOpts)
	}

	p.mu.RLock()
	stages := make(map[string]Stage[T], len(p.stages))
	maps.Copy(stages, p.stages)
	edges := make(map[string][]string, len(p.edges))
	for k, v := range p.edges {
		edges[k] = append([]string(nil), v...)
	}
	p.mu.RUnlock()

	if err := p.validateSnapshot(stages, edges); err != nil {
		return nil, err
	}

	for seedStage := range seeds {
		if _, ok := stages[seedStage]; !ok {
			return nil, StageNotFound(seedStage)
		}
	}
	for _, trigger := range runOpts.Triggers {
		if _, ok := stages[trigger.Stage()]; !ok {
			return nil, fmt.Errorf("trigger %s: %w", trigger.Name(), StageNotFound(trigger.Stage()))
		}
	}
	for _, sink := range runOpts.Sinks {
		if _, ok := stages[sink.Stage()]; !ok {
			return nil, fmt.Errorf("sink %s: %w", sink.Name(), StageNotFound(sink.Stage()))
		}
	}

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	results := make(map[string][]T)
	var resultsMu sync.Mutex
	var tasksWG sync.WaitGroup
	var poolErrWG sync.WaitGroup
	var triggersWG sync.WaitGroup
	var sinksWG sync.WaitGroup

	jobsByStage := make(map[string]chan Job, len(stages))
	sinksByStage := make(map[string][]chan T)

	var errsMu sync.Mutex
	var runErrs []error
	var errOnce sync.Once

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	runCtx := ctx

	isContextErr := func(err error) bool {
		return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
	}

	addErr := func(err error) {
		if err == nil {
			return
		}
		errsMu.Lock()
		runErrs = append(runErrs, err)
		errsMu.Unlock()
	}

	recordErr := func(err error) {
		if err == nil || isContextErr(err) {
			return
		}
		if runOpts.FailFast {
			errOnce.Do(func() {
				addErr(err)
				cancel()
			})
			return
		}
		addErr(err)
	}

	for _, sink := range runOpts.Sinks {
		sinkCh := make(chan T, runOpts.BufferSize)
		sinksByStage[sink.Stage()] = append(sinksByStage[sink.Stage()], sinkCh)

		sinksWG.Add(1)
		go func(sink Sink[T], sinkCh chan T) {
			defer sinksWG.Done()
			disabled := false
			for {
				select {
				case <-ctx.Done():
					return
				case item, ok := <-sinkCh:
					if !ok {
						return
					}
					if disabled {
						continue
					}

					attempts := 0
					for {
						err := sink.Consume(ctx, item)
						if err == nil {
							break
						}
						attempts++

						if runOpts.SinkRetry.MaxRetries >= 0 && attempts > runOpts.SinkRetry.MaxRetries {
							recordErr(fmt.Errorf("sink %s: retries exhausted: %w", sink.Name(), err))
							disabled = true
							break
						}

						if ctx.Err() != nil {
							recordErr(fmt.Errorf("sink %s: %w", sink.Name(), err))
							return
						}

						select {
						case <-ctx.Done():
							return
						case <-time.After(runOpts.SinkRetry.Backoff):
						}
					}
				}
			}
		}(sink, sinkCh)
	}

	for stageName, stage := range stages {
		stageJobs := make(chan Job, runOpts.BufferSize)
		jobsByStage[stageName] = stageJobs

		pool, err := NewPool(PoolConfig{
			Workers: stage.Workers(),
			Queue:   runOpts.BufferSize,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create worker pool for stage %s: %w", stageName, err)
		}

		poolErrWG.Add(1)
		// Keep stage workers alive until stage queues are closed so queued jobs
		// can still call tasksWG.Done() even after run context cancellation.
		poolErrCh := pool.Run(context.Background(), stageJobs)
		go func(poolErrCh <-chan error) {
			defer poolErrWG.Done()
			for err := range poolErrCh {
				recordErr(err)
			}
		}(poolErrCh)
	}

	var enqueue func(stageName string, in T) error
	enqueue = func(stageName string, in T) error {
		stageJobs, ok := jobsByStage[stageName]
		if !ok {
			return StageNotFound(stageName)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		tasksWG.Add(1)
		job := Job{
			Name: fmt.Sprintf("%s_%v", stageName, in),
			Run: func(_ context.Context) error {
				defer tasksWG.Done()
				if runCtx.Err() != nil {
					return runCtx.Err()
				}

				stage := stages[stageName]
				out, err := stage.Process(runCtx, in)
				if err != nil {
					return fmt.Errorf("stage %s: %w", stageName, err)
				}

				resultsMu.Lock()
				results[stageName] = append(results[stageName], out...)
				resultsMu.Unlock()

				for _, item := range out {
					for _, sinkCh := range sinksByStage[stageName] {
						select {
						case <-runCtx.Done():
							return runCtx.Err()
						case sinkCh <- item:
						}
					}
				}

				for _, nextStage := range edges[stageName] {
					for _, item := range out {
						if err := enqueue(nextStage, item); err != nil {
							return fmt.Errorf("enqueue to %s: %w", nextStage, err)
						}
					}
				}

				return nil
			},
		}

		select {
		case <-ctx.Done():
			tasksWG.Done()
			return ctx.Err()
		case stageJobs <- job:
			return nil
		}
	}

	for stageName, inputs := range seeds {
		for _, input := range inputs {
			if err := enqueue(stageName, input); err != nil {
				recordErr(fmt.Errorf("enqueue seed %s: %w", stageName, err))
			}
		}
	}

	for _, trigger := range runOpts.Triggers {
		triggersWG.Add(1)
		go func(trigger Trigger[T]) {
			defer triggersWG.Done()
			emit := func(item T) error {
				if err := enqueue(trigger.Stage(), item); err != nil {
					return fmt.Errorf("trigger enqueue %s: %w", trigger.Name(), err)
				}
				return nil
			}
			if err := trigger.Start(ctx, emit); err != nil {
				recordErr(fmt.Errorf("trigger %s: %w", trigger.Name(), err))
			}
		}(trigger)
	}

	triggersWG.Wait()
	tasksWG.Wait()
	for _, jobsCh := range jobsByStage {
		close(jobsCh)
	}
	poolErrWG.Wait()
	for _, sinkChs := range sinksByStage {
		for _, sinkCh := range sinkChs {
			close(sinkCh)
		}
	}
	sinksWG.Wait()

	errsMu.Lock()
	joinedErr := errors.Join(runErrs...)
	errsMu.Unlock()
	if joinedErr != nil {
		if runOpts.ReturnPartialResults {
			return results, joinedErr
		}
		return nil, joinedErr
	}

	if err := ctx.Err(); err != nil {
		if runOpts.ReturnPartialResults {
			return results, err
		}
		return nil, err
	}

	return results, nil
}

// AddStage adds a new stage to the pipeline. It checks that the stage is not nil and that a stage with the same name does not already exist in the pipeline.
func (p *Pipeline[T]) AddStage(stage Stage[T]) error {
	// Lock the pipeline for writing to ensure thread safety when modifying the stages map.
	p.mu.Lock()
	defer p.mu.Unlock()

	if stage == nil {
		return ErrNilStage
	}
	if stage.Name() == "" {
		return ErrStageNameEmpty
	}
	if stage.Workers() <= 0 {
		return ErrStageInvalidWorkerCount(stage.Name(), stage.Workers())
	}
	if _, exists := p.stages[stage.Name()]; exists {
		return fmt.Errorf("%w: %s", ErrStageExists, stage.Name())
	}

	p.stages[stage.Name()] = stage
	return nil
}

// Connect creates a directed edge from the stage named "from" to the stage named "to". This indicates that the output of the "from" stage will be passed as input to the "to" stage. The method checks that "from" and "to" are distinct stages and that both stages exist.
func (p *Pipeline[T]) Connect(from, to string) error {
	// Lock the pipeline for writing to ensure thread safety when modifying the edges map.
	p.mu.Lock()
	defer p.mu.Unlock()
	if from == to {
		return ErrCycle
	}
	if _, okFrom := p.stages[from]; !okFrom {
		return StageNotFound(from)
	}
	if _, okTo := p.stages[to]; !okTo {
		return StageNotFound(to)
	}
	if slices.Contains(p.edges[from], to) {
		return fmt.Errorf("%w: %s -> %s", ErrEdgeExists, from, to)
	}

	p.edges[from] = append(p.edges[from], to)
	return nil
}

// Validate checks that the pipeline configuration is valid. It ensures that all stages referenced in the edges exist in the stages map. It also checks for cycles in the graph.
func (p *Pipeline[T]) Validate() error {
	// Lock the pipeline for reading to ensure thread safety when accessing the stages and edges maps.
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.validateSnapshot(p.stages, p.edges)
}

func (p *Pipeline[T]) validateSnapshot(stages map[string]Stage[T], edges map[string][]string) error {
	// Check that there is at least one stage
	if len(stages) == 0 {
		return ErrNoStages
	}

	// Check that all stages referenced in edges exist
	for from, tos := range edges {
		if _, ok := stages[from]; !ok {
			return StageNotFound(from)
		}
		for _, to := range tos {
			if _, ok := stages[to]; !ok {
				return StageNotFound(to)
			}
		}
	}

	// Check for cycles using depth-first search
	visited := make(map[string]bool)
	recStack := make(map[string]bool)

	var dfs func(string) bool
	dfs = func(node string) bool {
		if recStack[node] {
			return true // cycle detected
		}
		if visited[node] {
			return false // already visited, no cycle from this node
		}

		visited[node] = true
		recStack[node] = true

		if slices.ContainsFunc(edges[node], dfs) {
			return true
		}

		recStack[node] = false
		return false
	}

	for stage := range stages {
		if !visited[stage] {
			if dfs(stage) {
				return ErrCycle
			}
		}
	}

	return nil
}
