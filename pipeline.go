package pipex

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
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
func (p *Pipeline[T]) Run(ctx context.Context, seeds map[string][]T, opts ...Option) (map[string][]T, error) {
	runOpts := defaultOptions()
	for _, opt := range opts {
		opt(runOpts)
	}
	// Snapshot the pipeline configuration to avoid holding locks during execution
	// This allows the pipeline to be modified concurrently while it's running, but it also means that changes made to the pipeline after this point will not affect the current execution.
	p.mu.RLock()
	stages := make(map[string]Stage[T], len(p.stages))
	maps.Copy(stages, p.stages)
	edges := make(map[string][]string, len(p.edges))
	for k, v := range p.edges {
		edges[k] = append([]string(nil), v...)
	}
	p.mu.RUnlock()
	// Validate the pipeline configuration snapshot before starting execution. This ensures that we are working with a consistent view of the pipeline configuration and that any issues are caught early.
	if err := p.validateSnapshot(stages, edges); err != nil {
		return nil, err
	}

	// Validate that every seed stage exists in the pipeline
	for seedStage := range seeds {
		if _, ok := stages[seedStage]; !ok {
			return nil, ErrStageNotFound(seedStage)
		}
	}
	// Check if the context is already canceled before starting the execution
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// workItem represents a unit of work to be processed by a stage. It contains the name of the stage and the value to be processed.
	type workItem[T any] struct {
		name  string
		value T
	}
	// workersWG is used to wait for all worker goroutines to finish processing. tasksWG is used to track the number of tasks that are currently being processed. This allows us to know when all tasks have been completed and we can safely return the results.
	var workersWG sync.WaitGroup
	var tasksWG sync.WaitGroup

	// results will store the output items for each stage. It is protected by a mutex to allow concurrent access from multiple workers.
	results := make(map[string][]T)
	var resultsMu sync.Mutex

	// queue is a slice that holds the initial work items to be processed. It is initialized with the seeds provided for each stage.
	queue := make([]workItem[T], 0, runOpts.BufferSize)
	for stageName, inputs := range seeds {
		for _, input := range inputs {
			tasksWG.Add(1) // Increment the task count for each seed item
			queue = append(queue, workItem[T]{name: stageName, value: input})
		}
	}

	// inCh is a map of stage names to their corresponding input channels. Each stage will read from its input channel to receive work items to process.
	inCh := make(map[string]chan T)

	// Create a cancellable context to allow workers to stop processing when an error occurs or when the context is canceled. This ensures that we can gracefully shut down the pipeline execution in case of errors or cancellation.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var errsMu sync.Mutex
	var runErrs []error

	addErr := func(err error) {
		if err == nil {
			return
		}
		errsMu.Lock()
		runErrs = append(runErrs, err)
		errsMu.Unlock()
	}
	var errOnce sync.Once

	// Initialize channels for each stage
	// Seperate from the main loop to avoid holding locks while creating channels and starting workers.
	for stageName := range stages {
		inCh[stageName] = make(chan T, runOpts.BufferSize)
	}

	for stageName, stage := range stages {
		for i := 0; i < stage.Workers(); i++ {
			// Each worker goroutine will read from the input channel of its assigned stage, process the items, and send the results to the next stages as defined by the edges. The worker will also handle errors and update the results map concurrently.
			workersWG.Add(1)
			go func(stageName string, stage Stage[T]) {
				defer workersWG.Done()

				for in := range inCh[stageName] {
					// Check if the context has been canceled before processing each item. If it has, we can skip processing and exit the worker gracefully.
					if ctx.Err() != nil {
						tasksWG.Done()
						continue
					}

					// Process the input item using the stage's Process method. If an error occurs, we set the first error and stop processing further items.
					outs, err := stage.Process(ctx, in)
					if err != nil {
						wrapped := fmt.Errorf("stage %s: %w", stageName, err)
						if runOpts.FailFast {
							errOnce.Do(func() {
								addErr(wrapped)
								cancel()
							})
						} else {
							addErr(wrapped)
						}
						tasksWG.Done()
						continue
					}

					// Update the results map with the output items from this stage. We lock the mutex to ensure that updates to the results map are thread-safe.
					resultsMu.Lock()
					results[stageName] = append(results[stageName], outs...)
					resultsMu.Unlock()

					// Send the output items to the next stages as defined by the edges. We also check for context cancellation before sending items to avoid unnecessary work if the execution has been canceled.
					for _, out := range outs {
						for _, next := range edges[stageName] {
							tasksWG.Add(1)
							if ctx.Err() != nil {
								tasksWG.Done()
								continue
							}
							inCh[next] <- out
						}
					}

					tasksWG.Done()
				}
			}(stageName, stage)
		}
	}

	for len(queue) > 0 {
		for _, item := range queue {
			inCh[item.name] <- item.value
		}
		queue = queue[:0] // Clear the queue for the next iteration
	}

	// Start a goroutine to close all input channels once all tasks have been completed. This signals the workers that there are no more items to process and allows them to exit gracefully.
	go func() {
		tasksWG.Wait()
		for _, ch := range inCh {
			close(ch)
		}
	}()
	workersWG.Wait() // Wait for all workers to finish processing

	// Check if the context was canceled during processing. If so, return the context error. This ensures that we handle cancellation properly and do not return results if the execution was canceled.
	errsMu.Lock()
	joinedErr := errors.Join(runErrs...)
	errsMu.Unlock()
	if joinedErr != nil {
		return nil, joinedErr
	}

	// Check if the context was canceled before returning results. If it was, return the context error instead of the results. This ensures that we do not return partial results if the execution was canceled.
	if err := ctx.Err(); err != nil {
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
		return ErrStageNotFound(from)
	}
	if _, okTo := p.stages[to]; !okTo {
		return ErrStageNotFound(to)
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
			return ErrStageNotFound(from)
		}
		for _, to := range tos {
			if _, ok := stages[to]; !ok {
				return ErrStageNotFound(to)
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
