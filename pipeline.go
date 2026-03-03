package pipex

import (
	"maps"
	"context"
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
func (p *Pipeline[T]) Run(ctx context.Context, seeds map[string][]T) (map[string][]T, error) {
	// Validate the pipeline configuration before running
	if err := p.Validate(); err != nil {
		return nil, err
	}
	// Snapshot the pipeline configuration to avoid holding locks during execution
	// This allows the pipeline to be modified concurrently while it's running, but it also means that changes made to the pipeline after this point will not affect the current execution.
	p.mu.RLock()
	defer p.mu.RUnlock()
	stages := make(map[string]Stage[T], len(p.stages))
	maps.Copy(stages, p.stages)
	edges := make(map[string][]string, len(p.edges))
	for k, v := range p.edges {
		edges[k] = append([]string(nil), v...)
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

	type workItem[T any] struct {
		name  string
		value T
	}
	results := make(map[string][]T)
	queue := make([]workItem[T], 0, 128)

	for stageName, inputs := range seeds {
		for _, input := range inputs {
			queue = append(queue, workItem[T]{name: stageName, value: input})
		}
	}

	for len(queue) > 0 {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		item := queue[0]
		queue = queue[1:]

		stage, ok := stages[item.name]
		if !ok {
			return nil, ErrStageNotFound(item.name)
		}
		outputs, err := stage.Process(ctx, item.value)
		if err != nil {
			return nil, err
		}
		results[item.name] = append(results[item.name], outputs...)
		for _, nextStageName := range edges[item.name] {
			for _, output := range outputs {
				queue = append(queue, workItem[T]{name: nextStageName, value: output})
			}
		}
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
		return ErrStageExists(stage.Name())
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
		return ErrEdgeExists(from, to)
	}

	p.edges[from] = append(p.edges[from], to)
	return nil
}

// Validate checks that the pipeline configuration is valid. It ensures that all stages referenced in the edges exist in the stages map. It also checks for cycles in the graph.
func (p *Pipeline[T]) Validate() error {
	// Lock the pipeline for reading to ensure thread safety when accessing the stages and edges maps.
	p.mu.RLock()
	defer p.mu.RUnlock()
	// Check that there is at least one stage
	if len(p.stages) == 0 {
		return ErrNoStages
	}

	// Check that all stages referenced in edges exist
	for from, tos := range p.edges {
		if _, ok := p.stages[from]; !ok {
			return ErrStageNotFound(from)
		}
		for _, to := range tos {
			if _, ok := p.stages[to]; !ok {
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

		if slices.ContainsFunc(p.edges[node], dfs) {
			return true
		}

		recStack[node] = false
		return false
	}

	for stage := range p.stages {
		if !visited[stage] {
			if dfs(stage) {
				return ErrCycle
			}
		}
	}

	return nil
}
