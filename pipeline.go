package pipex

import "slices"

import "sync"

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

// AddStage adds a new stage to the pipeline. It checks that the stage is not nil and that a stage with the same name does not already exist in the pipeline.
func (p *Pipeline[T]) AddStage(stage Stage[T]) error {
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
