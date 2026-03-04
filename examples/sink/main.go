package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ruohao1/pipex"
)

type stageFn[T any] struct {
	name    string
	workers int
	fn      func(context.Context, T) ([]T, error)
}

func (s stageFn[T]) Name() string { return s.name }
func (s stageFn[T]) Workers() int { return s.workers }
func (s stageFn[T]) Process(ctx context.Context, in T) ([]T, error) {
	return s.fn(ctx, in)
}

type collectSink struct {
	name  string
	stage string
	mu    sync.Mutex
	items []int
}

func (s *collectSink) Name() string  { return s.name }
func (s *collectSink) Stage() string { return s.stage }
func (s *collectSink) Consume(ctx context.Context, item int) error {
	s.mu.Lock()
	s.items = append(s.items, item)
	s.mu.Unlock()
	return nil
}

func main() {
	p := pipex.NewPipeline[int]()

	src := stageFn[int]{
		name:    "src",
		workers: 2,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in + 1}, nil
		},
	}
	out := stageFn[int]{
		name:    "out",
		workers: 2,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in * 2}, nil
		},
	}

	if err := p.AddStage(src); err != nil {
		panic(err)
	}
	if err := p.AddStage(out); err != nil {
		panic(err)
	}
	if err := p.Connect("src", "out"); err != nil {
		panic(err)
	}

	cs := &collectSink{name: "collector", stage: "out"}

	res, err := p.Run(
		context.Background(),
		map[string][]int{"src": {1, 2, 3}},
		pipex.WithSinks[int](cs),
		pipex.WithSinkRetry[int](10, 10*time.Millisecond),
	)
	if err != nil {
		panic(err)
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	fmt.Println("out stage outputs:", res["out"])
	fmt.Println("sink collected:", cs.items)
}
