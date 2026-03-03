package main

import (
	"context"
	"fmt"
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

func main() {
	p := pipex.NewPipeline[int]()

	src := stageFn[int]{
		name:    "src",
		workers: 2,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in + 1}, nil
		},
	}
	sink := stageFn[int]{
		name:    "sink",
		workers: 2,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in * 2}, nil
		},
	}

	if err := p.AddStage(src); err != nil {
		panic(err)
	}
	if err := p.AddStage(sink); err != nil {
		panic(err)
	}
	if err := p.Connect("src", "sink"); err != nil {
		panic(err)
	}

	tr := pipex.NewTrigger(
		"ticker",
		"src",
		pipex.TriggerFunc[int](func(ctx context.Context, emit func(int) error) error {
			for _, v := range []int{10, 20, 30} {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(50 * time.Millisecond):
					if err := emit(v); err != nil {
						return err
					}
				}
			}
			return nil
		}),
	)

	res, err := p.Run(
		context.Background(),
		map[string][]int{"src": {1, 2}},
		pipex.WithBufferSize[int](64),
		pipex.WithFailFast[int](true),
		pipex.WithTriggers[int](tr),
	)
	if err != nil {
		panic(err)
	}

	fmt.Println("src outputs:", len(res["src"]))
	fmt.Println("sink outputs:", len(res["sink"]))
}
