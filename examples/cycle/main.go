package main

import (
	"context"
	"fmt"

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
	newCyclePipeline := func() *pipex.Pipeline[int] {
		p := pipex.NewPipeline[int]()
		a := stageFn[int]{
			name:    "a",
			workers: 2,
			fn: func(ctx context.Context, in int) ([]int, error) {
				return []int{in + 1}, nil
			},
		}
		b := stageFn[int]{
			name:    "b",
			workers: 2,
			fn: func(ctx context.Context, in int) ([]int, error) {
				return []int{in}, nil
			},
		}
		if err := p.AddStage(a); err != nil {
			panic(err)
		}
		if err := p.AddStage(b); err != nil {
			panic(err)
		}
		if err := p.Connect("a", "b"); err != nil {
			panic(err)
		}
		if err := p.Connect("b", "a"); err != nil {
			panic(err)
		}
		return p
	}

	// Example 1: no dedup, bounded by max hops.
	p1 := newCyclePipeline()
	res1, err := p1.Run(
		context.Background(),
		map[string][]int{"a": {1}},
		pipex.WithCycleMode[int](4, 100, nil),
	)
	if err != nil {
		panic(err)
	}
	fmt.Println("no dedup:")
	fmt.Println("  a outputs:", res1["a"])
	fmt.Println("  b outputs:", res1["b"])
	fmt.Println("  a count:", len(res1["a"]))
	fmt.Println("  b count:", len(res1["b"]))

	// Example 2: dedup by parity, so revisits of same stage/parity are dropped.
	p2 := newCyclePipeline()
	res2, err := p2.Run(
		context.Background(),
		map[string][]int{"a": {1}},
		pipex.WithCycleMode[int](
			10,
			100,
			func(v int) string {
				if v%2 == 0 {
					return "even"
				}
				return "odd"
			},
		),
	)
	if err != nil {
		panic(err)
	}
	fmt.Println("dedup by parity:")
	fmt.Println("  a outputs:", res2["a"])
	fmt.Println("  b outputs:", res2["b"])
	fmt.Println("  a count:", len(res2["a"]))
	fmt.Println("  b count:", len(res2["b"]))
}
