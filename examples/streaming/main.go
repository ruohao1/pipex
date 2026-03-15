package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ruohao1/pipex"
)

type streamingDiscoverStage struct {
	name    string
	workers int
	fn      func(context.Context, string, func(string) error) error
}

func (s streamingDiscoverStage) Name() string { return s.name }

func (s streamingDiscoverStage) Workers() int { return s.workers }

func (s streamingDiscoverStage) Process(ctx context.Context, in string) ([]string, error) {
	return []string{in}, nil
}

func (s streamingDiscoverStage) ProcessStream(ctx context.Context, in string, emit func(string) error) error {
	return s.fn(ctx, in, emit)
}

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
	runBasicStreamingDemo()
	fmt.Println()
	runRetryDemo(false, false)
	fmt.Println()
	runRetryDemo(true, false)
	fmt.Println()
	runRetryDemo(true, true)
}

func runBasicStreamingDemo() {
	fmt.Println("== basic streaming (downstream starts before discover finishes) ==")
	p := pipex.NewPipeline[string]()
	start := time.Now()

	discover := streamingDiscoverStage{
		name:    "discover",
		workers: 1,
		fn: func(ctx context.Context, base string, emit func(string) error) error {
			paths := []string{"/admin", "/assets", "/backup"}
			for _, path := range paths {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				full := strings.TrimRight(base, "/") + path
				fmt.Printf("[%s] discover emitted %s\n", time.Since(start).Truncate(time.Millisecond), full)
				if err := emit(full); err != nil {
					return err
				}

				// Simulate continued upstream work while downstream can already process.
				time.Sleep(120 * time.Millisecond)
			}
			fmt.Printf("[%s] discover finished\n", time.Since(start).Truncate(time.Millisecond))
			return nil
		},
	}

	var (
		scanMu  sync.Mutex
		scanned []string
	)
	scan := stageFn[string]{
		name:    "scan",
		workers: 2,
		fn: func(ctx context.Context, in string) ([]string, error) {
			fmt.Printf("[%s] scan started %s\n", time.Since(start).Truncate(time.Millisecond), in)
			time.Sleep(40 * time.Millisecond)
			fmt.Printf("[%s] scan finished %s\n", time.Since(start).Truncate(time.Millisecond), in)

			scanMu.Lock()
			scanned = append(scanned, in)
			scanMu.Unlock()

			return []string{in}, nil
		},
	}

	if err := p.AddStage(discover); err != nil {
		panic(err)
	}
	if err := p.AddStage(scan); err != nil {
		panic(err)
	}
	if err := p.Connect("discover", "scan"); err != nil {
		panic(err)
	}

	res, err := p.Run(
		context.Background(),
		map[string][]string{"discover": {"https://example.test"}},
		pipex.WithBufferSize[string](64),
	)
	if err != nil {
		panic(err)
	}

	scanMu.Lock()
	defer scanMu.Unlock()
	fmt.Println("discover outputs:", res["discover"])
	fmt.Println("scan outputs:", res["scan"])
	fmt.Println("scanned count:", len(scanned))
}

func runRetryDemo(withDedup, withFrontier bool) {
	label := "without dedup"
	if withDedup {
		label = "with dedup"
	}
	mode := "direct"
	if withFrontier {
		mode = "frontier"
	}
	fmt.Printf("== retry demo (%s, %s mode) ==\n", label, mode)

	p := pipex.NewPipeline[string]()
	start := time.Now()
	var attemptCounter atomic.Int64

	discover := streamingDiscoverStage{
		name:    "discover",
		workers: 1,
		fn: func(ctx context.Context, base string, emit func(string) error) error {
			attempt := attemptCounter.Add(1)
			fmt.Printf("[%s] discover attempt %d start\n", time.Since(start).Truncate(time.Millisecond), attempt)
			paths := []string{"/admin", "/assets"}
			for _, path := range paths {
				full := strings.TrimRight(base, "/") + path
				fmt.Printf("[%s] discover emitted %s\n", time.Since(start).Truncate(time.Millisecond), full)
				if err := emit(full); err != nil {
					return err
				}
			}
			if attempt == 1 {
				return fmt.Errorf("transient network glitch after emit")
			}
			fmt.Printf("[%s] discover attempt %d success\n", time.Since(start).Truncate(time.Millisecond), attempt)
			return nil
		},
	}

	var (
		scanMu  sync.Mutex
		scanned []string
	)
	scan := stageFn[string]{
		name:    "scan",
		workers: 2,
		fn: func(ctx context.Context, in string) ([]string, error) {
			fmt.Printf("[%s] scan saw %s\n", time.Since(start).Truncate(time.Millisecond), in)
			scanMu.Lock()
			scanned = append(scanned, in)
			scanMu.Unlock()
			return []string{in}, nil
		},
	}

	if err := p.AddStage(discover); err != nil {
		panic(err)
	}
	if err := p.AddStage(scan); err != nil {
		panic(err)
	}
	if err := p.Connect("discover", "scan"); err != nil {
		panic(err)
	}

	opts := []pipex.Option[string]{
		pipex.WithBufferSize[string](64),
		pipex.WithStagePolicies[string](map[string]pipex.StagePolicy{
			"discover": {MaxAttempts: 2},
		}),
	}
	if withDedup {
		opts = append(opts,
			pipex.WithDedupRules[string](pipex.DedupRule[string]{
				Name:  "by-url",
				Scope: pipex.DedupScopeGlobal,
				Key: func(v string) string {
					return v
				},
			}),
		)
	}
	if withFrontier {
		opts = append(opts, pipex.WithFrontier[string](true))
	}

	res, err := p.Run(
		context.Background(),
		map[string][]string{"discover": {"https://example.test"}},
		opts...,
	)
	if err != nil {
		panic(err)
	}

	scanMu.Lock()
	defer scanMu.Unlock()
	fmt.Println("discover outputs:", res["discover"])
	fmt.Println("scan outputs:", res["scan"])
	fmt.Println("scan saw count:", len(scanned), "items:", scanned)
}
