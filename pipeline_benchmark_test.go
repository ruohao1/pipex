package pipex

import (
	"context"
	"errors"
	"testing"
	"time"
)

func benchmarkPipelineForRun() *Pipeline[int] {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{
		name:    "a",
		workers: 8,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in + 1}, nil
		},
	})
	_ = p.AddStage(testStage[int]{
		name:    "b",
		workers: 8,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in * 2}, nil
		},
	})
	_ = p.Connect("a", "b")
	return p
}

func benchmarkSeeds(n int) map[string][]int {
	seeds := make([]int, n)
	for i := range seeds {
		seeds[i] = i
	}
	return map[string][]int{"a": seeds}
}

func BenchmarkRunNoSink(b *testing.B) {
	p := benchmarkPipelineForRun()
	seeds := benchmarkSeeds(256)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := p.Run(context.Background(), seeds, WithBufferSize[int](256)); err != nil {
			b.Fatalf("run failed: %v", err)
		}
	}
}

func BenchmarkRunWithSlowSink(b *testing.B) {
	p := benchmarkPipelineForRun()
	seeds := benchmarkSeeds(256)

	sink := testSink[int]{
		name:  "slow",
		stage: "b",
		fn: func(ctx context.Context, item int) error {
			time.Sleep(50 * time.Microsecond)
			return nil
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := p.Run(
			context.Background(),
			seeds,
			WithBufferSize[int](256),
			WithSinks[int](sink),
			WithSinkRetry[int](0, 10*time.Millisecond),
		); err != nil {
			b.Fatalf("run failed: %v", err)
		}
	}
}

func BenchmarkRunFrontierMode(b *testing.B) {
	p := benchmarkPipelineForRun()
	seeds := benchmarkSeeds(256)

	b.Run("frontier-off", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := p.Run(
				context.Background(),
				seeds,
				WithBufferSize[int](256),
				WithFrontier[int](false),
			); err != nil {
				b.Fatalf("run failed: %v", err)
			}
		}
	})

	b.Run("frontier-on", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := p.Run(
				context.Background(),
				seeds,
				WithBufferSize[int](256),
				WithFrontier[int](true),
			); err != nil {
				b.Fatalf("run failed: %v", err)
			}
		}
	})
}

func BenchmarkRunStageExhaustionParity(b *testing.B) {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{
		name:    "a",
		workers: 8,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return nil, context.DeadlineExceeded
		},
	})
	seeds := benchmarkSeeds(256)
	policy := map[string]StagePolicy{
		"a": {MaxAttempts: 2},
	}

	b.Run("frontier-off", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = p.Run(
				context.Background(),
				seeds,
				WithBufferSize[int](256),
				WithFrontier[int](false),
				WithFailFast[int](false),
				WithStagePolicies[int](policy),
			)
		}
	})

	b.Run("frontier-on", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = p.Run(
				context.Background(),
				seeds,
				WithBufferSize[int](256),
				WithFrontier[int](true),
				WithFailFast[int](false),
				WithStagePolicies[int](policy),
			)
		}
	})
}

func benchmarkPipelineFanoutForFrontier() *Pipeline[int] {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{
		name:    "a",
		workers: 8,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in, in + 1}, nil
		},
	})
	_ = p.AddStage(testStage[int]{
		name:    "b",
		workers: 8,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in * 2}, nil
		},
	})
	_ = p.AddStage(testStage[int]{
		name:    "c",
		workers: 8,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in * 3}, nil
		},
	})
	_ = p.AddStage(testStage[int]{name: "d", workers: 8})
	_ = p.Connect("a", "b")
	_ = p.Connect("a", "c")
	_ = p.Connect("b", "d")
	_ = p.Connect("c", "d")
	return p
}

func benchmarkPipelineRetryForFrontier() *Pipeline[int] {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{
		name:    "a",
		workers: 8,
		fn: func(ctx context.Context, in int) ([]int, error) {
			// Deterministic mixed workload: some items always fail, others succeed.
			if in%3 == 0 {
				return nil, errors.New("deterministic failure")
			}
			return []int{in}, nil
		},
	})
	return p
}

func benchmarkFrontierModes(b *testing.B, p *Pipeline[int], seeds map[string][]int, includeBlocking bool, opts ...Option[int]) {
	modes := []struct {
		name string
		opts []Option[int]
	}{
		{
			name: "frontier-off",
			opts: []Option[int]{WithFrontier[int](false)},
		},
		{
			name: "frontier-on",
			opts: []Option[int]{WithFrontier[int](true)},
		},
	}
	if includeBlocking {
		modes = append(modes, struct {
			name string
			opts []Option[int]
		}{
			name: "frontier-on-blocking-enqueue",
			opts: []Option[int]{
				WithFrontier[int](true),
				WithFrontierBlockingEnqueue[int](true),
				WithFrontierPendingCapacity[int](65536),
			},
		})
	}

	for _, mode := range modes {
		b.Run(mode.name, func(b *testing.B) {
			runOpts := make([]Option[int], 0, len(mode.opts)+len(opts))
			runOpts = append(runOpts, mode.opts...)
			runOpts = append(runOpts, opts...)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := p.Run(context.Background(), seeds, runOpts...); err != nil {
					// Some benchmark cases intentionally include failures; they should still return.
					_ = err
				}
			}
		})
	}
}

func BenchmarkRunFrontierComprehensive(b *testing.B) {
	cases := []struct {
		name            string
		build           func() *Pipeline[int]
		seeds           map[string][]int
		includeBlocking bool
		opts            []Option[int]
	}{
		{
			name:            "linear-success-256",
			build:           benchmarkPipelineForRun,
			seeds:           benchmarkSeeds(256),
			includeBlocking: true,
			opts:            []Option[int]{WithBufferSize[int](256)},
		},
		{
			name:            "linear-success-2048",
			build:           benchmarkPipelineForRun,
			seeds:           benchmarkSeeds(2048),
			includeBlocking: true,
			opts:            []Option[int]{WithBufferSize[int](512)},
		},
		{
			name:            "fanout-success-512",
			build:           benchmarkPipelineFanoutForFrontier,
			seeds:           benchmarkSeeds(512),
			includeBlocking: true,
			opts:            []Option[int]{WithBufferSize[int](512)},
		},
		{
			name:            "retry-exhaustion-mixed-512",
			build:           benchmarkPipelineRetryForFrontier,
			seeds:           benchmarkSeeds(512),
			includeBlocking: false,
			opts: []Option[int]{
				WithBufferSize[int](512),
				WithFailFast[int](false),
				WithStagePolicies[int](map[string]StagePolicy{
					"a": {MaxAttempts: 2},
				}),
			},
		},
		{
			name:            "sink-slow-256",
			build:           benchmarkPipelineForRun,
			seeds:           benchmarkSeeds(256),
			includeBlocking: true,
			opts: []Option[int]{
				WithBufferSize[int](256),
				WithSinks[int](testSink[int]{
					name:  "slow",
					stage: "b",
					fn: func(ctx context.Context, item int) error {
						time.Sleep(50 * time.Microsecond)
						return nil
					},
				}),
				WithSinkRetry[int](0, 10*time.Millisecond),
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			p := tc.build()
			benchmarkFrontierModes(b, p, tc.seeds, tc.includeBlocking, tc.opts...)
		})
	}
}

func benchmarkPipelineForStageWorkersFanout() *Pipeline[int] {
	p := NewPipeline[int]()
	_ = p.AddStage(testStage[int]{
		name:    "a",
		workers: 8,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in + 1}, nil
		},
	})
	_ = p.AddStage(testStage[int]{
		name:    "b",
		workers: 8,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in * 2}, nil
		},
	})
	_ = p.AddStage(testStage[int]{
		name:    "c",
		workers: 8,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in + 3}, nil
		},
	})
	_ = p.AddStage(testStage[int]{
		name:    "d",
		workers: 8,
		fn: func(ctx context.Context, in int) ([]int, error) {
			return []int{in - 1}, nil
		},
	})
	_ = p.Connect("a", "b")
	_ = p.Connect("a", "c")
	_ = p.Connect("b", "d")
	_ = p.Connect("c", "d")
	return p
}

func BenchmarkRunStageWorkersFanout(b *testing.B) {
	p := benchmarkPipelineForStageWorkersFanout()
	seeds := benchmarkSeeds(512)

	b.Run("default-workers", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := p.Run(context.Background(), seeds, WithBufferSize[int](256)); err != nil {
				b.Fatalf("run failed: %v", err)
			}
		}
	})

	b.Run("with-stage-workers-override", func(b *testing.B) {
		overrides := map[string]int{
			"a": 4,
			"b": 16,
			"c": 16,
			"d": 8,
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := p.Run(
				context.Background(),
				seeds,
				WithBufferSize[int](256),
				WithStageWorkers[int](overrides),
			); err != nil {
				b.Fatalf("run failed: %v", err)
			}
		}
	})
}

func BenchmarkRunStageRateLimitsFanout(b *testing.B) {
	p := benchmarkPipelineForStageWorkersFanout()
	seeds := benchmarkSeeds(512)

	b.Run("without-stage-rate-limits", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := p.Run(context.Background(), seeds, WithBufferSize[int](256)); err != nil {
				b.Fatalf("run failed: %v", err)
			}
		}
	})

	// Use high limits to isolate limiter overhead without introducing intentional throttling.
	b.Run("with-stage-rate-limits", func(b *testing.B) {
		limits := map[string]RateLimit{
			"a": {RPS: 100000, Burst: 2048},
			"b": {RPS: 100000, Burst: 2048},
			"c": {RPS: 100000, Burst: 2048},
			"d": {RPS: 100000, Burst: 2048},
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := p.Run(
				context.Background(),
				seeds,
				WithBufferSize[int](256),
				WithStageRateLimits[int](limits),
			); err != nil {
				b.Fatalf("run failed: %v", err)
			}
		}
	})
}
