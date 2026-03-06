package pipex

import (
	"context"
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
