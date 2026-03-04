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
