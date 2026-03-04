package pipex

import (
	"testing"
	"time"
)

func TestDefaultSinkRetryPolicyIsBounded(t *testing.T) {
	opts := defaultOptions[int]()
	if opts.SinkRetry.MaxRetries != 10 {
		t.Fatalf("unexpected default max retries: got %d want %d", opts.SinkRetry.MaxRetries, 10)
	}
	if opts.SinkRetry.Backoff <= 0 {
		t.Fatalf("expected positive default sink retry backoff, got %v", opts.SinkRetry.Backoff)
	}
}

func TestWithSinkRetryNormalizesZeroBackoff(t *testing.T) {
	opts := defaultOptions[int]()
	WithSinkRetry[int](3, 0)(opts)
	if opts.SinkRetry.MaxRetries != 3 {
		t.Fatalf("unexpected max retries: got %d want %d", opts.SinkRetry.MaxRetries, 3)
	}
	if opts.SinkRetry.Backoff != time.Millisecond {
		t.Fatalf("expected zero backoff normalized to 1ms, got %v", opts.SinkRetry.Backoff)
	}
}
