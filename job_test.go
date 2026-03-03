package pipex

import (
	"context"
	"errors"
	"testing"
)

func TestJobExecRunsFunc(t *testing.T) {
	called := false
	j := Job{
		Name: "ok",
		Run: func(ctx context.Context) error {
			called = true
			return nil
		},
	}

	if err := j.Exec(context.Background()); err != nil {
		t.Fatalf("unexpected exec error: %v", err)
	}
	if !called {
		t.Fatal("expected job function to be called")
	}
}

func TestJobExecPropagatesError(t *testing.T) {
	want := errors.New("boom")
	j := Job{
		Name: "err",
		Run: func(ctx context.Context) error {
			return want
		},
	}

	err := j.Exec(context.Background())
	if !errors.Is(err, want) {
		t.Fatalf("expected %v, got %v", want, err)
	}
}

func TestJobExecNilFunc(t *testing.T) {
	j := Job{Name: "nil"}

	err := j.Exec(context.Background())
	if !errors.Is(err, ErrNilJobFunc) {
		t.Fatalf("expected ErrNilJobFunc, got %v", err)
	}
}
