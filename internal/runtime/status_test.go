package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/ruohao1/pipex/internal/frontier"
)

func TestTryDurableStatusSnapshot_Present(t *testing.T) {
	store := durableStatusStore{
		snapshot: frontier.DurableStatusSnapshot{
			Pending: 1, Reserved: 2, Acked: 3, RetriedEntries: 4,
		},
	}
	snap, ok, err := TryDurableStatusSnapshot(context.Background(), store)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !ok {
		t.Fatal("expected provider detection")
	}
	if snap.Pending != 1 || snap.Reserved != 2 || snap.Acked != 3 || snap.RetriedEntries != 4 {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}
}

func TestTryDurableStatusSnapshot_Absent(t *testing.T) {
	type noProvider struct{}
	_, ok, err := TryDurableStatusSnapshot(context.Background(), noProvider{})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ok {
		t.Fatal("expected no provider")
	}
}

func TestTryDurableStatusSnapshot_Error(t *testing.T) {
	want := errors.New("boom")
	store := durableStatusStore{err: want}
	_, ok, err := TryDurableStatusSnapshot(context.Background(), store)
	if !ok {
		t.Fatal("expected provider detection")
	}
	if !errors.Is(err, want) {
		t.Fatalf("err=%v want=%v", err, want)
	}
}

func TestDurableSnapshotToStats(t *testing.T) {
	in := frontier.DurableStatusSnapshot{
		Pending:        10,
		Reserved:       3,
		Acked:          8,
		RetriedEntries: 2,
		Dropped:        1,
		TerminalFailed: 4,
		Canceled:       5,
	}
	out := DurableSnapshotToStats(in)
	if out.Pending != 10 || out.Inflight != 3 || out.Acked != 8 || out.Retried != 2 ||
		out.Dropped != 1 || out.TerminalFailed != 4 || out.Canceled != 5 {
		t.Fatalf("unexpected stats conversion: %+v", out)
	}
}

type durableStatusStore struct {
	snapshot frontier.DurableStatusSnapshot
	err      error
}

func (d durableStatusStore) StatusSnapshot(ctx context.Context) (frontier.DurableStatusSnapshot, error) {
	if d.err != nil {
		return frontier.DurableStatusSnapshot{}, d.err
	}
	return d.snapshot, nil
}
