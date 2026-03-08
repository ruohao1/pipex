package runtime

import "sync"

import "testing"

func TestEvaluateCycleGate(t *testing.T) {
	if got := EvaluateCycleGate(false, 3, 10, 9, 0, 0); got != CycleGateAllow {
		t.Fatalf("got %v, want allow", got)
	}
	if got := EvaluateCycleGate(true, 2, 10, 3, 0, 0); got != CycleGateDropHopLimit {
		t.Fatalf("got %v, want hop-limit drop", got)
	}
	if got := EvaluateCycleGate(true, -1, 3, 0, 2, 1); got != CycleGateRejectMaxJobs {
		t.Fatalf("got %v, want max-jobs reject", got)
	}
	if got := EvaluateCycleGate(true, 3, 10, 2, 3, 2); got != CycleGateAllow {
		t.Fatalf("got %v, want allow", got)
	}
}

func TestSeenContainsOrInsert(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]struct{}{}

	if got := SeenContainsOrInsert(&mu, seen, "a"); got {
		t.Fatal("first insert should not exist")
	}
	if got := SeenContainsOrInsert(&mu, seen, "a"); !got {
		t.Fatal("second insert should report exists")
	}
}
