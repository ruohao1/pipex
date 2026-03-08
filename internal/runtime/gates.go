package runtime

import "sync"

type CycleGateDecision int

const (
	CycleGateAllow CycleGateDecision = iota
	CycleGateDropHopLimit
	CycleGateRejectMaxJobs
)

func EvaluateCycleGate(enabled bool, maxHops int, maxJobs int, hops int, acceptedJobs int, reservedJobs int) CycleGateDecision {
	if !enabled {
		return CycleGateAllow
	}
	if maxHops >= 0 && hops > maxHops {
		return CycleGateDropHopLimit
	}
	if acceptedJobs+reservedJobs >= maxJobs {
		return CycleGateRejectMaxJobs
	}
	return CycleGateAllow
}

// SeenContainsOrInsert checks presence and inserts the key if absent.
// It is safe for concurrent use with a shared mutex+map pair.
func SeenContainsOrInsert(mu *sync.Mutex, seen map[string]struct{}, key string) (exists bool) {
	mu.Lock()
	defer mu.Unlock()
	if _, ok := seen[key]; ok {
		return true
	}
	seen[key] = struct{}{}
	return false
}
