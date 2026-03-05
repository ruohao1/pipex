package runtime

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"
)

var runSeq uint64

// NewRunID returns a best-effort unique run identifier.
func NewRunID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err == nil {
		return hex.EncodeToString(b[:])
	}

	n := atomic.AddUint64(&runSeq, 1)
	return fmt.Sprintf("run-%d-%d", time.Now().UnixNano(), n)
}
