package idgen

import (
	"strconv"
	"sync/atomic"
)

// Sequential returns a concurrency-safe process-local identifier generator.
// It is suitable for deterministic demos, not persistent distributed data.
func Sequential(prefix string) func() string {
	var sequence atomic.Uint64
	return func() string {
		return prefix + strconv.FormatUint(sequence.Add(1), 10)
	}
}
