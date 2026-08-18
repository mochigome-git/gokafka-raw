package service

import "sync"

// offsetTracker ensures we only commit an offset once every earlier offset
// on that partition has also finished processing — so a crash never leaves
// a committed-but-unprocessed gap.
type partitionTracker struct {
	mu         sync.Mutex
	completed  map[int64]bool
	nextCommit int64
	started    bool
}

type offsetTracker struct {
	mu    sync.Mutex
	parts map[int]*partitionTracker
}

func newOffsetTracker() *offsetTracker {
	return &offsetTracker{parts: make(map[int]*partitionTracker)}
}

// registerRead must be called right after FetchMessage, before processing.
//
// CHANGED — rebalance-safe: previously nextCommit was set only once per
// partition ("started" latch). After a consumer-group rebalance revoked
// and reassigned a partition, Kafka could resume delivery from an offset
// behind or ahead of the stale nextCommit; offsets would then never form
// a contiguous run, markDone would never return a committable offset, and
// commits on that partition silently stalled forever (inserts kept
// working, so it looked healthy until the next restart redelivered
// everything since the stall).
//
// Now: if a fetched offset is LOWER than nextCommit, Kafka has rewound us
// (rebalance/reset) — resync nextCommit to it and drop stale completion
// marks. Returns true when a resync happened so the caller can log it
// (it's a useful rebalance indicator).
func (t *offsetTracker) registerRead(partition int, offset int64) (resynced bool) {
	t.mu.Lock()
	pt, ok := t.parts[partition]
	if !ok {
		pt = &partitionTracker{completed: make(map[int64]bool)}
		t.parts[partition] = pt
	}
	t.mu.Unlock()

	pt.mu.Lock()
	defer pt.mu.Unlock()
	if !pt.started {
		pt.nextCommit = offset
		pt.started = true
		return false
	}
	if offset < pt.nextCommit {
		// Kafka rewound this partition on us — resync.
		pt.nextCommit = offset
		for k := range pt.completed {
			if k < offset {
				delete(pt.completed, k)
			}
		}
		return true
	}
	return false
}

// markDone marks an offset as fully processed (all its fan-out inserts
// are done or dead-lettered) and returns the highest contiguous offset
// now safe to commit, or -1 if nothing new is committable yet.
func (t *offsetTracker) markDone(partition int, offset int64) int64 {
	t.mu.Lock()
	pt := t.parts[partition]
	t.mu.Unlock()
	if pt == nil {
		return -1
	}

	pt.mu.Lock()
	defer pt.mu.Unlock()
	pt.completed[offset] = true

	safe := int64(-1)
	for pt.completed[pt.nextCommit] {
		delete(pt.completed, pt.nextCommit)
		safe = pt.nextCommit
		pt.nextCommit++
	}
	return safe
}
