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

// registerRead must be called right after FetchMessage, before processing,
// so we know the true lower bound to commit from.
func (t *offsetTracker) registerRead(partition int, offset int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	pt, ok := t.parts[partition]
	if !ok {
		pt = &partitionTracker{completed: make(map[int64]bool)}
		t.parts[partition] = pt
	}
	pt.mu.Lock()
	if !pt.started {
		pt.nextCommit = offset
		pt.started = true
	}
	pt.mu.Unlock()
}

// markDone marks an offset as fully processed (all its fan-out inserts are
// done) and returns the highest contiguous offset now safe to commit, or
// -1 if nothing new is committable yet.
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
