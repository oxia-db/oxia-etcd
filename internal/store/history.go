// Copyright 2023-2026 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
	"sort"
	"sync"

	"go.etcd.io/etcd/api/v3/mvccpb"
)

// history keeps the recent events, in revision order. It feeds the watchers,
// and the reads at a past revision, which undo the events after it.
// Every event has a PrevKv when the key existed before it.
type history struct {
	mu     sync.Mutex
	events []*mvccpb.Event
	// floor is the revision the history starts after: it has every event
	// with a revision above it
	floor int64
	rev   int64
	max   int
	// changed is closed, and replaced, when the history changes
	changed chan struct{}
}

func newHistory(max int) *history {
	return &history{max: max, changed: make(chan struct{})}
}

// reset starts an empty history at rev.
func (h *history) reset(rev int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = nil
	h.floor = rev
	h.rev = rev
	h.signal()
}

func (h *history) append(ev *mvccpb.Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, ev)
	h.rev = ev.Kv.ModRevision
	if over := len(h.events) - h.max; over > 0 {
		h.floor = h.events[over-1].Kv.ModRevision
		h.events = append([]*mvccpb.Event(nil), h.events[over:]...)
	}
	h.signal()
}

// compact drops the events before rev: the history can still be read at rev.
func (h *history) compact(rev int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if rev-1 <= h.floor {
		return
	}
	i := sort.Search(len(h.events), func(i int) bool { return h.events[i].Kv.ModRevision >= rev })
	h.events = append([]*mvccpb.Event(nil), h.events[i:]...)
	h.floor = rev - 1
	h.signal()
}

func (h *history) signal() {
	close(h.changed)
	h.changed = make(chan struct{})
}

func (h *history) current() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.rev
}

// since returns the events from rev on, the revision they go up to, and a
// channel closed on the next change. ok is false when the history no longer
// has the events from rev on.
func (h *history) since(rev int64) (events []*mvccpb.Event, current int64, changed <-chan struct{}, ok bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if rev <= h.floor {
		return nil, h.rev, h.changed, false
	}
	i := sort.Search(len(h.events), func(i int) bool { return h.events[i].Kv.ModRevision >= rev })
	return h.events[i:len(h.events):len(h.events)], h.rev, h.changed, true
}

// oldest returns the lowest revision a watch can start from.
func (h *history) oldest() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.floor + 1
}
