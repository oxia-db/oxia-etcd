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
	"sync"
	"time"
)

type lease struct {
	id     int64
	ttl    int64
	expiry time.Time
	keys   map[string]struct{}
}

// lessor keeps the leases in memory: their expiry and the keys attached to them.
type lessor struct {
	mu     sync.Mutex
	leases map[int64]*lease
	now    func() time.Time
}

func newLessor() *lessor {
	return &lessor{leases: map[int64]*lease{}, now: time.Now}
}

func (l *lessor) exists(id int64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.leases[id]
	return ok
}

func (l *lessor) add(id, ttl int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.leases[id] = &lease{id: id, ttl: ttl, expiry: l.now().Add(time.Duration(ttl) * time.Second), keys: map[string]struct{}{}}
}

func (l *lessor) remove(id int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.leases, id)
}

// move detaches key from the lease from, and attaches it to the lease to.
func (l *lessor) move(key []byte, from, to int64) {
	if from == to {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if le, ok := l.leases[from]; ok {
		delete(le.keys, string(key))
	}
	if le, ok := l.leases[to]; ok {
		le.keys[string(key)] = struct{}{}
	}
}

func (l *lessor) keys(id int64) [][]byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	le, ok := l.leases[id]
	if !ok {
		return nil
	}
	keys := make([][]byte, 0, len(le.keys))
	for k := range le.keys {
		keys = append(keys, []byte(k))
	}
	return keys
}

// renew extends the lease by its TTL, and returns the TTL.
func (l *lessor) renew(id int64) (int64, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	le, ok := l.leases[id]
	if !ok || !le.expiry.After(l.now()) {
		return 0, false
	}
	le.expiry = l.now().Add(time.Duration(le.ttl) * time.Second)
	return le.ttl, true
}

// renewAll restarts the TTL of every lease, as a new etcd leader does: the
// time the leases were not served doesn't count against them.
func (l *lessor) renewAll() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, le := range l.leases {
		le.expiry = l.now().Add(time.Duration(le.ttl) * time.Second)
	}
}

func (l *lessor) timeToLive(id int64) (remaining, granted int64, keys [][]byte, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	le, ok := l.leases[id]
	if !ok {
		return 0, 0, nil, false
	}
	remaining = int64(le.expiry.Sub(l.now()).Round(time.Second) / time.Second)
	for k := range le.keys {
		keys = append(keys, []byte(k))
	}
	return max(remaining, 0), le.ttl, keys, true
}

func (l *lessor) expired() []int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	var ids []int64
	for id, le := range l.leases {
		if !le.expiry.After(now) {
			ids = append(ids, id)
		}
	}
	return ids
}

func (l *lessor) isExpired(id int64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	le, ok := l.leases[id]
	return ok && !le.expiry.After(l.now())
}

func (l *lessor) list() []int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	ids := make([]int64, 0, len(l.leases))
	for id := range l.leases {
		ids = append(ids, id)
	}
	return ids
}
