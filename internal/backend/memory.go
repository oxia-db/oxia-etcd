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

package backend

import (
	"context"
	"slices"
	"sync"
)

// Memory keeps the records in memory, with the version semantics of Oxia. It is for tests.
type Memory struct {
	mu        sync.Mutex
	records   map[string]Record
	versionId int64
}

func NewMemory() *Memory {
	return &Memory{records: map[string]Record{}, versionId: -1}
}

func (m *Memory) Get(_ context.Context, key string) (Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.records[key]
	if !ok {
		return Record{}, ErrNotFound
	}
	return r, nil
}

func (m *Memory) Put(_ context.Context, key string, value []byte, expected int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(key, expected); err != nil {
		return 0, err
	}
	m.versionId++
	m.records[key] = Record{Key: key, Value: slices.Clone(value), VersionId: m.versionId}
	return m.versionId, nil
}

func (m *Memory) PutEphemeral(ctx context.Context, key string, value []byte, expected int64) (int64, error) {
	return m.Put(ctx, key, value, expected)
}

func (m *Memory) Delete(_ context.Context, key string, expected int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.records[key]; !ok {
		return ErrNotFound
	}
	if err := m.check(key, expected); err != nil {
		return err
	}
	delete(m.records, key)
	return nil
}

func (m *Memory) check(key string, expected int64) error {
	r, ok := m.records[key]
	switch {
	case expected == VersionAny:
		return nil
	case expected == VersionNotExists && ok:
		return ErrConflict
	case expected >= 0 && (!ok || r.VersionId != expected):
		return ErrConflict
	}
	return nil
}

func (m *Memory) Scan(_ context.Context, minKey, maxKey string, fn func(Record) bool) error {
	m.mu.Lock()
	var records []Record
	for k, r := range m.records {
		if k >= minKey && k < maxKey {
			records = append(records, r)
		}
	}
	m.mu.Unlock()
	slices.SortFunc(records, func(a, b Record) int { return compareStrings(a.Key, b.Key) })
	for _, r := range records {
		if !fn(r) {
			return nil
		}
	}
	return nil
}

func (m *Memory) Close() error { return nil }

func compareStrings(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}
