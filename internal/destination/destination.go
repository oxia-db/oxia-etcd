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

// Package destination is the key-value storage the gateway keeps its records in.
package destination

import (
	"context"
	"errors"
)

var (
	// ErrNotFound is returned when the key has no record.
	ErrNotFound = errors.New("destination: key not found")
	// ErrConflict is returned when the record's version is not the expected one.
	ErrConflict = errors.New("destination: unexpected version")
)

const (
	// VersionNotExists expects the key to have no record.
	VersionNotExists int64 = -1
	// VersionAny skips the version check.
	VersionAny int64 = -2
)

// Record is a stored key with its value and version. VersionId comes from a
// counter that grows with every write to the destination, so it orders writes.
type Record struct {
	Key       string
	Value     []byte
	VersionId int64
}

// Destination stores the records of one gateway. All the records share one
// ordering domain: the version ids of two writes compare like their commit order.
type Destination interface {
	Get(ctx context.Context, key string) (Record, error)
	// Put writes the record if its current version is expected, and returns the new version.
	Put(ctx context.Context, key string, value []byte, expected int64) (int64, error)
	// PutEphemeral writes a record that is removed when the gateway's session ends.
	PutEphemeral(ctx context.Context, key string, value []byte, expected int64) (int64, error)
	Delete(ctx context.Context, key string, expected int64) error
	// Scan calls fn for the records in [minKey, maxKey), in key order, until fn returns false.
	Scan(ctx context.Context, minKey, maxKey string, fn func(Record) bool) error
	Close() error
}
