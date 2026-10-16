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
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/oxia-db/oxia/oxia"
)

// Oxia keeps the records of a cluster in an Oxia namespace. The records share
// one partition key, so they live in one shard and their version ids follow
// the shard's commit order. Their keys share a prefix, which keeps them apart
// from the records of the other clusters in the namespace.
type Oxia struct {
	client       oxia.SyncClient
	partitionKey string
	prefix       string
}

func NewOxia(serviceAddress, namespace, partitionKey string, sessionTimeout time.Duration,
	extra ...oxia.ClientOption) (*Oxia, error) {
	opts := append([]oxia.ClientOption{
		oxia.WithNamespace(namespace),
		oxia.WithSessionTimeout(sessionTimeout),
		oxia.WithIdentity("oxia-etcd"),
		// The gateway issues one write at a time, so there is nothing to batch
		oxia.WithBatchLinger(0),
	}, extra...)
	client, err := oxia.NewSyncClient(serviceAddress, opts...)
	if err != nil {
		return nil, err
	}
	// Hex encoded, so that the prefix has no separator of Oxia's hierarchical keys
	return &Oxia{client: client, partitionKey: partitionKey, prefix: hex.EncodeToString([]byte(partitionKey)) + "-"}, nil
}

func (o *Oxia) Get(ctx context.Context, key string) (Record, error) {
	_, value, version, err := o.client.Get(ctx, o.prefix+key, oxia.PartitionKey(o.partitionKey))
	if err != nil {
		return Record{}, mapError(err)
	}
	return Record{Key: key, Value: value, VersionId: version.VersionId}, nil
}

func (o *Oxia) Put(ctx context.Context, key string, value []byte, expected int64) (int64, error) {
	return o.put(ctx, key, value, expected)
}

func (o *Oxia) PutEphemeral(ctx context.Context, key string, value []byte, expected int64) (int64, error) {
	return o.put(ctx, key, value, expected, oxia.Ephemeral())
}

func (o *Oxia) put(ctx context.Context, key string, value []byte, expected int64, extra ...oxia.PutOption) (int64, error) {
	opts := append([]oxia.PutOption{oxia.PartitionKey(o.partitionKey)}, extra...)
	switch expected {
	case VersionAny:
	case VersionNotExists:
		opts = append(opts, oxia.ExpectedRecordNotExists())
	default:
		opts = append(opts, oxia.ExpectedVersionId(expected))
	}
	_, version, err := o.client.Put(ctx, o.prefix+key, value, opts...)
	if err != nil {
		return 0, mapError(err)
	}
	return version.VersionId, nil
}

func (o *Oxia) Delete(ctx context.Context, key string, expected int64) error {
	opts := []oxia.DeleteOption{oxia.PartitionKey(o.partitionKey)}
	if expected >= 0 {
		opts = append(opts, oxia.ExpectedVersionId(expected))
	}
	return mapError(o.client.Delete(ctx, o.prefix+key, opts...))
}

func (o *Oxia) Scan(ctx context.Context, minKey, maxKey string, fn func(Record) bool) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for res := range o.client.RangeScan(ctx, o.prefix+minKey, o.prefix+maxKey, oxia.PartitionKey(o.partitionKey)) {
		if res.Err != nil {
			return res.Err
		}
		if !fn(Record{Key: strings.TrimPrefix(res.Key, o.prefix), Value: res.Value, VersionId: res.Version.VersionId}) {
			return nil
		}
	}
	return ctx.Err()
}

func (o *Oxia) Close() error {
	return o.client.Close()
}

func mapError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, oxia.ErrKeyNotFound):
		return ErrNotFound
	case errors.Is(err, oxia.ErrUnexpectedVersionId):
		return ErrConflict
	default:
		return err
	}
}
