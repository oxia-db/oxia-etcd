package store

import (
	"encoding/hex"
	"fmt"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/proto"
)

// The backend keys of the gateway. The etcd keys are hex encoded, which keeps
// their byte order and keeps them clear of the backend's key separators.
const (
	kvPrefix    = "k"
	kvEnd       = "l" // the first key after all the kv keys
	leasePrefix = "l"
	leaseEnd    = "m"
	ownerKey    = "m-owner"
)

const (
	flagLive      byte = 0
	flagTombstone byte = 1
)

func kvKey(key []byte) string {
	return kvPrefix + hex.EncodeToString(key)
}

func decodeKvKey(key string) ([]byte, error) {
	return hex.DecodeString(key[len(kvPrefix):])
}

func leaseKey(id int64) string {
	return fmt.Sprintf("%s%016x", leasePrefix, id)
}

// scanBounds returns the backend range of the etcd range [key, end).
func scanBounds(key, end []byte) (string, string) {
	if len(end) == 1 && end[0] == 0 {
		return kvKey(key), kvEnd
	}
	return kvKey(key), kvKey(end)
}

// InRange reports whether k is in the etcd range [key, end): one key when end
// is empty, and every key from key on when end is "\x00".
func InRange(k, key, end []byte) bool {
	switch {
	case len(end) == 0:
		return string(k) == string(key)
	case len(end) == 1 && end[0] == 0:
		return string(k) >= string(key)
	default:
		return string(k) >= string(key) && string(k) < string(end)
	}
}

// entry is the record of an etcd key. A deleted key keeps a tombstone record,
// which gives the deletion its revision, until the tombstone is compacted.
type entry struct {
	kv        *mvccpb.KeyValue
	tombstone bool
	// versionId of the record in the backend, which the next write of the key expects
	versionId int64
}

// encodeEntry stores what the revision of the record doesn't give: the key's
// mod revision is the record's version id. A key that is created doesn't
// know its revision yet, so it stores a create revision of 0, which reads as
// the mod revision.
func encodeEntry(kv *mvccpb.KeyValue, tombstone bool) ([]byte, error) {
	stored := &mvccpb.KeyValue{
		CreateRevision: kv.CreateRevision,
		Version:        kv.Version,
		Value:          kv.Value,
		Lease:          kv.Lease,
	}
	b, err := proto.Marshal(stored)
	if err != nil {
		return nil, err
	}
	flag := flagLive
	if tombstone {
		flag = flagTombstone
	}
	return append([]byte{flag}, b...), nil
}

func decodeEntry(key []byte, data []byte, versionId int64) (*entry, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty record for key %q", key)
	}
	kv := &mvccpb.KeyValue{}
	if err := proto.Unmarshal(data[1:], kv); err != nil {
		return nil, err
	}
	kv.Key = key
	kv.ModRevision = versionId
	if kv.CreateRevision == 0 {
		kv.CreateRevision = versionId
	}
	return &entry{kv: kv, tombstone: data[0] == flagTombstone, versionId: versionId}, nil
}
