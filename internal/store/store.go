// Package store implements the etcd v3 data model over a backend.
//
// The gateway is the only writer of its backend records: an ephemeral owner
// record keeps other gateways on standby. Every write takes the store lock, so
// the writes, their revisions and their events follow one order. The revision
// of a write is the version id the backend gives its record, which grows with
// every write. A deleted key keeps a tombstone record, so the deletion gets a
// revision too.
package store

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"log/slog"
	"math/big"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"

	"github.com/oxia-db/oxia-etcd/internal/backend"
)

const (
	// MinLeaseTTL is the shortest lease, as in etcd with its default election timeout.
	MinLeaseTTL = 2
	// MaxLeaseTTL is the longest lease etcd accepts.
	MaxLeaseTTL = 9_000_000_000
	// MaxTxnOps is etcd's default limit of operations in a transaction.
	MaxTxnOps = 128
)

// ErrLostOwnership is returned by Run when another gateway took over the backend records.
var ErrLostOwnership = errors.New("store: another gateway owns the records")

type Options struct {
	// HistorySize is the number of recent events kept for the watchers and
	// the reads at a past revision.
	HistorySize int
	// MaxValueSize is the largest value a key can have.
	MaxValueSize int
	// OpTimeout bounds each backend operation.
	OpTimeout time.Duration
	// OwnerCheckInterval is how often the gateway checks it still owns the records.
	OwnerCheckInterval time.Duration
	// LeaseCheckInterval is how often expired leases are revoked.
	LeaseCheckInterval time.Duration
}

func (o *Options) withDefaults() Options {
	out := *o
	if out.HistorySize <= 0 {
		out.HistorySize = 100_000
	}
	if out.MaxValueSize <= 0 {
		out.MaxValueSize = 1536 * 1024
	}
	if out.OpTimeout <= 0 {
		out.OpTimeout = 10 * time.Second
	}
	if out.OwnerCheckInterval <= 0 {
		out.OwnerCheckInterval = 2 * time.Second
	}
	if out.LeaseCheckInterval <= 0 {
		out.LeaseCheckInterval = 500 * time.Millisecond
	}
	return out
}

type Store struct {
	be   backend.Backend
	opts Options
	log  *slog.Logger

	// mu is held to write, and shared to read, so that a read sees one revision
	mu sync.RWMutex
	// compactRev is the revision of the last compaction
	compactRev int64
	// dirty is set when the outcome of a write is unknown: the history may
	// miss its event, so it restarts before the next write
	dirty bool

	hist   *history
	leases *lessor
	ready  atomic.Bool
	size   atomic.Int64
}

func New(be backend.Backend, opts Options, log *slog.Logger) *Store {
	opts = opts.withDefaults()
	return &Store{
		be:     be,
		opts:   opts,
		log:    log,
		hist:   newHistory(opts.HistorySize),
		leases: newLessor(),
	}
}

// Ready reports whether the gateway owns the records and serves them.
func (s *Store) Ready() bool { return s.ready.Load() }

// Rev returns the current revision.
func (s *Store) Rev() int64 { return s.hist.current() }

// CompactRev returns the revision of the last compaction.
func (s *Store) CompactRev() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.compactRev
}

// Size returns an estimate of the size of the stored keys and values.
func (s *Store) Size() int64 { return s.size.Load() }

func (s *Store) opCtx() (context.Context, context.CancelFunc) {
	// Not the request context: a canceled request must not leave a write
	// with an unknown outcome
	return context.WithTimeout(context.Background(), s.opts.OpTimeout)
}

// Run takes the ownership of the records, waiting for the current owner to
// go away, loads them, and serves them until ctx ends or the ownership is lost.
func (s *Store) Run(ctx context.Context) error {
	token := randomToken()
	rev, err := s.acquire(ctx, token)
	if err != nil {
		return err
	}
	if err := s.recover(ctx, rev); err != nil {
		return err
	}
	s.ready.Store(true)
	defer s.ready.Store(false)
	s.log.Info("Serving the records", slog.Int64("revision", rev))

	owner := time.NewTicker(s.opts.OwnerCheckInterval)
	defer owner.Stop()
	leases := time.NewTicker(s.opts.LeaseCheckInterval)
	defer leases.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-owner.C:
			if err := s.checkOwner(token); err != nil {
				return err
			}
		case <-leases.C:
			s.revokeExpired()
		}
	}
}

func (s *Store) acquire(ctx context.Context, token string) (int64, error) {
	for {
		opCtx, cancel := s.opCtx()
		rev, err := s.be.PutEphemeral(opCtx, ownerKey, []byte(token), backend.VersionNotExists)
		cancel()
		if err == nil {
			return rev, nil
		}
		if errors.Is(err, backend.ErrConflict) {
			s.log.Info("Another gateway owns the records, standing by")
		} else {
			s.log.Warn("Failed to take the ownership of the records", slog.Any("error", err))
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func (s *Store) checkOwner(token string) error {
	ctx, cancel := s.opCtx()
	defer cancel()
	rec, err := s.be.Get(ctx, ownerKey)
	switch {
	case errors.Is(err, backend.ErrNotFound):
		return ErrLostOwnership
	case err != nil:
		s.log.Warn("Failed to check the ownership of the records", slog.Any("error", err))
		return nil
	case string(rec.Value) != token:
		return ErrLostOwnership
	}
	return nil
}

// recover loads the leases and their keys. The history starts at rev, the
// revision of the owner record, which is above every record before it.
func (s *Store) recover(ctx context.Context, rev int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	opCtx, cancel := context.WithTimeout(ctx, 10*s.opts.OpTimeout)
	defer cancel()

	var scanErr error
	err := s.be.Scan(opCtx, leasePrefix, leaseEnd, func(r backend.Record) bool {
		id, err := strconv.ParseInt(r.Key[len(leasePrefix):], 16, 64)
		if err != nil {
			scanErr = err
			return false
		}
		ttl, _ := binary.Varint(r.Value)
		s.leases.add(id, ttl)
		return true
	})
	if err = errors.Join(err, scanErr); err != nil {
		return err
	}

	var orphans []*entry
	var size int64
	err = s.be.Scan(opCtx, kvPrefix, kvEnd, func(r backend.Record) bool {
		e, err := recordEntry(r)
		if err != nil {
			scanErr = err
			return false
		}
		if e.tombstone {
			return true
		}
		size += int64(len(e.kv.Key) + len(e.kv.Value))
		if e.kv.Lease != 0 {
			if s.leases.exists(e.kv.Lease) {
				s.leases.move(e.kv.Key, 0, e.kv.Lease)
			} else {
				orphans = append(orphans, e)
			}
		}
		return true
	})
	if err = errors.Join(err, scanErr); err != nil {
		return err
	}
	s.size.Store(size)
	s.leases.renewAll()
	s.hist.reset(rev)
	s.compactRev = 0
	s.dirty = false

	// The keys of a lease that was revoked before they were all deleted
	for _, e := range orphans {
		if _, err := s.deleteEntry(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

// write puts the record of key over the one at version expected. When the
// backend can't tell whether the write happened, the record is read back: the
// gateway is the only writer, so a newer record with the written data is its own.
func (s *Store) write(key string, data []byte, expected int64) (int64, error) {
	ctx, cancel := s.opCtx()
	defer cancel()
	rev, err := s.be.Put(ctx, key, data, expected)
	if err == nil {
		return rev, nil
	}
	if errors.Is(err, backend.ErrConflict) {
		// Someone else writes the records
		s.dirty = true
		s.log.Error("Unexpected record version, another writer?", slog.String("key", key))
		return 0, rpctypes.ErrGRPCLeaderChanged
	}
	rec, getErr := s.be.Get(ctx, key)
	if getErr == nil && rec.VersionId != expected && bytes.Equal(rec.Value, data) {
		return rec.VersionId, nil
	}
	if errors.Is(getErr, backend.ErrNotFound) || (getErr == nil && rec.VersionId == expected) {
		// The write did not happen
		return 0, rpctypes.ErrGRPCTimeout
	}
	s.dirty = true
	s.log.Warn("Unknown outcome of a write", slog.String("key", key), slog.Any("error", err))
	return 0, rpctypes.ErrGRPCTimeout
}

// clean restarts the history after a write with an unknown outcome: the
// watchers are told it was compacted, and they read the keys again.
func (s *Store) clean() error {
	if !s.dirty {
		return nil
	}
	ctx, cancel := s.opCtx()
	defer cancel()
	rev, err := s.be.Put(ctx, "m-sync", nil, backend.VersionAny)
	if err != nil {
		return rpctypes.ErrGRPCTimeout
	}
	s.hist.reset(rev)
	s.dirty = false
	return nil
}

func (s *Store) lockWrite() error {
	if !s.Ready() {
		return rpctypes.ErrGRPCNoLeader
	}
	s.mu.Lock()
	if err := s.clean(); err != nil {
		s.mu.Unlock()
		return err
	}
	return nil
}

func (s *Store) lockRead() error {
	if !s.Ready() {
		return rpctypes.ErrGRPCNoLeader
	}
	s.mu.RLock()
	return nil
}

func (s *Store) header() *pb.ResponseHeader {
	return &pb.ResponseHeader{Revision: s.hist.current()}
}

func recordEntry(r backend.Record) (*entry, error) {
	key, err := decodeKvKey(r.Key)
	if err != nil {
		return nil, err
	}
	return decodeEntry(key, r.Value, r.VersionId)
}

// getEntry returns the record of key, live or tombstone, or nil.
func (s *Store) getEntry(key []byte) (*entry, error) {
	ctx, cancel := s.opCtx()
	defer cancel()
	r, err := s.be.Get(ctx, kvKey(key))
	if errors.Is(err, backend.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return decodeEntry(key, r.Value, r.VersionId)
}

// liveEntries returns the live keys in the etcd range [key, end), in key order.
func (s *Store) liveEntries(key, end []byte) ([]*entry, error) {
	if len(end) == 0 {
		e, err := s.getEntry(key)
		if err != nil || e == nil || e.tombstone {
			return nil, err
		}
		return []*entry{e}, nil
	}
	minKey, maxKey := scanBounds(key, end)
	if minKey >= maxKey {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*s.opts.OpTimeout)
	defer cancel()
	var entries []*entry
	var decodeErr error
	err := s.be.Scan(ctx, minKey, maxKey, func(r backend.Record) bool {
		e, err := recordEntry(r)
		if err != nil {
			decodeErr = err
			return false
		}
		if !e.tombstone {
			entries = append(entries, e)
		}
		return true
	})
	return entries, errors.Join(err, decodeErr)
}

func (s *Store) liveKVs(key, end []byte) ([]*mvccpb.KeyValue, error) {
	entries, err := s.liveEntries(key, end)
	if err != nil {
		return nil, err
	}
	kvs := make([]*mvccpb.KeyValue, len(entries))
	for i, e := range entries {
		kvs[i] = e.kv
	}
	return kvs, nil
}

// Range reads keys, at the current revision or a past one.
func (s *Store) Range(_ context.Context, r *pb.RangeRequest) (*pb.RangeResponse, error) {
	if err := s.lockRead(); err != nil {
		return nil, err
	}
	defer s.mu.RUnlock()
	resp, err := s.rangeLocked(r)
	if err != nil {
		return nil, err
	}
	resp.Header = s.header()
	return resp, nil
}

func (s *Store) rangeLocked(r *pb.RangeRequest) (*pb.RangeResponse, error) {
	if len(r.Key) == 0 {
		return nil, rpctypes.ErrGRPCEmptyKey
	}
	kvs, err := s.kvsAt(r.Key, r.RangeEnd, r.Revision)
	if err != nil {
		return nil, err
	}
	kvs = slices.DeleteFunc(kvs, func(kv *mvccpb.KeyValue) bool {
		return (r.MinModRevision > 0 && kv.ModRevision < r.MinModRevision) ||
			(r.MaxModRevision > 0 && kv.ModRevision > r.MaxModRevision) ||
			(r.MinCreateRevision > 0 && kv.CreateRevision < r.MinCreateRevision) ||
			(r.MaxCreateRevision > 0 && kv.CreateRevision > r.MaxCreateRevision)
	})
	if err := sortKVs(kvs, r.SortOrder, r.SortTarget); err != nil {
		return nil, err
	}

	resp := &pb.RangeResponse{Count: int64(len(kvs))}
	if r.Limit > 0 && int64(len(kvs)) > r.Limit {
		kvs = kvs[:r.Limit]
		resp.More = true
	}
	if r.CountOnly {
		return resp, nil
	}
	if r.KeysOnly {
		for i, kv := range kvs {
			kvs[i] = &mvccpb.KeyValue{Key: kv.Key, CreateRevision: kv.CreateRevision, ModRevision: kv.ModRevision,
				Version: kv.Version, Lease: kv.Lease}
		}
	}
	resp.Kvs = kvs
	return resp, nil
}

// kvsAt returns the live keys of the range at rev, 0 being the current
// revision: the current keys, with the events after rev undone.
func (s *Store) kvsAt(key, end []byte, rev int64) ([]*mvccpb.KeyValue, error) {
	current := s.hist.current()
	switch {
	case rev > current:
		return nil, rpctypes.ErrGRPCFutureRev
	case rev > 0 && rev < s.compactRev:
		return nil, rpctypes.ErrGRPCCompacted
	}
	kvs, err := s.liveKVs(key, end)
	if err != nil || rev <= 0 || rev == current {
		return kvs, err
	}
	events, _, _, ok := s.hist.since(rev + 1)
	if !ok {
		return nil, rpctypes.ErrGRPCCompacted
	}
	byKey := make(map[string]*mvccpb.KeyValue, len(kvs))
	for _, kv := range kvs {
		byKey[string(kv.Key)] = kv
	}
	for i := len(events) - 1; i >= 0; i-- {
		ev := events[i]
		if !InRange(ev.Kv.Key, key, end) {
			continue
		}
		if ev.PrevKv != nil {
			byKey[string(ev.Kv.Key)] = ev.PrevKv
		} else {
			delete(byKey, string(ev.Kv.Key))
		}
	}
	kvs = kvs[:0]
	for _, kv := range byKey {
		kvs = append(kvs, kv)
	}
	slices.SortFunc(kvs, func(a, b *mvccpb.KeyValue) int { return bytes.Compare(a.Key, b.Key) })
	return kvs, nil
}

func sortKVs(kvs []*mvccpb.KeyValue, order pb.RangeRequest_SortOrder, target pb.RangeRequest_SortTarget) error {
	if order == pb.RangeRequest_NONE {
		if target == pb.RangeRequest_KEY {
			// Already in key order
			return nil
		}
		order = pb.RangeRequest_ASCEND
	}
	var compare func(a, b *mvccpb.KeyValue) int
	switch target {
	case pb.RangeRequest_KEY:
		compare = func(a, b *mvccpb.KeyValue) int { return bytes.Compare(a.Key, b.Key) }
	case pb.RangeRequest_VERSION:
		compare = func(a, b *mvccpb.KeyValue) int { return cmp.Compare(a.Version, b.Version) }
	case pb.RangeRequest_CREATE:
		compare = func(a, b *mvccpb.KeyValue) int { return cmp.Compare(a.CreateRevision, b.CreateRevision) }
	case pb.RangeRequest_MOD:
		compare = func(a, b *mvccpb.KeyValue) int { return cmp.Compare(a.ModRevision, b.ModRevision) }
	case pb.RangeRequest_VALUE:
		compare = func(a, b *mvccpb.KeyValue) int { return bytes.Compare(a.Value, b.Value) }
	default:
		return rpctypes.ErrGRPCInvalidSortOption
	}
	switch order {
	case pb.RangeRequest_ASCEND:
		slices.SortStableFunc(kvs, compare)
	case pb.RangeRequest_DESCEND:
		slices.SortStableFunc(kvs, func(a, b *mvccpb.KeyValue) int { return compare(b, a) })
	default:
		return rpctypes.ErrGRPCInvalidSortOption
	}
	return nil
}

// Put writes a key.
func (s *Store) Put(_ context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	if err := s.checkPut(r); err != nil {
		return nil, err
	}
	if err := s.lockWrite(); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	resp, err := s.putLocked(r)
	if err != nil {
		return nil, err
	}
	resp.Header = s.header()
	return resp, nil
}

func (s *Store) checkPut(r *pb.PutRequest) error {
	switch {
	case len(r.Key) == 0:
		return rpctypes.ErrGRPCEmptyKey
	case r.IgnoreValue && len(r.Value) != 0:
		return rpctypes.ErrGRPCValueProvided
	case r.IgnoreLease && r.Lease != 0:
		return rpctypes.ErrGRPCLeaseProvided
	case len(r.Value) > s.opts.MaxValueSize:
		return rpctypes.ErrGRPCRequestTooLarge
	}
	return nil
}

func (s *Store) putLocked(r *pb.PutRequest) (*pb.PutResponse, error) {
	cur, err := s.getEntry(r.Key)
	if err != nil {
		return nil, err
	}
	live := cur != nil && !cur.tombstone
	value, lease := r.Value, r.Lease
	if r.IgnoreValue || r.IgnoreLease {
		if !live {
			return nil, rpctypes.ErrGRPCKeyNotFound
		}
		if r.IgnoreValue {
			value = cur.kv.Value
		}
		if r.IgnoreLease {
			lease = cur.kv.Lease
		}
	}
	if lease != 0 && !s.leases.exists(lease) {
		return nil, rpctypes.ErrGRPCLeaseNotFound
	}

	kv := &mvccpb.KeyValue{Key: r.Key, Value: value, Lease: lease, Version: 1}
	var prev *mvccpb.KeyValue
	expected := backend.VersionNotExists
	if cur != nil {
		expected = cur.versionId
	}
	if live {
		prev = cur.kv
		kv.CreateRevision = prev.CreateRevision
		kv.Version = prev.Version + 1
	}
	data, err := encodeEntry(kv, false)
	if err != nil {
		return nil, err
	}
	rev, err := s.write(kvKey(r.Key), data, expected)
	if err != nil {
		return nil, err
	}
	kv.ModRevision = rev
	if kv.CreateRevision == 0 {
		kv.CreateRevision = rev
	}
	var prevLease int64
	var prevSize int
	if prev != nil {
		prevLease = prev.Lease
		prevSize = len(prev.Key) + len(prev.Value)
	}
	s.leases.move(r.Key, prevLease, lease)
	s.size.Add(int64(len(kv.Key) + len(kv.Value) - prevSize))
	s.hist.append(&mvccpb.Event{Type: mvccpb.Event_PUT, Kv: kv, PrevKv: prev})

	resp := &pb.PutResponse{}
	if r.PrevKv {
		resp.PrevKv = prev
	}
	return resp, nil
}

// deleteEntry writes the tombstone of a live key.
func (s *Store) deleteEntry(_ context.Context, e *entry) (*mvccpb.KeyValue, error) {
	kv := &mvccpb.KeyValue{Key: e.kv.Key}
	data, err := encodeEntry(kv, true)
	if err != nil {
		return nil, err
	}
	rev, err := s.write(kvKey(e.kv.Key), data, e.versionId)
	if err != nil {
		return nil, err
	}
	kv.ModRevision = rev
	s.leases.move(e.kv.Key, e.kv.Lease, 0)
	s.size.Add(-int64(len(e.kv.Key) + len(e.kv.Value)))
	s.hist.append(&mvccpb.Event{Type: mvccpb.Event_DELETE, Kv: kv, PrevKv: e.kv})
	return e.kv, nil
}

// DeleteRange deletes keys.
func (s *Store) DeleteRange(ctx context.Context, r *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	if len(r.Key) == 0 {
		return nil, rpctypes.ErrGRPCEmptyKey
	}
	if err := s.lockWrite(); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	resp, err := s.deleteRangeLocked(ctx, r)
	if err != nil {
		return nil, err
	}
	resp.Header = s.header()
	return resp, nil
}

func (s *Store) deleteRangeLocked(ctx context.Context, r *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	entries, err := s.liveEntries(r.Key, r.RangeEnd)
	if err != nil {
		return nil, err
	}
	resp := &pb.DeleteRangeResponse{}
	for _, e := range entries {
		prev, err := s.deleteEntry(ctx, e)
		if err != nil {
			return nil, err
		}
		resp.Deleted++
		if r.PrevKv {
			resp.PrevKvs = append(resp.PrevKvs, prev)
		}
	}
	return resp, nil
}

// Txn runs a transaction. The transactions are serialized with the other
// writes, so no other write comes between its comparisons and its operations.
func (s *Store) Txn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error) {
	if err := s.checkTxn(r); err != nil {
		return nil, err
	}
	if isReadOnly(r) {
		if err := s.lockRead(); err != nil {
			return nil, err
		}
		defer s.mu.RUnlock()
	} else {
		if err := s.lockWrite(); err != nil {
			return nil, err
		}
		defer s.mu.Unlock()
	}
	resp, err := s.txnLocked(ctx, r)
	if err != nil {
		return nil, err
	}
	resp.Header = s.header()
	return resp, nil
}

func (s *Store) txnLocked(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error) {
	succeeded := true
	for _, c := range r.Compare {
		ok, err := s.compare(c)
		if err != nil {
			return nil, err
		}
		if !ok {
			succeeded = false
			break
		}
	}
	ops := r.Success
	if !succeeded {
		ops = r.Failure
	}
	// Fail before the first write rather than in the middle of the transaction
	for _, op := range ops {
		if put := op.GetRequestPut(); put != nil && put.Lease != 0 && !s.leases.exists(put.Lease) {
			return nil, rpctypes.ErrGRPCLeaseNotFound
		}
	}

	resp := &pb.TxnResponse{Succeeded: succeeded, Responses: make([]*pb.ResponseOp, 0, len(ops))}
	for _, op := range ops {
		var res *pb.ResponseOp
		switch req := op.Request.(type) {
		case *pb.RequestOp_RequestRange:
			rr, err := s.rangeLocked(req.RequestRange)
			if err != nil {
				return nil, err
			}
			rr.Header = s.header()
			res = &pb.ResponseOp{Response: &pb.ResponseOp_ResponseRange{ResponseRange: rr}}
		case *pb.RequestOp_RequestPut:
			pr, err := s.putLocked(req.RequestPut)
			if err != nil {
				return nil, err
			}
			pr.Header = s.header()
			res = &pb.ResponseOp{Response: &pb.ResponseOp_ResponsePut{ResponsePut: pr}}
		case *pb.RequestOp_RequestDeleteRange:
			dr, err := s.deleteRangeLocked(ctx, req.RequestDeleteRange)
			if err != nil {
				return nil, err
			}
			dr.Header = s.header()
			res = &pb.ResponseOp{Response: &pb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: dr}}
		case *pb.RequestOp_RequestTxn:
			tr, err := s.txnLocked(ctx, req.RequestTxn)
			if err != nil {
				return nil, err
			}
			tr.Header = s.header()
			res = &pb.ResponseOp{Response: &pb.ResponseOp_ResponseTxn{ResponseTxn: tr}}
		default:
			return nil, rpctypes.ErrGRPCNotCapable
		}
		resp.Responses = append(resp.Responses, res)
	}
	return resp, nil
}

func (s *Store) compare(c *pb.Compare) (bool, error) {
	kvs, err := s.liveKVs(c.Key, c.RangeEnd)
	if err != nil {
		return false, err
	}
	if len(kvs) == 0 {
		if c.Target == pb.Compare_VALUE {
			// A missing key has no value to compare
			return false, nil
		}
		return compareKV(c, &mvccpb.KeyValue{}), nil
	}
	for _, kv := range kvs {
		if !compareKV(c, kv) {
			return false, nil
		}
	}
	return true, nil
}

func compareKV(c *pb.Compare, kv *mvccpb.KeyValue) bool {
	var r int
	switch c.Target {
	case pb.Compare_VALUE:
		v, ok := c.TargetUnion.(*pb.Compare_Value)
		if !ok {
			return false
		}
		r = bytes.Compare(kv.Value, v.Value)
	case pb.Compare_VERSION:
		r = cmp.Compare(kv.Version, c.GetVersion())
	case pb.Compare_CREATE:
		r = cmp.Compare(kv.CreateRevision, c.GetCreateRevision())
	case pb.Compare_MOD:
		r = cmp.Compare(kv.ModRevision, c.GetModRevision())
	case pb.Compare_LEASE:
		r = cmp.Compare(kv.Lease, c.GetLease())
	default:
		return false
	}
	switch c.Result {
	case pb.Compare_EQUAL:
		return r == 0
	case pb.Compare_NOT_EQUAL:
		return r != 0
	case pb.Compare_GREATER:
		return r > 0
	case pb.Compare_LESS:
		return r < 0
	}
	return false
}

func isReadOnly(r *pb.TxnRequest) bool {
	for _, ops := range [][]*pb.RequestOp{r.Success, r.Failure} {
		for _, op := range ops {
			switch req := op.Request.(type) {
			case *pb.RequestOp_RequestRange:
			case *pb.RequestOp_RequestTxn:
				if !isReadOnly(req.RequestTxn) {
					return false
				}
			default:
				return false
			}
		}
	}
	return true
}

// checkTxn applies etcd's static checks: the size of the transaction, and no
// key written twice by the same branch.
func (s *Store) checkTxn(r *pb.TxnRequest) error {
	if len(r.Compare) > MaxTxnOps || len(r.Success) > MaxTxnOps || len(r.Failure) > MaxTxnOps {
		return rpctypes.ErrGRPCTooManyOps
	}
	for _, c := range r.Compare {
		if len(c.Key) == 0 {
			return rpctypes.ErrGRPCEmptyKey
		}
	}
	for _, ops := range [][]*pb.RequestOp{r.Success, r.Failure} {
		puts := map[string]struct{}{}
		var deletes []*pb.DeleteRangeRequest
		for _, op := range ops {
			switch req := op.Request.(type) {
			case *pb.RequestOp_RequestRange:
				if len(req.RequestRange.Key) == 0 {
					return rpctypes.ErrGRPCEmptyKey
				}
			case *pb.RequestOp_RequestPut:
				if err := s.checkPut(req.RequestPut); err != nil {
					return err
				}
				if _, dup := puts[string(req.RequestPut.Key)]; dup {
					return rpctypes.ErrGRPCDuplicateKey
				}
				puts[string(req.RequestPut.Key)] = struct{}{}
			case *pb.RequestOp_RequestDeleteRange:
				if len(req.RequestDeleteRange.Key) == 0 {
					return rpctypes.ErrGRPCEmptyKey
				}
				deletes = append(deletes, req.RequestDeleteRange)
			case *pb.RequestOp_RequestTxn:
				if err := s.checkTxn(req.RequestTxn); err != nil {
					return err
				}
			}
		}
		for _, d := range deletes {
			for k := range puts {
				if InRange([]byte(k), d.Key, d.RangeEnd) {
					return rpctypes.ErrGRPCDuplicateKey
				}
			}
		}
	}
	return nil
}

// Compact discards the history before rev, and the tombstones of the keys
// deleted before it.
func (s *Store) Compact(_ context.Context, r *pb.CompactionRequest) (*pb.CompactionResponse, error) {
	if err := s.lockWrite(); err != nil {
		return nil, err
	}
	switch {
	case r.Revision > s.hist.current():
		s.mu.Unlock()
		return nil, rpctypes.ErrGRPCFutureRev
	case r.Revision <= s.compactRev:
		s.mu.Unlock()
		return nil, rpctypes.ErrGRPCCompacted
	}
	s.compactRev = r.Revision
	s.hist.compact(r.Revision)
	header := s.header()
	s.mu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.removeTombstones(r.Revision)
	}()
	if r.Physical {
		<-done
	}
	return &pb.CompactionResponse{Header: header}, nil
}

func (s *Store) removeTombstones(rev int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*s.opts.OpTimeout)
	defer cancel()
	var tombstones []backend.Record
	err := s.be.Scan(ctx, kvPrefix, kvEnd, func(r backend.Record) bool {
		if r.VersionId <= rev && len(r.Value) > 0 && r.Value[0] == flagTombstone {
			tombstones = append(tombstones, backend.Record{Key: r.Key, VersionId: r.VersionId})
		}
		return true
	})
	if err != nil {
		s.log.Warn("Failed to scan the tombstones", slog.Any("error", err))
		return
	}
	for _, t := range tombstones {
		// Under the lock: a write of the key expects the tombstone's version
		s.mu.Lock()
		opCtx, opCancel := s.opCtx()
		err := s.be.Delete(opCtx, t.Key, t.VersionId)
		opCancel()
		s.mu.Unlock()
		if err != nil && !errors.Is(err, backend.ErrConflict) && !errors.Is(err, backend.ErrNotFound) {
			s.log.Warn("Failed to remove a tombstone", slog.String("key", t.Key), slog.Any("error", err))
		}
	}
	if len(tombstones) > 0 {
		s.log.Info("Removed tombstones", slog.Int64("revision", rev), slog.Int("count", len(tombstones)))
	}
}

// HashKV hashes the keys and values at rev, 0 being the current revision.
func (s *Store) HashKV(_ context.Context, rev int64) (hash uint32, hashRev int64, err error) {
	if err := s.lockRead(); err != nil {
		return 0, 0, err
	}
	defer s.mu.RUnlock()
	kvs, err := s.kvsAt([]byte{0}, []byte{0}, rev)
	if err != nil {
		return 0, 0, err
	}
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	for _, kv := range kvs {
		h.Write(kv.Key)
		h.Write(kv.Value)
	}
	if rev <= 0 {
		rev = s.hist.current()
	}
	return h.Sum32(), rev, nil
}

// LeaseGrant creates a lease.
func (s *Store) LeaseGrant(_ context.Context, r *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error) {
	if r.TTL > MaxLeaseTTL {
		return nil, rpctypes.ErrGRPCLeaseTTLTooLarge
	}
	if err := s.lockWrite(); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	id := r.ID
	if id == 0 {
		for id == 0 || s.leases.exists(id) {
			n, err := rand.Int(rand.Reader, big.NewInt(1<<62))
			if err != nil {
				return nil, err
			}
			id = n.Int64() + 1
		}
	} else if s.leases.exists(id) {
		return nil, rpctypes.ErrGRPCLeaseExist
	}
	ttl := max(r.TTL, MinLeaseTTL)
	ctx, cancel := s.opCtx()
	defer cancel()
	if _, err := s.be.Put(ctx, leaseKey(id), binary.AppendVarint(nil, ttl), backend.VersionNotExists); err != nil {
		if errors.Is(err, backend.ErrConflict) {
			return nil, rpctypes.ErrGRPCLeaseExist
		}
		return nil, err
	}
	s.leases.add(id, ttl)
	return &pb.LeaseGrantResponse{Header: s.header(), ID: id, TTL: ttl}, nil
}

// LeaseRevoke deletes a lease and its keys.
func (s *Store) LeaseRevoke(ctx context.Context, id int64) (*pb.LeaseRevokeResponse, error) {
	if err := s.lockWrite(); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	if !s.leases.exists(id) {
		return nil, rpctypes.ErrGRPCLeaseNotFound
	}
	if err := s.revokeLocked(ctx, id); err != nil {
		return nil, err
	}
	return &pb.LeaseRevokeResponse{Header: s.header()}, nil
}

// revokeLocked deletes the keys of the lease first, so that a lease record
// outlives its keys and a restart finishes an interrupted revoke.
func (s *Store) revokeLocked(ctx context.Context, id int64) error {
	keys := s.leases.keys(id)
	slices.SortFunc(keys, bytes.Compare)
	for _, key := range keys {
		e, err := s.getEntry(key)
		if err != nil {
			return err
		}
		if e == nil || e.tombstone || e.kv.Lease != id {
			continue
		}
		if _, err := s.deleteEntry(ctx, e); err != nil {
			return err
		}
	}
	opCtx, cancel := s.opCtx()
	defer cancel()
	if err := s.be.Delete(opCtx, leaseKey(id), backend.VersionAny); err != nil && !errors.Is(err, backend.ErrNotFound) {
		return err
	}
	s.leases.remove(id)
	return nil
}

func (s *Store) revokeExpired() {
	for _, id := range s.leases.expired() {
		if err := s.lockWrite(); err != nil {
			return
		}
		if s.leases.isExpired(id) {
			if err := s.revokeLocked(context.Background(), id); err != nil {
				s.log.Warn("Failed to revoke an expired lease", slog.Int64("lease", id), slog.Any("error", err))
			}
		}
		s.mu.Unlock()
	}
}

// LeaseRenew extends a lease, and returns its TTL, or 0 if there is no such lease.
func (s *Store) LeaseRenew(id int64) (int64, error) {
	if !s.Ready() {
		return 0, rpctypes.ErrGRPCNoLeader
	}
	ttl, _ := s.leases.renew(id)
	return ttl, nil
}

// LeaseTimeToLive describes a lease.
func (s *Store) LeaseTimeToLive(r *pb.LeaseTimeToLiveRequest) (*pb.LeaseTimeToLiveResponse, error) {
	if !s.Ready() {
		return nil, rpctypes.ErrGRPCNoLeader
	}
	remaining, granted, keys, ok := s.leases.timeToLive(r.ID)
	resp := &pb.LeaseTimeToLiveResponse{Header: s.header(), ID: r.ID, TTL: -1}
	if !ok {
		return resp, nil
	}
	resp.TTL = remaining
	resp.GrantedTTL = granted
	if r.Keys {
		slices.SortFunc(keys, bytes.Compare)
		resp.Keys = keys
	}
	return resp, nil
}

// Leases lists the leases.
func (s *Store) Leases() (*pb.LeaseLeasesResponse, error) {
	if !s.Ready() {
		return nil, rpctypes.ErrGRPCNoLeader
	}
	ids := s.leases.list()
	slices.Sort(ids)
	resp := &pb.LeaseLeasesResponse{Header: s.header()}
	for _, id := range ids {
		resp.Leases = append(resp.Leases, &pb.LeaseStatus{ID: id})
	}
	return resp, nil
}

// Events returns the events from rev on, up to the current revision, and a
// channel closed on the next change. ok is false when the events from rev on
// were compacted.
func (s *Store) Events(rev int64) (events []*mvccpb.Event, current int64, changed <-chan struct{}, ok bool) {
	return s.hist.since(rev)
}

// OldestWatchRev returns the lowest revision a watch can start from.
func (s *Store) OldestWatchRev() int64 {
	return max(s.hist.oldest(), s.CompactRev())
}

func randomToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
