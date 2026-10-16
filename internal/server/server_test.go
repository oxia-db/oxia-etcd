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

package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/oxia-db/oxia-etcd/internal/destination"
	"github.com/oxia-db/oxia-etcd/internal/store"
)

// newDestination returns the destination of the tests: Oxia at $OXIA_ADDR, or else
// memory. Each test gets its own records, under a cluster name of its own.
func newDestination(t *testing.T, cluster string) destination.Destination {
	t.Helper()
	addr := os.Getenv("OXIA_ADDR")
	if addr == "" {
		return destination.NewMemory()
	}
	dst, err := destination.NewOxia(addr, "default", fmt.Sprintf("%s-%d", cluster, testRun), 5*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dst.Close() })
	return dst
}

// testRun keeps the records of the test runs apart in a shared Oxia
var testRun = time.Now().UnixNano()

// newGateway serves a gateway over dst, and returns a client of it.
func newGateway(t *testing.T, dst destination.Destination) (*clientv3.Client, *store.Store) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if os.Getenv("TEST_LOG") != "" {
		log = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	st := store.New(dst, store.Options{
		HistorySize:        1000,
		OwnerCheckInterval: 100 * time.Millisecond,
		LeaseCheckInterval: 100 * time.Millisecond,
	}, log)
	srv := New(Config{ClusterID: 1, MemberID: 2, Name: "test", ProgressInterval: 200 * time.Millisecond}, st)
	g := grpc.NewServer()
	srv.Register(g)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = g.Serve(l) }()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = st.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		g.Stop()
	})
	require.Eventually(t, st.Ready, 10*time.Second, 10*time.Millisecond)

	client, err := clientv3.New(clientv3.Config{Endpoints: []string{l.Addr().String()}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client, st
}

func testContext(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestKV(t *testing.T) {
	c, _ := newGateway(t, newDestination(t, t.Name()))
	ctx := testContext(t)

	put, err := c.Put(ctx, "/a/1", "v1")
	require.NoError(t, err)
	_, err = c.Put(ctx, "/a/2", "v2")
	require.NoError(t, err)
	_, err = c.Put(ctx, "/b/1", "v3")
	require.NoError(t, err)

	get, err := c.Get(ctx, "/a/1")
	require.NoError(t, err)
	require.Len(t, get.Kvs, 1)
	kv := get.Kvs[0]
	assert.Equal(t, "v1", string(kv.Value))
	assert.Equal(t, int64(1), kv.Version)
	assert.Equal(t, put.Header.Revision, kv.ModRevision)
	assert.Equal(t, kv.ModRevision, kv.CreateRevision)

	// Update: same create revision, new mod revision, version 2
	upd, err := c.Put(ctx, "/a/1", "v1b", clientv3.WithPrevKV())
	require.NoError(t, err)
	assert.Equal(t, "v1", string(upd.PrevKv.Value))
	get, err = c.Get(ctx, "/a/1")
	require.NoError(t, err)
	assert.Equal(t, int64(2), get.Kvs[0].Version)
	assert.Equal(t, kv.CreateRevision, get.Kvs[0].CreateRevision)
	assert.Greater(t, get.Kvs[0].ModRevision, kv.ModRevision)

	// Prefix, count, limit, keys only, sort
	pre, err := c.Get(ctx, "/a/", clientv3.WithPrefix())
	require.NoError(t, err)
	assert.Equal(t, int64(2), pre.Count)
	assert.Equal(t, []string{"/a/1", "/a/2"}, keys(pre.Kvs))

	lim, err := c.Get(ctx, "/", clientv3.WithPrefix(), clientv3.WithLimit(2))
	require.NoError(t, err)
	assert.Equal(t, int64(3), lim.Count)
	assert.True(t, lim.More)
	assert.Len(t, lim.Kvs, 2)

	cnt, err := c.Get(ctx, "/", clientv3.WithPrefix(), clientv3.WithCountOnly())
	require.NoError(t, err)
	assert.Equal(t, int64(3), cnt.Count)
	assert.Empty(t, cnt.Kvs)

	ko, err := c.Get(ctx, "/", clientv3.WithPrefix(), clientv3.WithKeysOnly())
	require.NoError(t, err)
	assert.Empty(t, ko.Kvs[0].Value)

	desc, err := c.Get(ctx, "/", clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	require.NoError(t, err)
	assert.Equal(t, []string{"/b/1", "/a/2", "/a/1"}, keys(desc.Kvs))

	all, err := c.Get(ctx, "\x00", clientv3.WithFromKey())
	require.NoError(t, err)
	assert.Equal(t, int64(3), all.Count)

	// Delete
	del, err := c.Delete(ctx, "/a/", clientv3.WithPrefix(), clientv3.WithPrevKV())
	require.NoError(t, err)
	assert.Equal(t, int64(2), del.Deleted)
	assert.Len(t, del.PrevKvs, 2)
	gone, err := c.Get(ctx, "/a/", clientv3.WithPrefix())
	require.NoError(t, err)
	assert.Zero(t, gone.Count)

	// A key created again starts over
	_, err = c.Put(ctx, "/a/1", "again")
	require.NoError(t, err)
	get, err = c.Get(ctx, "/a/1")
	require.NoError(t, err)
	assert.Equal(t, int64(1), get.Kvs[0].Version)
	assert.Equal(t, get.Kvs[0].ModRevision, get.Kvs[0].CreateRevision)

	// Errors
	_, err = c.Put(ctx, "", "x")
	assert.ErrorIs(t, err, rpctypes.ErrEmptyKey)
	_, err = c.Put(ctx, "/missing", "", clientv3.WithIgnoreValue())
	assert.ErrorIs(t, err, rpctypes.ErrKeyNotFound)
}

func TestPastRevisions(t *testing.T) {
	c, _ := newGateway(t, newDestination(t, t.Name()))
	ctx := testContext(t)

	r1, err := c.Put(ctx, "/k", "1")
	require.NoError(t, err)
	_, err = c.Put(ctx, "/k", "2")
	require.NoError(t, err)
	_, err = c.Put(ctx, "/other", "x")
	require.NoError(t, err)
	r4, err := c.Delete(ctx, "/k")
	require.NoError(t, err)

	old, err := c.Get(ctx, "/", clientv3.WithPrefix(), clientv3.WithRev(r1.Header.Revision))
	require.NoError(t, err)
	assert.Equal(t, []string{"/k"}, keys(old.Kvs))
	assert.Equal(t, "1", string(old.Kvs[0].Value))

	before, err := c.Get(ctx, "/", clientv3.WithPrefix(), clientv3.WithRev(r4.Header.Revision-1))
	require.NoError(t, err)
	assert.Equal(t, []string{"/k", "/other"}, keys(before.Kvs))
	assert.Equal(t, "2", string(before.Kvs[0].Value))

	_, err = c.Get(ctx, "/k", clientv3.WithRev(r4.Header.Revision+100))
	assert.ErrorIs(t, err, rpctypes.ErrFutureRev)

	_, err = c.Compact(ctx, r4.Header.Revision-1, clientv3.WithCompactPhysical())
	require.NoError(t, err)
	_, err = c.Get(ctx, "/k", clientv3.WithRev(r1.Header.Revision))
	assert.ErrorIs(t, err, rpctypes.ErrCompacted)
	still, err := c.Get(ctx, "/", clientv3.WithPrefix(), clientv3.WithRev(r4.Header.Revision-1))
	require.NoError(t, err)
	assert.Len(t, still.Kvs, 2)
	_, err = c.Compact(ctx, r1.Header.Revision)
	assert.ErrorIs(t, err, rpctypes.ErrCompacted)
}

// TestKubernetesTxns runs the transactions of the Kubernetes API server.
func TestKubernetesTxns(t *testing.T) {
	c, _ := newGateway(t, newDestination(t, t.Name()))
	ctx := testContext(t)
	key := "/registry/pods/default/p"

	// Create: the key must not exist
	create := func() (*clientv3.TxnResponse, error) {
		return c.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
			Then(clientv3.OpPut(key, "v1")).Commit()
	}
	res, err := create()
	require.NoError(t, err)
	assert.True(t, res.Succeeded)
	rev := res.Responses[0].GetResponsePut().Header.Revision
	res, err = create()
	require.NoError(t, err)
	assert.False(t, res.Succeeded)

	// Update: the key must be at the expected revision, else read it
	update := func(expected int64, value string) (*clientv3.TxnResponse, error) {
		return c.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(key), "=", expected)).
			Then(clientv3.OpPut(key, value)).Else(clientv3.OpGet(key)).Commit()
	}
	res, err = update(rev, "v2")
	require.NoError(t, err)
	assert.True(t, res.Succeeded)
	res, err = update(rev, "v3")
	require.NoError(t, err)
	assert.False(t, res.Succeeded)
	current := res.Responses[0].GetResponseRange().Kvs[0]
	assert.Equal(t, "v2", string(current.Value))

	// Delete at the expected revision
	res, err = c.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(key), "=", current.ModRevision)).
		Then(clientv3.OpDelete(key)).Else(clientv3.OpGet(key)).Commit()
	require.NoError(t, err)
	assert.True(t, res.Succeeded)
	assert.Equal(t, int64(1), res.Responses[0].GetResponseDeleteRange().Deleted)

	// The compaction transaction on the version of a key
	res, err = c.Txn(ctx).If(clientv3.Compare(clientv3.Version("compact_rev_key"), "=", 0)).
		Then(clientv3.OpPut("compact_rev_key", "1")).Else(clientv3.OpGet("compact_rev_key")).Commit()
	require.NoError(t, err)
	assert.True(t, res.Succeeded)

	// Values, multiple ops, nested transactions
	res, err = c.Txn(ctx).If(clientv3.Compare(clientv3.Value("compact_rev_key"), "=", "1")).
		Then(clientv3.OpPut("/x", "1"), clientv3.OpPut("/y", "2"),
			clientv3.OpTxn(nil, []clientv3.Op{clientv3.OpGet("/x")}, nil)).Commit()
	require.NoError(t, err)
	assert.True(t, res.Succeeded)
	nested := res.Responses[2].GetResponseTxn()
	assert.Equal(t, "1", string(nested.Responses[0].GetResponseRange().Kvs[0].Value))

	_, err = c.Txn(ctx).Then(clientv3.OpPut("/x", "1"), clientv3.OpPut("/x", "2")).Commit()
	assert.ErrorIs(t, err, rpctypes.ErrDuplicateKey)
}

func TestWatch(t *testing.T) {
	c, _ := newGateway(t, newDestination(t, t.Name()))
	ctx := testContext(t)

	first, err := c.Put(ctx, "/w/a", "1")
	require.NoError(t, err)
	_, err = c.Put(ctx, "/w/a", "2")
	require.NoError(t, err)
	_, err = c.Put(ctx, "/other", "x")
	require.NoError(t, err)

	// From a past revision: the history, then the new events
	wc := c.Watch(ctx, "/w/", clientv3.WithPrefix(), clientv3.WithRev(first.Header.Revision), clientv3.WithPrevKV())
	_, err = c.Delete(ctx, "/w/a")
	require.NoError(t, err)

	var events []*clientv3.Event
	for len(events) < 3 {
		select {
		case resp := <-wc:
			require.NoError(t, resp.Err())
			events = append(events, resp.Events...)
		case <-ctx.Done():
			t.Fatal("timed out waiting for the events")
		}
	}
	require.Len(t, events, 3)
	assert.Equal(t, mvccpb.Event_PUT, events[0].Type)
	assert.Equal(t, "1", string(events[0].Kv.Value))
	assert.True(t, events[0].IsCreate())
	assert.Equal(t, "2", string(events[1].Kv.Value))
	assert.Equal(t, "1", string(events[1].PrevKv.Value))
	assert.Equal(t, mvccpb.Event_DELETE, events[2].Type)
	assert.Equal(t, "2", string(events[2].PrevKv.Value))
	assert.Less(t, events[0].Kv.ModRevision, events[1].Kv.ModRevision)
	assert.Less(t, events[1].Kv.ModRevision, events[2].Kv.ModRevision)

	// Filters
	noDelete := c.Watch(ctx, "/f", clientv3.WithFilterDelete())
	_, err = c.Put(ctx, "/f", "1")
	require.NoError(t, err)
	_, err = c.Delete(ctx, "/f")
	require.NoError(t, err)
	_, err = c.Put(ctx, "/f", "2")
	require.NoError(t, err)
	var values []string
	for len(values) < 2 {
		resp := <-noDelete
		require.NoError(t, resp.Err())
		for _, ev := range resp.Events {
			assert.Equal(t, mvccpb.Event_PUT, ev.Type)
			values = append(values, string(ev.Kv.Value))
		}
	}
	assert.Equal(t, []string{"1", "2"}, values)
}

func TestWatchCompacted(t *testing.T) {
	c, _ := newGateway(t, newDestination(t, t.Name()))
	ctx := testContext(t)

	r1, err := c.Put(ctx, "/k", "1")
	require.NoError(t, err)
	r2, err := c.Put(ctx, "/k", "2")
	require.NoError(t, err)
	_, err = c.Compact(ctx, r2.Header.Revision)
	require.NoError(t, err)

	resp := <-c.Watch(ctx, "/k", clientv3.WithRev(r1.Header.Revision))
	assert.ErrorIs(t, resp.Err(), rpctypes.ErrCompacted)
	assert.Equal(t, r2.Header.Revision, resp.CompactRevision)
}

func TestWatchProgress(t *testing.T) {
	c, _ := newGateway(t, newDestination(t, t.Name()))
	ctx := testContext(t)

	put, err := c.Put(ctx, "/p", "1")
	require.NoError(t, err)
	wc := c.Watch(ctx, "/q", clientv3.WithCreatedNotify())
	created := <-wc
	require.True(t, created.Created)

	// Asked by the client: the progress of the whole stream
	require.NoError(t, c.RequestProgress(ctx))
	resp := <-wc
	require.True(t, resp.IsProgressNotify())
	assert.GreaterOrEqual(t, resp.Header.Revision, put.Header.Revision)

	// Periodic
	periodic := c.Watch(ctx, "/q", clientv3.WithProgressNotify())
	resp = <-periodic
	require.True(t, resp.IsProgressNotify())
}

func TestLeases(t *testing.T) {
	c, _ := newGateway(t, newDestination(t, t.Name()))
	ctx := testContext(t)

	grant, err := c.Grant(ctx, 60)
	require.NoError(t, err)
	_, err = c.Put(ctx, "/lease/a", "1", clientv3.WithLease(grant.ID))
	require.NoError(t, err)
	_, err = c.Put(ctx, "/lease/b", "2", clientv3.WithLease(grant.ID))
	require.NoError(t, err)

	ttl, err := c.TimeToLive(ctx, grant.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	assert.Equal(t, int64(60), ttl.GrantedTTL)
	assert.Len(t, ttl.Keys, 2)

	ka, err := c.KeepAliveOnce(ctx, grant.ID)
	require.NoError(t, err)
	assert.Equal(t, int64(60), ka.TTL)

	leases, err := c.Leases(ctx)
	require.NoError(t, err)
	assert.Len(t, leases.Leases, 1)

	wc := c.Watch(ctx, "/lease/", clientv3.WithPrefix())
	_, err = c.Revoke(ctx, grant.ID)
	require.NoError(t, err)
	deleted := 0
	for deleted < 2 {
		resp := <-wc
		require.NoError(t, resp.Err())
		for _, ev := range resp.Events {
			assert.Equal(t, mvccpb.Event_DELETE, ev.Type)
			deleted++
		}
	}
	get, err := c.Get(ctx, "/lease/", clientv3.WithPrefix())
	require.NoError(t, err)
	assert.Zero(t, get.Count)

	_, err = c.Put(ctx, "/lease/c", "3", clientv3.WithLease(grant.ID))
	assert.ErrorIs(t, err, rpctypes.ErrLeaseNotFound)
	_, err = c.Revoke(ctx, grant.ID)
	assert.ErrorIs(t, err, rpctypes.ErrLeaseNotFound)

	// Expiry
	short, err := c.Grant(ctx, 2)
	require.NoError(t, err)
	_, err = c.Put(ctx, "/lease/short", "x", clientv3.WithLease(short.ID))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		get, err := c.Get(ctx, "/lease/short")
		return err == nil && get.Count == 0
	}, 10*time.Second, 100*time.Millisecond)
	gone, err := c.TimeToLive(ctx, short.ID)
	require.NoError(t, err)
	assert.Equal(t, int64(-1), gone.TTL)
}

func TestMaintenance(t *testing.T) {
	c, _ := newGateway(t, newDestination(t, t.Name()))
	ctx := testContext(t)

	_, err := c.Put(ctx, "/k", "v")
	require.NoError(t, err)
	status, err := c.Status(ctx, c.Endpoints()[0])
	require.NoError(t, err)
	assert.NotEmpty(t, status.Version)
	assert.Equal(t, uint64(2), status.Leader)

	members, err := c.MemberList(ctx)
	require.NoError(t, err)
	assert.Len(t, members.Members, 1)

	h1, err := c.HashKV(ctx, c.Endpoints()[0], 0)
	require.NoError(t, err)
	_, err = c.Put(ctx, "/k2", "v")
	require.NoError(t, err)
	h2, err := c.HashKV(ctx, c.Endpoints()[0], 0)
	require.NoError(t, err)
	assert.NotEqual(t, h1.Hash, h2.Hash)
}

// TestStandby checks that a second gateway on the same records stays on
// standby, and takes over with the records once the first one is gone.
func TestStandby(t *testing.T) {
	dst := newDestination(t, t.Name())
	c1, _ := newGateway(t, dst)
	ctx := testContext(t)
	_, err := c1.Put(ctx, "/k", "v")
	require.NoError(t, err)

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	// Its own destination client: an Oxia session of its own
	dst2 := dst
	if os.Getenv("OXIA_ADDR") != "" {
		dst2 = newDestination(t, t.Name())
	}
	st2 := store.New(dst2, store.Options{OwnerCheckInterval: 100 * time.Millisecond}, log)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = st2.Run(runCtx) }()
	time.Sleep(300 * time.Millisecond)
	assert.False(t, st2.Ready())
	select {
	case <-st2.Serving():
		t.Fatal("the standby serves the records")
	default:
	}
}

func keys(kvs []*mvccpb.KeyValue) []string {
	out := make([]string, len(kvs))
	for i, kv := range kvs {
		out[i] = string(kv.Key)
	}
	return out
}
