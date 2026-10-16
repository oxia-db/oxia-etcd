package server

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc"

	"github.com/oxia-db/oxia-etcd/internal/store"
)

const (
	// watchEventsChunk is the most events in one watch response.
	watchEventsChunk = 1000
	// progressWatchId is the watch id of a progress response for the whole
	// stream, as clientv3.InvalidWatchID.
	progressWatchId = -1
)

type watchServer struct {
	pb.UnimplementedWatchServer
	s *Server
}

func (w *watchServer) Watch(stream grpc.BidiStreamingServer[pb.WatchRequest, pb.WatchResponse]) error {
	if !w.s.st.Ready() {
		return rpctypes.ErrGRPCNoLeader
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	ws := &watchStream{
		s:        w.s,
		ctx:      ctx,
		cancel:   cancel,
		stream:   stream,
		sendC:    make(chan *pb.WatchResponse, 128),
		watchers: map[int64]*watcher{},
	}
	go ws.sendLoop()
	return ws.recvLoop()
}

// watchStream serves the watchers of one stream. Their responses go through
// one queue, so a response is sent after the ones queued before it.
type watchStream struct {
	s      *Server
	ctx    context.Context
	cancel context.CancelFunc
	stream grpc.BidiStreamingServer[pb.WatchRequest, pb.WatchResponse]
	sendC  chan *pb.WatchResponse

	mu       sync.Mutex
	watchers map[int64]*watcher
	nextId   int64
}

type watcher struct {
	id             int64
	key, end       []byte
	prevKv         bool
	noPut          bool
	noDelete       bool
	progressNotify bool
	canceled       chan struct{}
	// synced is the revision up to which the events were queued
	synced atomic.Int64
}

func (ws *watchStream) sendLoop() {
	for {
		select {
		case <-ws.ctx.Done():
			return
		case resp := <-ws.sendC:
			if err := ws.stream.Send(resp); err != nil {
				ws.cancel()
				return
			}
		}
	}
}

func (ws *watchStream) send(resp *pb.WatchResponse) bool {
	select {
	case ws.sendC <- resp:
		return true
	case <-ws.ctx.Done():
		return false
	}
}

func (ws *watchStream) recvLoop() error {
	defer ws.cancelAll()
	for {
		req, err := ws.stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		switch r := req.RequestUnion.(type) {
		case *pb.WatchRequest_CreateRequest:
			ws.create(r.CreateRequest)
		case *pb.WatchRequest_CancelRequest:
			ws.cancelWatch(r.CancelRequest.WatchId)
		case *pb.WatchRequest_ProgressRequest:
			go ws.progress()
		}
	}
}

func (ws *watchStream) create(r *pb.WatchCreateRequest) {
	key, end := r.Key, r.RangeEnd
	if len(key) == 0 {
		key = []byte{0}
	}
	w := &watcher{
		key:            key,
		end:            end,
		prevKv:         r.PrevKv,
		progressNotify: r.ProgressNotify,
		canceled:       make(chan struct{}),
	}
	for _, f := range r.Filters {
		switch f {
		case pb.WatchCreateRequest_NOPUT:
			w.noPut = true
		case pb.WatchCreateRequest_NODELETE:
			w.noDelete = true
		}
	}

	ws.mu.Lock()
	if r.WatchId != 0 {
		if _, ok := ws.watchers[r.WatchId]; ok {
			ws.mu.Unlock()
			ws.send(&pb.WatchResponse{Header: ws.s.header(nil), WatchId: r.WatchId, Created: true, Canceled: true,
				CancelReason: "mvcc: duplicate watch ID provided on the WatchStream"})
			return
		}
		w.id = r.WatchId
	} else {
		for {
			if _, ok := ws.watchers[ws.nextId]; !ok {
				break
			}
			ws.nextId++
		}
		w.id = ws.nextId
		ws.nextId++
	}
	ws.watchers[w.id] = w
	ws.mu.Unlock()

	rev := ws.s.st.Rev()
	start := r.StartRevision
	if start <= 0 {
		start = rev + 1
	}
	w.synced.Store(start - 1)
	// Queued before any event of the watcher
	ws.send(&pb.WatchResponse{Header: ws.s.header(&pb.ResponseHeader{Revision: rev}), WatchId: w.id, Created: true})
	go ws.run(w, start)
}

// run sends the events of the watcher from rev on, as they come.
func (ws *watchStream) run(w *watcher, rev int64) {
	var progress <-chan time.Time
	if w.progressNotify {
		t := time.NewTicker(time.Duration(ws.s.progressInterval.Load()))
		defer t.Stop()
		progress = t.C
	}
	for {
		events, current, changed, ok := ws.s.st.Events(rev)
		if !ok {
			ws.send(&pb.WatchResponse{
				Header:          ws.s.header(nil),
				WatchId:         w.id,
				CompactRevision: ws.s.st.OldestWatchRev(),
				Canceled:        true,
				CancelReason:    rpctypes.ErrGRPCCompacted.Error(),
			})
			ws.remove(w.id)
			return
		}
		if matched := w.filter(events); len(matched) > 0 {
			for len(matched) > 0 {
				n := min(len(matched), watchEventsChunk)
				if !ws.send(&pb.WatchResponse{
					Header:  ws.s.header(&pb.ResponseHeader{Revision: current}),
					WatchId: w.id,
					Events:  matched[:n],
				}) {
					return
				}
				matched = matched[n:]
			}
		}
		if current >= rev {
			rev = current + 1
		}
		w.synced.Store(rev - 1)

		select {
		case <-changed:
		case <-w.canceled:
			return
		case <-ws.ctx.Done():
			return
		case <-progress:
			// Only a watcher with no event left to send reports its progress
			if events, current, _, ok := ws.s.st.Events(rev); ok && len(events) == 0 {
				ws.send(&pb.WatchResponse{Header: ws.s.header(&pb.ResponseHeader{Revision: current}), WatchId: w.id})
			}
		}
	}
}

func (w *watcher) filter(events []*mvccpb.Event) []*mvccpb.Event {
	var matched []*mvccpb.Event
	for _, ev := range events {
		if (ev.Type == mvccpb.Event_PUT && w.noPut) || (ev.Type == mvccpb.Event_DELETE && w.noDelete) {
			continue
		}
		if !store.InRange(ev.Kv.Key, w.key, w.end) {
			continue
		}
		if !w.prevKv && ev.PrevKv != nil {
			ev = &mvccpb.Event{Type: ev.Type, Kv: ev.Kv}
		}
		matched = append(matched, ev)
	}
	return matched
}

// progress tells the stream the current revision, once every watcher of the
// stream has queued its events up to it.
func (ws *watchStream) progress() {
	rev := ws.s.st.Rev()
	for {
		ws.mu.Lock()
		synced := true
		for _, w := range ws.watchers {
			if w.synced.Load() < rev {
				synced = false
				break
			}
		}
		ws.mu.Unlock()
		if synced {
			break
		}
		select {
		case <-ws.ctx.Done():
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
	ws.send(&pb.WatchResponse{Header: ws.s.header(&pb.ResponseHeader{Revision: rev}), WatchId: progressWatchId})
}

func (ws *watchStream) cancelWatch(id int64) {
	if ws.remove(id) {
		ws.send(&pb.WatchResponse{Header: ws.s.header(nil), WatchId: id, Canceled: true})
	}
}

func (ws *watchStream) remove(id int64) bool {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	w, ok := ws.watchers[id]
	if !ok {
		return false
	}
	close(w.canceled)
	delete(ws.watchers, id)
	return true
}

func (ws *watchStream) cancelAll() {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	for id, w := range ws.watchers {
		close(w.canceled)
		delete(ws.watchers, id)
	}
}
