// Package server serves the etcd v3 gRPC API over the store.
package server

import (
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"

	"github.com/oxia-db/oxia-etcd/internal/store"
)

type Config struct {
	ClusterID  uint64
	MemberID   uint64
	Name       string
	ClientURLs []string
	// ProgressInterval is how often an idle watcher that asked for progress
	// notifications gets one.
	ProgressInterval time.Duration
}

type Server struct {
	cfg Config
	st  *store.Store
}

func New(cfg Config, st *store.Store) *Server {
	if cfg.ProgressInterval <= 0 {
		cfg.ProgressInterval = 5 * time.Second
	}
	return &Server{cfg: cfg, st: st}
}

// Register registers the etcd services on g.
func (s *Server) Register(g *grpc.Server) {
	pb.RegisterKVServer(g, &kvServer{s: s})
	pb.RegisterWatchServer(g, &watchServer{s: s})
	pb.RegisterLeaseServer(g, &leaseServer{s: s})
	pb.RegisterMaintenanceServer(g, &maintenanceServer{s: s})
	pb.RegisterClusterServer(g, &clusterServer{s: s})
	pb.RegisterAuthServer(g, &authServer{s: s})
}

// header completes a header of the store with the cluster's identity.
func (s *Server) header(h *pb.ResponseHeader) *pb.ResponseHeader {
	if h == nil {
		h = &pb.ResponseHeader{Revision: s.st.Rev()}
	}
	h.ClusterId = s.cfg.ClusterID
	h.MemberId = s.cfg.MemberID
	h.RaftTerm = 1
	return h
}
