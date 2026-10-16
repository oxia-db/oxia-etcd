package server

import (
	"context"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/api/v3/version"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type maintenanceServer struct {
	pb.UnimplementedMaintenanceServer
	s *Server
}

func (m *maintenanceServer) Status(context.Context, *pb.StatusRequest) (*pb.StatusResponse, error) {
	if !m.s.st.Ready() {
		return nil, rpctypes.ErrGRPCNoLeader
	}
	rev := uint64(m.s.st.Rev())
	size := m.s.st.Size()
	return &pb.StatusResponse{
		Header:           m.s.header(nil),
		Version:          version.Version,
		DbSize:           size,
		DbSizeInUse:      size,
		Leader:           m.s.cfg.MemberID,
		RaftIndex:        rev,
		RaftAppliedIndex: rev,
		RaftTerm:         1,
	}, nil
}

// Alarm reports no alarm: the space is the backend's to manage.
func (m *maintenanceServer) Alarm(context.Context, *pb.AlarmRequest) (*pb.AlarmResponse, error) {
	return &pb.AlarmResponse{Header: m.s.header(nil)}, nil
}

// Defragment has nothing to do: the backend manages its storage.
func (m *maintenanceServer) Defragment(context.Context, *pb.DefragmentRequest) (*pb.DefragmentResponse, error) {
	return &pb.DefragmentResponse{Header: m.s.header(nil)}, nil
}

func (m *maintenanceServer) Hash(ctx context.Context, _ *pb.HashRequest) (*pb.HashResponse, error) {
	hash, _, err := m.s.st.HashKV(ctx, 0)
	if err != nil {
		return nil, err
	}
	return &pb.HashResponse{Header: m.s.header(nil), Hash: hash}, nil
}

func (m *maintenanceServer) HashKV(ctx context.Context, r *pb.HashKVRequest) (*pb.HashKVResponse, error) {
	hash, rev, err := m.s.st.HashKV(ctx, r.Revision)
	if err != nil {
		return nil, err
	}
	return &pb.HashKVResponse{Header: m.s.header(nil), Hash: hash, HashRevision: rev, CompactRevision: m.s.st.CompactRev()}, nil
}

func (m *maintenanceServer) Snapshot(*pb.SnapshotRequest, grpc.ServerStreamingServer[pb.SnapshotResponse]) error {
	return status.Error(codes.Unimplemented, "oxia-etcd: snapshots are taken with the Oxia backups")
}

func (m *maintenanceServer) MoveLeader(context.Context, *pb.MoveLeaderRequest) (*pb.MoveLeaderResponse, error) {
	return nil, status.Error(codes.Unimplemented, "oxia-etcd: leadership is managed by Oxia")
}

func (m *maintenanceServer) Downgrade(context.Context, *pb.DowngradeRequest) (*pb.DowngradeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "oxia-etcd: downgrade is not supported")
}

type clusterServer struct {
	pb.UnimplementedClusterServer
	s *Server
}

// MemberList lists the gateway as the one member: the replicas are Oxia's.
func (c *clusterServer) MemberList(context.Context, *pb.MemberListRequest) (*pb.MemberListResponse, error) {
	return &pb.MemberListResponse{
		Header: c.s.header(nil),
		Members: []*pb.Member{{
			ID:         c.s.cfg.MemberID,
			Name:       c.s.cfg.Name,
			ClientURLs: c.s.cfg.ClientURLs,
		}},
	}, nil
}

func (c *clusterServer) MemberAdd(context.Context, *pb.MemberAddRequest) (*pb.MemberAddResponse, error) {
	return nil, errMembership
}

func (c *clusterServer) MemberRemove(context.Context, *pb.MemberRemoveRequest) (*pb.MemberRemoveResponse, error) {
	return nil, errMembership
}

func (c *clusterServer) MemberUpdate(context.Context, *pb.MemberUpdateRequest) (*pb.MemberUpdateResponse, error) {
	return nil, errMembership
}

func (c *clusterServer) MemberPromote(context.Context, *pb.MemberPromoteRequest) (*pb.MemberPromoteResponse, error) {
	return nil, errMembership
}

var errMembership = status.Error(codes.Unimplemented, "oxia-etcd: membership is managed by Oxia")

// authServer reports authentication as disabled: the gateway relies on TLS
// and the network to protect access.
type authServer struct {
	pb.UnimplementedAuthServer
	s *Server
}

func (a *authServer) AuthStatus(context.Context, *pb.AuthStatusRequest) (*pb.AuthStatusResponse, error) {
	return &pb.AuthStatusResponse{Header: a.s.header(nil), Enabled: false}, nil
}

func (a *authServer) Authenticate(context.Context, *pb.AuthenticateRequest) (*pb.AuthenticateResponse, error) {
	return nil, rpctypes.ErrGRPCAuthNotEnabled
}
