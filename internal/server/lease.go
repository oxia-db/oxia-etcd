package server

import (
	"context"
	"errors"
	"io"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
)

type leaseServer struct {
	pb.UnimplementedLeaseServer
	s *Server
}

func (l *leaseServer) LeaseGrant(ctx context.Context, r *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error) {
	resp, err := l.s.st.LeaseGrant(ctx, r)
	if err != nil {
		return nil, err
	}
	resp.Header = l.s.header(resp.Header)
	return resp, nil
}

func (l *leaseServer) LeaseRevoke(ctx context.Context, r *pb.LeaseRevokeRequest) (*pb.LeaseRevokeResponse, error) {
	resp, err := l.s.st.LeaseRevoke(ctx, r.ID)
	if err != nil {
		return nil, err
	}
	resp.Header = l.s.header(resp.Header)
	return resp, nil
}

// LeaseKeepAlive renews leases. A lease that doesn't exist gets a TTL of 0, as in etcd.
func (l *leaseServer) LeaseKeepAlive(stream grpc.BidiStreamingServer[pb.LeaseKeepAliveRequest, pb.LeaseKeepAliveResponse]) error {
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		ttl, err := l.s.st.LeaseRenew(req.ID)
		if err != nil {
			return err
		}
		if err := stream.Send(&pb.LeaseKeepAliveResponse{Header: l.s.header(nil), ID: req.ID, TTL: ttl}); err != nil {
			return err
		}
	}
}

func (l *leaseServer) LeaseTimeToLive(_ context.Context, r *pb.LeaseTimeToLiveRequest) (*pb.LeaseTimeToLiveResponse, error) {
	resp, err := l.s.st.LeaseTimeToLive(r)
	if err != nil {
		return nil, err
	}
	resp.Header = l.s.header(resp.Header)
	return resp, nil
}

func (l *leaseServer) LeaseLeases(context.Context, *pb.LeaseLeasesRequest) (*pb.LeaseLeasesResponse, error) {
	resp, err := l.s.st.Leases()
	if err != nil {
		return nil, err
	}
	resp.Header = l.s.header(resp.Header)
	return resp, nil
}
