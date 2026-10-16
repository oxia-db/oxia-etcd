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
