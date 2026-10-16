package server

import (
	"context"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
)

// rangeStreamChunk is the number of keys in each response of a RangeStream.
const rangeStreamChunk = 1000

type kvServer struct {
	pb.UnimplementedKVServer
	s *Server
}

func (k *kvServer) Range(ctx context.Context, r *pb.RangeRequest) (*pb.RangeResponse, error) {
	resp, err := k.s.st.Range(ctx, r)
	if err != nil {
		return nil, err
	}
	resp.Header = k.s.header(resp.Header)
	return resp, nil
}

func (k *kvServer) RangeStream(r *pb.RangeRequest, stream grpc.ServerStreamingServer[pb.RangeStreamResponse]) error {
	resp, err := k.Range(stream.Context(), r)
	if err != nil {
		return err
	}
	// The keys go in chunks, and the header, more and count with the last one
	kvs := resp.Kvs
	for len(kvs) > rangeStreamChunk {
		if err := stream.Send(&pb.RangeStreamResponse{
			RangeResponse: &pb.RangeResponse{Kvs: kvs[:rangeStreamChunk]},
		}); err != nil {
			return err
		}
		kvs = kvs[rangeStreamChunk:]
	}
	resp.Kvs = kvs
	return stream.Send(&pb.RangeStreamResponse{RangeResponse: resp})
}

func (k *kvServer) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	resp, err := k.s.st.Put(ctx, r)
	if err != nil {
		return nil, err
	}
	resp.Header = k.s.header(resp.Header)
	return resp, nil
}

func (k *kvServer) DeleteRange(ctx context.Context, r *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	resp, err := k.s.st.DeleteRange(ctx, r)
	if err != nil {
		return nil, err
	}
	resp.Header = k.s.header(resp.Header)
	return resp, nil
}

func (k *kvServer) Txn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error) {
	resp, err := k.s.st.Txn(ctx, r)
	if err != nil {
		return nil, err
	}
	resp.Header = k.s.header(resp.Header)
	return resp, nil
}

func (k *kvServer) Compact(ctx context.Context, r *pb.CompactionRequest) (*pb.CompactionResponse, error) {
	resp, err := k.s.st.Compact(ctx, r)
	if err != nil {
		return nil, err
	}
	resp.Header = k.s.header(resp.Header)
	return resp, nil
}
