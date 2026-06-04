package grpc

import (
	"crypto/subtle"
	"io"
	"strings"

	"golang-learning/internal/usecase"
	pb "golang-learning/proto/gen"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type TokenServer struct {
	pb.UnimplementedTokenServiceServer
	hub usecase.ITokenHub
}

func NewTokenServer(hub usecase.ITokenHub) *TokenServer {
	return &TokenServer{hub: hub}
}

func (s *TokenServer) DeliverTokens(stream pb.TokenService_DeliverTokensServer) error {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pb.Ack{})
		}
		if err != nil {
			return err
		}
		if msg.RequestId == "" {
			return status.Error(codes.InvalidArgument, "request_id required")
		}
		s.hub.Deliver(msg.RequestId, usecase.PubSubToken{
			RequestID: msg.RequestId,
			Delta:     msg.Delta,
			Done:      msg.Done,
		})
	}
}

// StreamAuthInterceptor validates the Authorization metadata on every stream.
func StreamAuthInterceptor(secret string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if secret == "" {
			return status.Error(codes.Internal, "server not configured")
		}
		md, ok := metadata.FromIncomingContext(ss.Context())
		if !ok {
			return status.Error(codes.Unauthenticated, "missing metadata")
		}
		values := md.Get("authorization")
		if len(values) == 0 {
			return status.Error(codes.Unauthenticated, "missing authorization")
		}
		bearer := strings.TrimPrefix(values[0], "Bearer ")
		if !strings.HasPrefix(values[0], "Bearer ") || subtle.ConstantTimeCompare([]byte(bearer), []byte(secret)) != 1 {
			return status.Error(codes.Unauthenticated, "invalid credentials")
		}
		return handler(srv, ss)
	}
}
