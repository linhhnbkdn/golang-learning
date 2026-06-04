package grpc

import (
	"io"
	"log/slog"

	"golang-learning/internal/usecase"
	pb "golang-learning/proto/gen"
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
		s.hub.Deliver(msg.RequestId, usecase.PubSubToken{
			RequestID: msg.RequestId,
			Delta:     msg.Delta,
			Done:      msg.Done,
		})
		slog.Debug("token delivered", "request_id", msg.RequestId, "done", msg.Done)
	}
}
