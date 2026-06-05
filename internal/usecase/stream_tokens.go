package usecase

import (
	"context"
	"fmt"
	"sync"

	pb "golang-learning/proto/gen"
	"golang-learning/shared"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type StreamTokensUseCase struct {
	callbackStore ICallbackStore
	secret        string

	streams sync.Map // requestID → pb.TokenService_DeliverTokensClient
	addrMap sync.Map // requestID → string (grpc addr RAM cache)

	connMu  sync.RWMutex
	connMap map[string]*grpc.ClientConn // addr → shared conn
}

func NewStreamTokens(callbackStore ICallbackStore, secret string) *StreamTokensUseCase {
	return &StreamTokensUseCase{
		callbackStore: callbackStore,
		secret:        secret,
		connMap:       make(map[string]*grpc.ClientConn),
	}
}

// Execute được gọi từ 1 goroutine duy nhất per requestID — không cần lock trên stream.
func (uc *StreamTokensUseCase) Execute(ctx context.Context, token shared.TokenEvent) error {
	stream, err := uc.getOrOpenStream(ctx, token.RequestID)
	if err != nil {
		return err
	}

	if err := stream.Send(&pb.TokenMessage{
		RequestId: token.RequestID,
		Delta:     token.Delta,
		Done:      token.Done,
	}); err != nil {
		return err
	}

	if token.Done {
		_, _ = stream.CloseAndRecv()
		uc.streams.Delete(token.RequestID)
		uc.addrMap.Delete(token.RequestID)
	}
	return nil
}

func (uc *StreamTokensUseCase) getOrOpenStream(ctx context.Context, requestID string) (pb.TokenService_DeliverTokensClient, error) {
	if s, ok := uc.streams.Load(requestID); ok {
		return s.(pb.TokenService_DeliverTokensClient), nil
	}

	addr, err := uc.resolveAddr(ctx, requestID)
	if err != nil {
		return nil, fmt.Errorf("resolve addr: %w", err)
	}

	conn, err := uc.getConn(addr)
	if err != nil {
		return nil, fmt.Errorf("grpc conn: %w", err)
	}

	stream, err := pb.NewTokenServiceClient(conn).DeliverTokens(ctx)
	if err != nil {
		return nil, fmt.Errorf("grpc stream: %w", err)
	}

	uc.streams.Store(requestID, stream)
	return stream, nil
}

func (uc *StreamTokensUseCase) resolveAddr(ctx context.Context, requestID string) (string, error) {
	if addr, ok := uc.addrMap.Load(requestID); ok {
		return addr.(string), nil
	}
	addr, err := uc.callbackStore.GetCallback(ctx, requestID)
	if err != nil {
		return "", err
	}
	uc.addrMap.Store(requestID, addr)
	return addr, nil
}

func (uc *StreamTokensUseCase) getConn(addr string) (*grpc.ClientConn, error) {
	uc.connMu.RLock()
	conn, ok := uc.connMap[addr]
	uc.connMu.RUnlock()
	if ok {
		return conn, nil
	}

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(streamCreds{secret: uc.secret}),
	)
	if err != nil {
		return nil, err
	}

	uc.connMu.Lock()
	uc.connMap[addr] = conn
	uc.connMu.Unlock()
	return conn, nil
}

type streamCreds struct{ secret string }

func (c streamCreds) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + c.secret}, nil
}
func (c streamCreds) RequireTransportSecurity() bool { return false }
