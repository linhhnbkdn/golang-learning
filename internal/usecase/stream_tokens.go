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

	addrMu  sync.RWMutex
	addrMap map[string]string // requestID → grpc addr

	connMu  sync.RWMutex
	connMap map[string]*grpc.ClientConn // addr → conn

	streamMu  sync.Mutex
	streamMap map[string]pb.TokenService_DeliverTokensClient // requestID → stream
}

func NewStreamTokens(callbackStore ICallbackStore, secret string) *StreamTokensUseCase {
	return &StreamTokensUseCase{
		callbackStore: callbackStore,
		secret:        secret,
		addrMap:       make(map[string]string),
		connMap:       make(map[string]*grpc.ClientConn),
		streamMap:     make(map[string]pb.TokenService_DeliverTokensClient),
	}
}

func (uc *StreamTokensUseCase) Execute(ctx context.Context, token shared.TokenEvent) error {
	stream, err := uc.getStream(ctx, token.RequestID)
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
		uc.cleanup(token.RequestID)
	}
	return nil
}

func (uc *StreamTokensUseCase) getStream(ctx context.Context, requestID string) (pb.TokenService_DeliverTokensClient, error) {
	uc.streamMu.Lock()
	defer uc.streamMu.Unlock()

	if s, ok := uc.streamMap[requestID]; ok {
		return s, nil
	}

	addr, err := uc.resolveAddr(ctx, requestID)
	if err != nil {
		return nil, fmt.Errorf("resolve addr: %w", err)
	}

	conn, err := uc.getConn(addr)
	if err != nil {
		return nil, fmt.Errorf("grpc conn: %w", err)
	}

	client := pb.NewTokenServiceClient(conn)
	stream, err := client.DeliverTokens(ctx)
	if err != nil {
		return nil, fmt.Errorf("grpc stream: %w", err)
	}

	uc.streamMap[requestID] = stream
	return stream, nil
}

func (uc *StreamTokensUseCase) resolveAddr(ctx context.Context, requestID string) (string, error) {
	uc.addrMu.RLock()
	addr, ok := uc.addrMap[requestID]
	uc.addrMu.RUnlock()
	if ok {
		return addr, nil
	}

	addr, err := uc.callbackStore.GetCallback(ctx, requestID)
	if err != nil {
		return "", err
	}

	uc.addrMu.Lock()
	uc.addrMap[requestID] = addr
	uc.addrMu.Unlock()
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

func (uc *StreamTokensUseCase) cleanup(requestID string) {
	uc.addrMu.Lock()
	delete(uc.addrMap, requestID)
	uc.addrMu.Unlock()

	uc.streamMu.Lock()
	delete(uc.streamMap, requestID)
	uc.streamMu.Unlock()
}

type streamCreds struct{ secret string }

func (c streamCreds) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + c.secret}, nil
}
func (c streamCreds) RequireTransportSecurity() bool { return false }
