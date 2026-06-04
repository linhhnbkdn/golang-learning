package usecase

import (
	"context"
	"fmt"
	"strings"

	"golang-learning/internal/entity"
	pb "golang-learning/proto/gen"
	"golang-learning/shared"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ProcessChatRequestUseCase struct {
	generator      ITokenGenerator
	publisher      IEventPublisher
	cache          IConversationCache
	grpcTarget     string
	callbackSecret string
}

func NewProcessChatRequest(
	generator ITokenGenerator,
	publisher IEventPublisher,
	cache IConversationCache,
	grpcTarget string,
	callbackSecret string,
) *ProcessChatRequestUseCase {
	return &ProcessChatRequestUseCase{
		generator:      generator,
		publisher:      publisher,
		cache:          cache,
		grpcTarget:     grpcTarget,
		callbackSecret: callbackSecret,
	}
}

type staticCreds struct{ secret string }

func (c staticCreds) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + c.secret}, nil
}
func (c staticCreds) RequireTransportSecurity() bool { return false }

func (uc *ProcessChatRequestUseCase) Execute(ctx context.Context, req shared.ChatRequest) error {
	fullResponse, err := uc.streamTokens(ctx, req)
	if err != nil {
		return err
	}
	if err := uc.cacheMessages(ctx, req, fullResponse); err != nil {
		return err
	}
	return uc.publisher.PublishCompleted(ctx, shared.ChatCompleted{
		SessionID: req.SessionID,
		RequestID: req.RequestID,
	})
}

func (uc *ProcessChatRequestUseCase) streamTokens(ctx context.Context, req shared.ChatRequest) (string, error) {
	conn, err := grpc.NewClient(uc.grpcTarget,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(staticCreds{secret: uc.callbackSecret}),
	)
	if err != nil {
		return "", fmt.Errorf("grpc dial: %w", err)
	}
	defer conn.Close()

	client := pb.NewTokenServiceClient(conn)
	stream, err := client.DeliverTokens(ctx)
	if err != nil {
		return "", fmt.Errorf("grpc stream: %w", err)
	}

	tokenCh, err := uc.generator.Generate(ctx, req.Content)
	if err != nil {
		return "", err
	}

	var sb strings.Builder
	for token := range tokenCh {
		sb.WriteString(token)
		if err := stream.Send(&pb.TokenMessage{
			RequestId: req.RequestID,
			Delta:     token,
			Done:      false,
		}); err != nil {
			return "", err
		}
	}

	if err := stream.Send(&pb.TokenMessage{
		RequestId: req.RequestID,
		Done:      true,
	}); err != nil {
		return "", err
	}

	if _, err := stream.CloseAndRecv(); err != nil {
		return "", err
	}

	return sb.String(), nil
}

func (uc *ProcessChatRequestUseCase) cacheMessages(ctx context.Context, req shared.ChatRequest, fullResponse string) error {
	if err := uc.cache.SaveMessage(ctx, entity.Message{
		SessionID: req.SessionID,
		RequestID: req.RequestID,
		Role:      entity.RoleUser,
		Content:   req.Content,
	}); err != nil {
		return err
	}
	return uc.cache.SaveMessage(ctx, entity.Message{
		SessionID: req.SessionID,
		RequestID: req.RequestID,
		Role:      entity.RoleAssistant,
		Content:   fullResponse,
	})
}
