package wshandler

import (
	"net/http"
	"sync/atomic"
	"time"

	"golang-learning/config"
	"golang-learning/internal/adapter/controller/http/middleware"
	"golang-learning/internal/usecase"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

type ChatWsHandler struct {
	sendMessage *usecase.SendMessageUseCase
	ownerStore  usecase.ISessionOwnerStore
	pubSub      usecase.IPubSubStream
	cfg         config.Config
	log         *zap.Logger
	upgrader    websocket.Upgrader
}

func NewChatWsHandler(
	sendMessage *usecase.SendMessageUseCase,
	ownerStore usecase.ISessionOwnerStore,
	pubSub usecase.IPubSubStream,
	cfg config.Config,
	log *zap.Logger,
) *ChatWsHandler {
	return &ChatWsHandler{
		sendMessage: sendMessage,
		ownerStore:  ownerStore,
		pubSub:      pubSub,
		cfg:         cfg,
		log:         log,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
}

func (h *ChatWsHandler) RegisterRoutes(r *gin.Engine) {
	r.GET("/ws/chat/:session_id", h.Handle)
}

type clientMsg struct {
	Content string `json:"content"`
}

type wsPresenter struct {
	RequestID string
	Err       error
}

func (p *wsPresenter) PresentRequestID(id string) { p.RequestID = id }
func (p *wsPresenter) PresentError(err error)      { p.Err = err }

func (h *ChatWsHandler) Handle(c *gin.Context) {
	sessionID := c.Param("session_id")

	userID, err := middleware.ParseJWT(c.Query("token"), h.cfg.JWTSecret)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
		return
	}

	conn, err := h.upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		h.log.Error("ws upgrade failed", zap.Error(err))
		return
	}
	defer conn.Close()

	ctx := c.Request.Context()

	owned, err := h.ownerStore.ClaimOwner(ctx, sessionID, userID)
	if err != nil || !owned {
		conn.WriteJSON(gin.H{"error": "forbidden"})
		return
	}

	tokenCh, unsubscribe, err := h.pubSub.Subscribe(ctx, sessionID)
	if err != nil {
		h.log.Error("pubsub subscribe failed", zap.Error(err))
		conn.WriteJSON(gin.H{"error": "internal error"})
		return
	}
	defer unsubscribe()

	// writeCh serialises all WS writes through one goroutine (gorilla: one concurrent writer).
	writeCh := make(chan any, 16)
	done := make(chan struct{})

	go func() {
		defer close(done)
		for msg := range writeCh {
			if err := conn.WriteJSON(msg); err != nil {
				return
			}
		}
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	}()

	var streaming int32
	go func() {
		for token := range tokenCh {
			writeCh <- map[string]any{
				"request_id": token.RequestID,
				"delta":      token.Delta,
				"done":       token.Done,
			}
			if token.Done {
				atomic.StoreInt32(&streaming, 0)
				// Unblock conn.ReadJSON in the main loop so it can exit cleanly.
				// The write goroutine will flush done:true then send a proper close frame.
				conn.SetReadDeadline(time.Now())
				return
			}
		}
	}()

	for {
		var msg clientMsg
		if err := conn.ReadJSON(&msg); err != nil {
			break
		}

		if atomic.LoadInt32(&streaming) == 1 {
			writeCh <- gin.H{"error": "stream in progress"}
			continue
		}

		atomic.StoreInt32(&streaming, 1)
		p := &wsPresenter{}
		h.sendMessage.Execute(ctx, sessionID, msg.Content, p)
		if p.Err != nil {
			h.log.Error("send message failed", zap.Error(p.Err))
			writeCh <- gin.H{"error": p.Err.Error()}
			atomic.StoreInt32(&streaming, 0)
		}
	}

	close(writeCh)
	<-done
}
