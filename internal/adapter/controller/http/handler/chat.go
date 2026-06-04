package handler

import (
	"errors"
	"net/http"

	"golang-learning/internal/adapter/controller/http/middleware"
	httppresenter "golang-learning/internal/adapter/presenter/http"
	"golang-learning/internal/usecase"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type ChatHandler struct {
	getHistory *usecase.GetHistoryUseCase
	store      usecase.IMessageStore
	ownerStore usecase.ISessionOwnerStore
	log        *zap.Logger
}

func NewChatHandler(
	getHistory *usecase.GetHistoryUseCase,
	store usecase.IMessageStore,
	ownerStore usecase.ISessionOwnerStore,
	log *zap.Logger,
) *ChatHandler {
	return &ChatHandler{
		getHistory: getHistory,
		store:      store,
		ownerStore: ownerStore,
		log:        log,
	}
}

func (h *ChatHandler) RegisterRoutes(r *gin.Engine, authMiddleware gin.HandlerFunc) {
	auth := r.Group("/", authMiddleware)
	auth.GET("/history/:session_id", h.GetHistory)
	auth.GET("/history/:session_id/db", h.GetHistoryDB)
}

func (h *ChatHandler) GetHistory(c *gin.Context) {
	sessionID := c.Param("session_id")
	if h.guardSession(c, sessionID) {
		return
	}
	p := &httppresenter.GetHistoryPresenter{}
	h.getHistory.Execute(c.Request.Context(), sessionID, p)
	if p.Err != nil {
		h.log.Error("get history failed", zap.Error(p.Err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": p.Err.Error()})
		return
	}
	c.JSON(http.StatusOK, p.Messages)
}

func (h *ChatHandler) GetHistoryDB(c *gin.Context) {
	sessionID := c.Param("session_id")
	if h.guardSession(c, sessionID) {
		return
	}
	messages, err := h.store.GetHistory(c.Request.Context(), sessionID)
	if err != nil {
		h.log.Error("get history db failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	p := &httppresenter.GetHistoryPresenter{}
	p.PresentMessages(messages)
	c.JSON(http.StatusOK, p.Messages)
}

func (h *ChatHandler) guardSession(c *gin.Context, sessionID string) bool {
	userID := c.GetString(middleware.UserIDKey)
	owner, err := h.ownerStore.GetOwner(c.Request.Context(), sessionID)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			c.JSON(http.StatusForbidden, gin.H{"error": "forbidden"})
			return true
		}
		h.log.Error("get session owner failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return true
	}
	if owner != userID {
		c.JSON(http.StatusForbidden, gin.H{"error": "forbidden"})
		return true
	}
	return false
}
