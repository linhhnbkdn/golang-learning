package handler

import (
	"crypto/subtle"
	"net/http"
	"strings"

	"golang-learning/internal/usecase"

	"github.com/gin-gonic/gin"
)

type TokenCallbackHandler struct {
	hub    usecase.ITokenHub
	secret string
}

func NewTokenCallbackHandler(hub usecase.ITokenHub, secret string) *TokenCallbackHandler {
	return &TokenCallbackHandler{hub: hub, secret: secret}
}

func (h *TokenCallbackHandler) RegisterRoutes(r *gin.Engine) {
	r.POST("/internal/tokens/:request_id", h.Handle)
}

func (h *TokenCallbackHandler) Handle(c *gin.Context) {
	bearer := strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer ")
	if subtle.ConstantTimeCompare([]byte(bearer), []byte(h.secret)) != 1 {
		c.Status(http.StatusUnauthorized)
		return
	}

	requestID := c.Param("request_id")
	var token usecase.PubSubToken
	if err := c.ShouldBindJSON(&token); err != nil {
		c.Status(http.StatusBadRequest)
		return
	}

	h.hub.Deliver(requestID, token)
	c.Status(http.StatusOK)
}
