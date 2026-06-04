package handler

import (
	"crypto/subtle"
	"fmt"
	"net/http"
	"strings"

	"golang-learning/internal/usecase"

	"github.com/gin-gonic/gin"
)

type TokenCallbackHandler struct {
	hub    usecase.ITokenHub
	secret string
}

func NewTokenCallbackHandler(hub usecase.ITokenHub, secret string) (*TokenCallbackHandler, error) {
	if secret == "" {
		return nil, fmt.Errorf("CALLBACK_SECRET must not be empty")
	}
	return &TokenCallbackHandler{hub: hub, secret: secret}, nil
}

func (h *TokenCallbackHandler) RegisterRoutes(r *gin.Engine) {
	r.POST("/internal/tokens/:request_id", h.Handle)
}

func (h *TokenCallbackHandler) Handle(c *gin.Context) {
	authz := c.GetHeader("Authorization")
	bearer := ""
	if strings.HasPrefix(authz, "Bearer ") {
		bearer = strings.TrimPrefix(authz, "Bearer ")
	}
	if h.secret == "" || subtle.ConstantTimeCompare([]byte(bearer), []byte(h.secret)) != 1 {
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
