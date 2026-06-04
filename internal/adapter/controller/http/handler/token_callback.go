package handler

import (
	"net/http"

	"golang-learning/internal/usecase"

	"github.com/gin-gonic/gin"
)

type TokenCallbackHandler struct {
	hub usecase.ITokenHub
}

func NewTokenCallbackHandler(hub usecase.ITokenHub) *TokenCallbackHandler {
	return &TokenCallbackHandler{hub: hub}
}

func (h *TokenCallbackHandler) RegisterRoutes(r *gin.Engine) {
	r.POST("/internal/tokens/:request_id", h.Handle)
}

func (h *TokenCallbackHandler) Handle(c *gin.Context) {
	requestID := c.Param("request_id")

	var token usecase.PubSubToken
	if err := c.ShouldBindJSON(&token); err != nil {
		c.Status(http.StatusBadRequest)
		return
	}

	h.hub.Deliver(requestID, token)
	c.Status(http.StatusOK)
}
