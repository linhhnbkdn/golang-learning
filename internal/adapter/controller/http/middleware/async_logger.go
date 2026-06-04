package middleware

import (
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type logEntry struct {
	method   string
	path     string
	status   int
	latency  time.Duration
	clientIP string
}

// AsyncLogger returns a Gin middleware that logs HTTP requests asynchronously.
// Log writes are offloaded to a background goroutine via a buffered channel so
// the request handler is never blocked by I/O. Entries are dropped (never block)
// if the channel is full under extreme load.
func AsyncLogger(log *zap.Logger) gin.HandlerFunc {
	ch := make(chan logEntry, 8192)

	go func() {
		for e := range ch {
			log.Info("http",
				zap.String("method", e.method),
				zap.String("path", e.path),
				zap.Int("status", e.status),
				zap.Duration("latency", e.latency),
				zap.String("ip", e.clientIP),
			)
		}
	}()

	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		select {
		case ch <- logEntry{
			method:   c.Request.Method,
			path:     c.FullPath(),
			status:   c.Writer.Status(),
			latency:  time.Since(start),
			clientIP: c.ClientIP(),
		}:
		default: // channel full — drop entry, never block request
		}
	}
}
