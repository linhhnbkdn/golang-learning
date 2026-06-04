package middleware

import (
	"net/http"
	"strings"

	"golang-learning/config"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

const UserIDKey = "user_id"

// ParseJWT validates a raw JWT string and returns the user_id claim.
func ParseJWT(raw, secret string) (string, error) {
	token, err := jwt.Parse(raw, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, jwt.ErrSignatureInvalid
		}
		return []byte(secret), nil
	})
	if err != nil || !token.Valid {
		return "", jwt.ErrSignatureInvalid
	}
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return "", jwt.ErrSignatureInvalid
	}
	userID, ok := claims[UserIDKey].(string)
	if !ok || userID == "" {
		return "", jwt.ErrSignatureInvalid
	}
	return userID, nil
}

// JWT validates Authorization: Bearer <token> and injects user_id into context.
func JWT(cfg config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		header := c.GetHeader("Authorization")
		if !strings.HasPrefix(header, "Bearer ") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing token"})
			return
		}
		userID, err := ParseJWT(strings.TrimPrefix(header, "Bearer "), cfg.JWTSecret)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
			return
		}
		c.Set(UserIDKey, userID)
		c.Next()
	}
}
