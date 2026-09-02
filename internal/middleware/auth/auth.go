// Package auth 鉴权
package auth

import (
	"context"
	"feedsystem/internal/data"
	"feedsystem/internal/http/response"
	"feedsystem/internal/pkg/jwt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type AuthSecret struct {
	key []byte // 对称加密jwt token
}

func NewAuthSecret(key []byte) *AuthSecret {
	return &AuthSecret{
		key: key,
	}
}

// JWTAuthMiddleWare 对access token 鉴权
func (a *AuthSecret) JWTAuthMiddleWare(cache *data.RedisClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 获取access-token
		tokenstr := extractBearer(c.GetHeader("Authorization"))
		if tokenstr == "" {
			response.Fail(c, http.StatusUnauthorized, "missing authorization header")
			c.Abort()
			return
		}

		// 验证accessn-token是否有效
		claim, err := jwt.ParseToken(a.key, tokenstr)
		if err != nil {
			response.Fail(c, http.StatusUnauthorized, "invalid or expired token")
			c.Abort()
			return
		}

		// jwt撤销检测。有效会话的 account:%d 必须存在且与当前token一致；
		// 键缺失（登出/被踢）或值不一致（轮换/换设备覆盖）一律视为已失效。
		if cache != nil {
			ctx, cancel := context.WithTimeout(c.Request.Context(), 100*time.Millisecond)
			defer cancel()

			oldtoken, err := cache.Get(ctx, cache.Key("account:%d", claim.AccountID))
			if err != nil || oldtoken != tokenstr {
				response.Fail(c, http.StatusUnauthorized, "token has been revoked")
				c.Abort()
				return
			}
		}

		// 2.如果有效，允许通过
		// 设置用户信息，方便后续的接口调用
		c.Set("user_id", claim.AccountID)
		c.Set("username", claim.Username)
		c.Next()
	}
}

func extractBearer(header string) string {
	const prefix = "Bearer "
	if len(header) > len(prefix) && header[:len(prefix)] == prefix {
		return header[len(prefix):]
	}
	return ""
}
