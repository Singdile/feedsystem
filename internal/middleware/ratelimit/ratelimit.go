package ratelimit

import (
	"feedsystem/internal/data"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type KeyFunc func(*gin.Context) (string, bool)

// Limit 通用的限流中间件函数
func Limit(cache *data.RedisClient, keyPrefix string, maxRequests int64, window time.Duration, keyfunc KeyFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		if cache == nil {
			c.Next()
			return
		}

		// 构造要查询的key，检查操作次数
		// key :: feedsystem:ratelimit:<scope>:<subject>
		subject, ok := keyfunc(c)
		if !ok {
			c.Next()
			return
		}

		key := cache.Key("feedsystem:ratelimit:%s:%s", keyPrefix, subject)
		cnt, err := cache.IncrementWithExpire(c.Request.Context(), key, window)
		if err != nil { //cache 错误,放行
			c.Next()
			return
		}

		if cnt > maxRequests {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "too many requests"})
			return
		}

		c.Next()
	}
}

func KeyByIP(c *gin.Context) (string, bool) {
	ip := strings.TrimSpace(c.ClientIP())
	if ip == "" {
		return "", false
	}

	return ip, true
}

func KeyByAccount(c *gin.Context) (string, bool) {
	v, exist := c.Get("user_id")
	if !exist {
		return "", false
	}
	id, ok := v.(uint)
	if !ok || id == 0 {
		return "", false
	}
	return strconv.FormatUint(uint64(id), 10), true
}
