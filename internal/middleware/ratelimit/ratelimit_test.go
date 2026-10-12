package ratelimit

import (
	"context"
	"feedsystem/internal/config"
	"feedsystem/internal/data"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func newTestCache(t *testing.T) *data.RedisClient {
	t.Helper()
	mr := miniredis.RunT(t)
	host, portStr, _ := net.SplitHostPort(mr.Addr())
	port, _ := strconv.Atoi(portStr)
	cache, err := data.NewRedis(config.RedisConfig{Host: host, Port: port})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() })
	return cache
}

// 测试限流的基本功能
func TestLimit(t *testing.T) {
	// 启动一个测试后端
	gin.SetMode(gin.TestMode)
	cache := newTestCache(t)
	_ = context.Background()

	t.Run("未超限放行", func(t *testing.T) {
		cnt := 0
		r := gin.New()
		r.GET("/t",
			Limit(cache, "scope", 3, time.Minute, func(c *gin.Context) (string, bool) { return "u1", true }),
			func(c *gin.Context) { cnt++; c.Status(200) })

		for range 3 {
			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest("GET", "/t", nil))
			require.Equal(t, 200, w.Code)
		}
		require.Equal(t, 3, cnt) // handler 跑了 3 次
	})

	t.Run("超限返回429且handler不执行", func(t *testing.T) {
		cnt := 0
		r := gin.New()
		r.GET("/t",
			Limit(cache, "scope2", 2, time.Minute, func(c *gin.Context) (string, bool) { return "u1", true }),
			func(c *gin.Context) { cnt++; c.Status(200) })

		for range 2 {
			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest("GET", "/t", nil))
			require.Equal(t, 200, w.Code)
		}
		// 第 3 次超限 → 429 + handler 不执行
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("GET", "/t", nil))
		require.Equal(t, http.StatusTooManyRequests, w.Code)
		require.Equal(t, 2, cnt) // handler 只跑了 2 次，第 3 次被限流跳过
	})
	t.Run("cache为空放行", func(t *testing.T) {
		cnt := 0
		r := gin.New()
		r.GET("/t",
			Limit(nil, "scope", 3, time.Minute, func(c *gin.Context) (string, bool) { return "u1", true }),
			func(c *gin.Context) { cnt++; c.Status(200) })
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("GET", "/t", nil))
		require.Equal(t, 200, w.Code)
		require.Equal(t, 1, cnt)
	})

	t.Run("keyfunc失败放行", func(t *testing.T) {
		cnt := 0
		r := gin.New()
		r.GET("/t",
			Limit(cache, "scope", 3, time.Minute, func(c *gin.Context) (string, bool) { return "", false }),
			func(c *gin.Context) { cnt++; c.Status(200) })
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("GET", "/t", nil))
		require.Equal(t, 200, w.Code)
		require.Equal(t, 1, cnt)
	})

}
