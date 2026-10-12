package data

import (
	"feedsystem/internal/config"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"
)

func newRedisTest(t *testing.T) (*RedisClient, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	host, portStr, _ := net.SplitHostPort(mr.Addr())
	port, _ := strconv.Atoi(portStr)
	rdb, err := NewRedis(config.RedisConfig{Host: host, Port: port})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = rdb.Close()
	})
	return rdb, mr
}
func TestIncrementWithExpire(t *testing.T) {
	cache, mr := newRedisTest(t)

	key := cache.Key("ratelimit:test:1")
	// 测试连续调用两次
	n, err := cache.IncrementWithExpire(t.Context(), key, time.Minute)
	require.NoError(t, err)
	n, err = cache.IncrementWithExpire(t.Context(), key, time.Minute)
	require.NoError(t, err)
	require.Equal(t, int64(2), n)
	// 测试第一次调用是否正确设置过期时间
	key2 := cache.Key("ratelimit:test:2")
	_, _ = cache.IncrementWithExpire(t.Context(), key2, time.Minute)
	ttl := mr.TTL(key2)
	require.True(t, true, ttl > 0 && ttl <= time.Minute)
	// 测试过期之后是否会重置次数
	mr.FastForward(time.Minute + time.Second)
	n, err = cache.IncrementWithExpire(t.Context(), key, time.Minute)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
}
