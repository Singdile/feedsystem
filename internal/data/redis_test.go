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
	require.True(t, ttl > 0 && ttl <= time.Minute, "ttl=%v", ttl)
	// 测试过期之后是否会重置次数
	mr.FastForward(time.Minute + time.Second)
	n, err = cache.IncrementWithExpire(t.Context(), key, time.Minute)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
}

func TestIncrFrom(t *testing.T) {
	t.Run("key 不存在时使用 seed 初始化", func(t *testing.T) {
		cache, _ := newRedisTest(t)
		key := cache.Key("rating:ver:1:1")

		v, err := cache.IncrFrom(t.Context(), key, 5, time.Hour)
		require.NoError(t, err)
		require.Equal(t, int64(6), v)
	})

	t.Run("已有计数时只做 INCR", func(t *testing.T) {
		cache, _ := newRedisTest(t)
		key := cache.Key("rating:ver:1:2")

		_, err := cache.IncrFrom(t.Context(), key, 0, time.Hour)
		require.NoError(t, err)

		v, err := cache.IncrFrom(t.Context(), key, 0, time.Hour)
		require.NoError(t, err)
		require.Equal(t, int64(2), v)
	})

	t.Run("当前值小于 seed 时抬到 seed 再递增", func(t *testing.T) {
		cache, _ := newRedisTest(t)
		key := cache.Key("rating:ver:1:3")

		_, err := cache.IncrFrom(t.Context(), key, 0, time.Hour) // 1
		require.NoError(t, err)

		v, err := cache.IncrFrom(t.Context(), key, 5, time.Hour) // 抬到 5 再 INCR
		require.NoError(t, err)
		require.Equal(t, int64(6), v)
	})

	t.Run("当前值大于 seed 时不降级", func(t *testing.T) {
		cache, _ := newRedisTest(t)
		key := cache.Key("rating:ver:1:4")

		_, err := cache.IncrFrom(t.Context(), key, 10, time.Hour) // 11
		require.NoError(t, err)

		v, err := cache.IncrFrom(t.Context(), key, 5, time.Hour) // 12
		require.NoError(t, err)
		require.Equal(t, int64(12), v)
	})

	t.Run("刷新过期时间", func(t *testing.T) {
		cache, mr := newRedisTest(t)
		key := cache.Key("rating:ver:1:5")

		_, err := cache.IncrFrom(t.Context(), key, 0, time.Hour)
		require.NoError(t, err)

		ttl := mr.TTL(key)
		require.True(t, ttl > 0 && ttl <= time.Hour, "ttl=%v", ttl)
	})
}
