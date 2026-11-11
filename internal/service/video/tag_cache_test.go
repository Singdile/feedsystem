package video

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// tagNameReaderStub 是 TagNameReader 的测试替身，记录整表加载次数。
type tagNameReaderStub struct {
	mu    sync.Mutex
	names map[uint]string
	err   error
	delay time.Duration
	calls int
}

func (s *tagNameReaderStub) ListTagNames(context.Context) (map[uint]string, error) {
	s.mu.Lock()
	s.calls++
	names, err, delay := s.names, s.err, s.delay
	s.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}
	if err != nil {
		return nil, err
	}
	if names == nil {
		return map[uint]string{}, nil
	}

	out := make(map[uint]string, len(names))
	for id, name := range names {
		out[id] = name
	}
	return out, nil
}

func (s *tagNameReaderStub) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *tagNameReaderStub) setNames(names map[uint]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.names = names
}

func TestTagNameCacheLoadsOnceAndHits(t *testing.T) {
	reader := &tagNameReaderStub{names: map[uint]string{1: "go", 2: "redis"}}
	cache := NewTagNameCache(reader, time.Minute)

	out, err := cache.GetNames(t.Context(), []uint{1, 2})
	require.NoError(t, err)
	require.Equal(t, map[uint]string{1: "go", 2: "redis"}, out)
	require.Equal(t, 1, reader.callCount())

	out, err = cache.GetNames(t.Context(), []uint{1})
	require.NoError(t, err)
	require.Equal(t, map[uint]string{1: "go"}, out)
	require.Equal(t, 1, reader.callCount(), "未过期时应命中本地快照")
}

func TestTagNameCacheMissingIDsIgnored(t *testing.T) {
	reader := &tagNameReaderStub{names: map[uint]string{1: "go"}}
	cache := NewTagNameCache(reader, time.Minute)

	out, err := cache.GetNames(t.Context(), []uint{1, 2})
	require.NoError(t, err)
	require.Equal(t, map[uint]string{1: "go"}, out)
	require.Equal(t, 1, reader.callCount(), "TTL 策略下缺失 ID 不应触发按 ID 回源")
}

func TestTagNameCacheEmptyTableDoesNotReload(t *testing.T) {
	reader := &tagNameReaderStub{names: map[uint]string{}}
	cache := NewTagNameCache(reader, time.Minute)

	for range 3 {
		out, err := cache.GetNames(t.Context(), []uint{1})
		require.NoError(t, err)
		require.Empty(t, out)
	}
	require.Equal(t, 1, reader.callCount(), "空表也应设置 expireAt，不能每请求重复加载")
}

func TestTagNameCacheRefreshesAfterTTL(t *testing.T) {
	reader := &tagNameReaderStub{names: map[uint]string{1: "go"}}
	cache := NewTagNameCache(reader, 50*time.Millisecond)

	out, err := cache.GetNames(t.Context(), []uint{1})
	require.NoError(t, err)
	require.Equal(t, "go", out[1])

	reader.setNames(map[uint]string{1: "golang"})
	time.Sleep(120 * time.Millisecond)

	out, err = cache.GetNames(t.Context(), []uint{1})
	require.NoError(t, err)
	require.Equal(t, "golang", out[1])
	require.Equal(t, 2, reader.callCount())
}

func TestTagNameCacheLoadErrorRetries(t *testing.T) {
	reader := &tagNameReaderStub{err: errors.New("db down")}
	cache := NewTagNameCache(reader, time.Minute)

	_, err := cache.GetNames(t.Context(), []uint{1})
	require.Error(t, err)

	_, err = cache.GetNames(t.Context(), []uint{1})
	require.Error(t, err)
	require.Equal(t, 2, reader.callCount(), "加载失败不应写入 expireAt，下次应重试")
}

func TestTagNameCacheEmptyIDs(t *testing.T) {
	reader := &tagNameReaderStub{names: map[uint]string{1: "go"}}
	cache := NewTagNameCache(reader, time.Minute)

	out, err := cache.GetNames(t.Context(), nil)
	require.NoError(t, err)
	require.NotNil(t, out)
	require.Empty(t, out)
	require.Equal(t, 0, reader.callCount())
}

func TestTagNameCacheConcurrentLoads(t *testing.T) {
	reader := &tagNameReaderStub{
		names: map[uint]string{1: "go"},
		delay: 20 * time.Millisecond,
	}
	cache := NewTagNameCache(reader, time.Minute)

	const requests = 20
	start := make(chan struct{})
	errCh := make(chan error, requests)

	var wg sync.WaitGroup
	wg.Add(requests)
	for range requests {
		go func() {
			defer wg.Done()
			<-start
			out, err := cache.GetNames(context.Background(), []uint{1})
			if err == nil && out[1] != "go" {
				err = errors.New("unexpected tag name")
			}
			errCh <- err
		}()
	}

	close(start)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}
	require.Equal(t, 1, reader.callCount(), "singleflight 应合并并发加载")
}
