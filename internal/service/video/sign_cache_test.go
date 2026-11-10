package video

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// signCacheTestSigner 记录下游签名次数，可模拟延迟与失败。
type signCacheTestSigner struct {
	calls atomic.Int64
	delay time.Duration
	fail  atomic.Bool
}

func (s *signCacheTestSigner) PresignedGetObject(
	_ context.Context, objectKey string, _ time.Duration,
) (string, error) {
	s.calls.Add(1)
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	if s.fail.Load() {
		return "", errors.New("sign failed")
	}
	return "https://media.example/" + objectKey, nil
}

func newSignCacheTestSigner(
	t *testing.T, opts ...URLCacheOption,
) (*CachingURLSigner, *signCacheTestSigner) {
	t.Helper()
	next := &signCacheTestSigner{}
	return NewCachingURLSigner(next, opts...), next
}

// 同一个 key 第二次请求应直接命中缓存，不再调用下游签名。
func TestCachingURLSigner_Hit(t *testing.T) {
	s, next := newSignCacheTestSigner(t)

	u1, err := s.PresignedGetObject(t.Context(), "videos/a.mp4", time.Hour)
	require.NoError(t, err)
	u2, err := s.PresignedGetObject(t.Context(), "videos/a.mp4", time.Hour)
	require.NoError(t, err)

	require.Equal(t, u1, u2)
	require.EqualValues(t, 1, next.calls.Load())

	hits, misses := s.Stats()
	require.EqualValues(t, 1, hits)
	require.EqualValues(t, 1, misses)
}

// 不同 objectKey 必须各自签名，不能互相串。
func TestCachingURLSigner_KeyIsolation(t *testing.T) {
	s, next := newSignCacheTestSigner(t)

	u1, err := s.PresignedGetObject(t.Context(), "videos/a.mp4", time.Hour)
	require.NoError(t, err)
	u2, err := s.PresignedGetObject(t.Context(), "covers/a.jpg", time.Hour)
	require.NoError(t, err)

	require.NotEqual(t, u1, u2)
	require.EqualValues(t, 2, next.calls.Load())
}

// 不同 expiry 必须分开缓存（1h 的 URL 不能返回给需要 2h 的调用方）。
func TestCachingURLSigner_ExpiryIsolation(t *testing.T) {
	s, next := newSignCacheTestSigner(t)

	_, err := s.PresignedGetObject(t.Context(), "videos/a.mp4", time.Hour)
	require.NoError(t, err)
	_, err = s.PresignedGetObject(t.Context(), "videos/a.mp4", 2*time.Hour)
	require.NoError(t, err)

	require.EqualValues(t, 2, next.calls.Load())
}

// 缓存 TTL 必须是 expiry - margin，而不是 expiry。
// expiry=1s、margin=900ms → 实际只缓存 100ms，200ms 后应重新签名。
func TestCachingURLSigner_TTLExpiry(t *testing.T) {
	s, next := newSignCacheTestSigner(t, WithURLRefreshMargin(900*time.Millisecond))

	_, err := s.PresignedGetObject(t.Context(), "videos/a.mp4", time.Second)
	require.NoError(t, err)
	_, err = s.PresignedGetObject(t.Context(), "videos/a.mp4", time.Second)
	require.NoError(t, err)
	require.EqualValues(t, 1, next.calls.Load(), "100ms 内应命中缓存")

	time.Sleep(200 * time.Millisecond)

	_, err = s.PresignedGetObject(t.Context(), "videos/a.mp4", time.Second)
	require.NoError(t, err)
	require.EqualValues(t, 2, next.calls.Load(), "超过 expiry-margin 后应重新签名")
}

// 签名失败不能写入缓存，下一次请求应重试。
func TestCachingURLSigner_ErrorNotCached(t *testing.T) {
	s, next := newSignCacheTestSigner(t)

	next.fail.Store(true)
	_, err := s.PresignedGetObject(t.Context(), "videos/a.mp4", time.Hour)
	require.Error(t, err)

	next.fail.Store(false)
	url, err := s.PresignedGetObject(t.Context(), "videos/a.mp4", time.Hour)
	require.NoError(t, err)
	require.NotEmpty(t, url)

	require.EqualValues(t, 2, next.calls.Load())
}

// 并发请求同一个 key 时，singleflight 保证下游只签名一次。
func TestCachingURLSigner_Singleflight(t *testing.T) {
	s, next := newSignCacheTestSigner(t)
	next.delay = 20 * time.Millisecond

	const n = 20
	start := make(chan struct{})
	errCh := make(chan error, n)

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			<-start
			_, err := s.PresignedGetObject(context.Background(), "videos/hot.mp4", time.Hour)
			errCh <- err
		}()
	}

	close(start)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}
	require.EqualValues(t, 1, next.calls.Load())
}

// 容量满时淘汰最久未使用的条目。
func TestCachingURLSigner_CapacityEviction(t *testing.T) {
	s, next := newSignCacheTestSigner(t, WithURLCacheSize(2))

	for _, key := range []string{"a", "b", "c"} {
		_, err := s.PresignedGetObject(t.Context(), key, time.Hour)
		require.NoError(t, err)
	}
	require.EqualValues(t, 3, next.calls.Load())
	require.Equal(t, 2, s.cache.Len())

	// a 已被淘汰，应重新签名
	_, err := s.PresignedGetObject(t.Context(), "a", time.Hour)
	require.NoError(t, err)
	require.EqualValues(t, 4, next.calls.Load())
}

// 过短的 expiry 不做缓存，每次都透传。
func TestCachingURLSigner_ShortExpiryNotCached(t *testing.T) {
	s, next := newSignCacheTestSigner(t)

	_, err := s.PresignedGetObject(t.Context(), "a", 500*time.Millisecond)
	require.NoError(t, err)
	_, err = s.PresignedGetObject(t.Context(), "a", 500*time.Millisecond)
	require.NoError(t, err)

	require.EqualValues(t, 2, next.calls.Load())
}

// 空 objectKey 不做缓存，直接透传。
func TestCachingURLSigner_EmptyKeyNotCached(t *testing.T) {
	s, next := newSignCacheTestSigner(t)

	_, err := s.PresignedGetObject(t.Context(), "", time.Hour)
	require.NoError(t, err)
	_, err = s.PresignedGetObject(t.Context(), "", time.Hour)
	require.NoError(t, err)

	require.EqualValues(t, 2, next.calls.Load())
}

// 选项应正确生效；负数 margin 应被忽略。
func TestCachingURLSigner_Options(t *testing.T) {
	s, _ := newSignCacheTestSigner(t,
		WithURLCacheSize(3),
		WithURLRefreshMargin(time.Minute),
	)
	require.Equal(t, 3, s.cache.cap)
	require.Equal(t, time.Minute, s.margin)

	negative, _ := newSignCacheTestSigner(t, WithURLRefreshMargin(-time.Second))
	require.Equal(t, defaultRefreshMargin, negative.margin)
}
