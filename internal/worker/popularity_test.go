package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"feedsystem/internal/messaging"
	"feedsystem/internal/model/video"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type MockCacheSetter struct {
	mock.Mock
}

func (m *MockCacheSetter) UpdatePopularity(ctx context.Context, videoID uint, change float64) error {
	args := m.Called(ctx, videoID, change)
	return args.Error(0)
}

func (m *MockCacheSetter) SetNX(ctx context.Context, key string, val any, ttl time.Duration) (bool, error) {
	args := m.Called(ctx, key, val, ttl)
	return args.Bool(0), args.Error(1)
}

func (m *MockCacheSetter) Del(ctx context.Context, key string) error {
	args := m.Called(ctx, key)
	return args.Error(0)
}

var _ cacheSetter = (*MockCacheSetter)(nil)

const testPopularityKey = "idem:popularity:evt-popularity"

func testPopularityEvent() video.PopularityEvent {
	return video.PopularityEvent{
		EventID: "evt-popularity",
		VideoID: 7,
		Change:  3,
	}
}

func expectSetNX(cache *MockCacheSetter, ok bool, err error) *mock.Call {
	return cache.
		On("SetNX", mock.Anything, testPopularityKey, "1", 24*time.Hour).
		Return(ok, err)
}

// TestPopularityHandler 测试热度事件能被正确解码并写入缓存
func TestPopularityHandler(t *testing.T) {
	event := testPopularityEvent()

	t.Run("合法事件", func(t *testing.T) {
		cache := new(MockCacheSetter)
		expectSetNX(cache, true, nil).Once()
		cache.On("UpdatePopularity", mock.Anything, uint(7), float64(3)).Return(nil).Once()

		msg := envelopeMessage(t, messaging.EventTypePopularity, event)
		require.NoError(t, PopularityHandler(cache)(context.Background(), msg))

		require.True(t, msg.accepted)
		require.False(t, msg.requeued)
		cache.AssertExpectations(t)
	})

	t.Run("非法 JSON", func(t *testing.T) {
		cache := new(MockCacheSetter)

		msg := rawMessage([]byte("not-json"))
		require.Error(t, PopularityHandler(cache)(context.Background(), msg))

		require.True(t, msg.accepted)
		require.False(t, msg.requeued)
		cache.AssertNotCalled(t, "SetNX")
		cache.AssertNotCalled(t, "UpdatePopularity")
	})

	t.Run("事件类型不匹配", func(t *testing.T) {
		cache := new(MockCacheSetter)

		msg := envelopeMessage(t, messaging.EventTypeRating, event)
		require.Error(t, PopularityHandler(cache)(context.Background(), msg))

		require.True(t, msg.accepted)
		require.False(t, msg.requeued)
		cache.AssertNotCalled(t, "SetNX")
		cache.AssertNotCalled(t, "UpdatePopularity")
	})

	t.Run("重复事件直接确认", func(t *testing.T) {
		cache := new(MockCacheSetter)
		expectSetNX(cache, false, nil).Once()

		msg := envelopeMessage(t, messaging.EventTypePopularity, event)
		require.NoError(t, PopularityHandler(cache)(context.Background(), msg))

		require.True(t, msg.accepted)
		require.False(t, msg.requeued)
		cache.AssertNotCalled(t, "UpdatePopularity")
		cache.AssertNotCalled(t, "Del")
	})

	t.Run("抢占标记失败则重投", func(t *testing.T) {
		cache := new(MockCacheSetter)
		redisErr := errors.New("redis unavailable")
		expectSetNX(cache, false, redisErr).Once()

		msg := envelopeMessage(t, messaging.EventTypePopularity, event)
		require.NoError(t, PopularityHandler(cache)(context.Background(), msg))

		require.True(t, msg.requeued)
		require.False(t, msg.accepted)
		cache.AssertNotCalled(t, "UpdatePopularity")
	})

	t.Run("抢占标记失败时ctx已取消仍然重投", func(t *testing.T) {
		cache := new(MockCacheSetter)
		expectSetNX(cache, false, context.DeadlineExceeded).Once()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		msg := envelopeMessage(t, messaging.EventTypePopularity, event)
		require.NoError(t, PopularityHandler(cache)(ctx, msg))

		require.True(t, msg.requeued)
		require.False(t, msg.accepted)
	})

	t.Run("首次失败后重试成功", func(t *testing.T) {
		cache := new(MockCacheSetter)
		expectSetNX(cache, true, nil).Once()
		cache.On("UpdatePopularity", mock.Anything, uint(7), float64(3)).
			Return(errors.New("boom")).Once()
		cache.On("UpdatePopularity", mock.Anything, uint(7), float64(3)).
			Return(nil).Once()

		msg := envelopeMessage(t, messaging.EventTypePopularity, event)
		require.NoError(t, PopularityHandler(cache)(context.Background(), msg))

		require.True(t, msg.accepted)
		require.False(t, msg.requeued)
		cache.AssertNumberOfCalls(t, "UpdatePopularity", 2)
		cache.AssertNotCalled(t, "Del")
		cache.AssertExpectations(t)
	})

	t.Run("重试耗尽后丢弃并释放标记", func(t *testing.T) {
		cache := new(MockCacheSetter)
		expectSetNX(cache, true, nil).Once()
		cache.On("UpdatePopularity", mock.Anything, uint(7), float64(3)).
			Return(errors.New("boom")).Times(4) // 首次 + 3 次重试
		cache.On("Del", mock.Anything, testPopularityKey).Return(nil).Once()

		msg := envelopeMessage(t, messaging.EventTypePopularity, event)
		require.NoError(t, PopularityHandler(cache)(context.Background(), msg))

		require.True(t, msg.accepted)
		require.False(t, msg.requeued)
		cache.AssertNumberOfCalls(t, "UpdatePopularity", 4)
		cache.AssertExpectations(t)
	})

	t.Run("重试耗尽且释放标记失败仍然确认", func(t *testing.T) {
		cache := new(MockCacheSetter)
		expectSetNX(cache, true, nil).Once()
		cache.On("UpdatePopularity", mock.Anything, uint(7), float64(3)).
			Return(errors.New("boom")).Times(4)
		cache.On("Del", mock.Anything, testPopularityKey).
			Return(errors.New("redis down")).Once()

		msg := envelopeMessage(t, messaging.EventTypePopularity, event)
		require.NoError(t, PopularityHandler(cache)(context.Background(), msg))

		require.True(t, msg.accepted)
		require.False(t, msg.requeued)
		cache.AssertExpectations(t)
	})
}

// TestRequeuePopularityCanceledContextSkipsBackoff 验证 ctx 已取消时不再等待退避。
func TestRequeuePopularityCanceledContextSkipsBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	msg := &fakeMessage{}
	start := time.Now()
	requeuePopularity(ctx, msg)

	require.True(t, msg.requeued)
	require.Less(t, time.Since(start), 300*time.Millisecond, "ctx 已取消时不应再等待退避")
}
