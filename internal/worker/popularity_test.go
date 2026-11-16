package worker

import (
	"context"
	"testing"

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

// TestPopularityHandler 测试热度事件能被正确解码并写入缓存
func TestPopularityHandler(t *testing.T) {
	event := video.PopularityEvent{
		EventID: "evt-popularity",
		VideoID: 7,
		Change:  3,
	}

	t.Run("合法事件", func(t *testing.T) {
		cache := new(MockCacheSetter)
		cache.On("UpdatePopularity", mock.Anything, uint(7), float64(3)).Return(nil)

		msg := envelopeMessage(t, messaging.EventTypePopularity, event)
		require.NoError(t, PopularityHandler(cache)(context.Background(), msg))
		require.True(t, msg.accepted)
		cache.AssertExpectations(t)
	})

	t.Run("非法 JSON", func(t *testing.T) {
		cache := new(MockCacheSetter)

		msg := rawMessage([]byte("not-json"))
		require.Error(t, PopularityHandler(cache)(context.Background(), msg))
		require.True(t, msg.accepted)
		cache.AssertNotCalled(t, "UpdatePopularity")
	})

	t.Run("事件类型不匹配", func(t *testing.T) {
		cache := new(MockCacheSetter)

		msg := envelopeMessage(t, messaging.EventTypeRating, event)
		require.Error(t, PopularityHandler(cache)(context.Background(), msg))
		require.True(t, msg.accepted)
		cache.AssertNotCalled(t, "UpdatePopularity")
	})
}
