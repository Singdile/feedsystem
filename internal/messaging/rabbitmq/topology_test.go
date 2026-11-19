package rabbitmq

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeclareAll_NilClient(t *testing.T) {
	require.ErrorIs(t, DeclareAll(context.Background(), nil), ErrNilClient)
}

// TestTopologyConstants 保证 exchange / binding key / routing key 非空且不重复。
// 队列名属于端口包，由 messaging 的测试负责。
func TestTopologyConstants(t *testing.T) {
	t.Run("exchange 非空且唯一", func(t *testing.T) {
		exchanges := []string{
			TimelineExchange,
			RatingExchange,
			CommentExchange,
			SocialExchange,
			PopularityExchange,
		}
		requireUniqueNonEmpty(t, "exchange", exchanges)
	})

	t.Run("binding key 非空且唯一", func(t *testing.T) {
		keys := []string{
			TimelineBindingKey,
			RatingBindingKey,
			CommentBindingKey,
			SocialBindingKey,
			PopularityBindingKey,
		}
		requireUniqueNonEmpty(t, "binding key", keys)
	})

	t.Run("publish routing key 非空且唯一", func(t *testing.T) {
		keys := []string{
			TimelinePublishRK,
			RatingPublishRK,
			CommentPublishRK,
			SocialPublishRK,
			PopularityPublishRK,
		}
		requireUniqueNonEmpty(t, "publish routing key", keys)
	})
}

func requireUniqueNonEmpty(t *testing.T, kind string, values []string) {
	t.Helper()

	seen := make(map[string]struct{}, len(values))
	for _, v := range values {
		require.NotEmpty(t, v, "%s 不能为空", kind)
		_, dup := seen[v]
		require.False(t, dup, "%s 重复: %s", kind, v)
		seen[v] = struct{}{}
	}
}
