package messaging

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestQueueNames 保证端口层暴露的队列名非空且不重复。
// 队列名是业务/装配与 MQ 适配器之间的契约，只能在这里定义一份。
func TestQueueNames(t *testing.T) {
	queues := []string{
		TimelineQueue,
		RatingQueue,
		CommentQueue,
		NotificationQueue,
		PopularityQueue,
	}

	seen := make(map[string]struct{}, len(queues))
	for _, q := range queues {
		require.NotEmpty(t, q, "queue 不能为空")
		_, dup := seen[q]
		require.False(t, dup, "queue 重复: %s", q)
		seen[q] = struct{}{}
	}
}
