package messaging

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPublisher_Nil 覆盖 MQ 不可用时 publisher 为 nil 的降级语义：
// 调用方不判空也不会 panic，而是拿到 ErrNilPublisher。
func TestPublisher_Nil(t *testing.T) {
	var nilPublisher *Publisher
	require.ErrorIs(t, nilPublisher.PublishJSON(context.Background(), struct{}{}), ErrNilPublisher)

	empty := &Publisher{}
	require.ErrorIs(t, empty.PublishJSON(context.Background(), struct{}{}), ErrNilPublisher)
}
