package messaging

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestSettleContext 验证"处理超时后，确认动作仍能成功"所依赖的 ctx 策略：
// 父 ctx 已取消时，派生出的 settle ctx 仍然可用，并且有自己的超时上限。
func TestSettleContext(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	cancelParent() // 模拟 handler 已经超时/被取消

	settleCtx, cancelSettle := settleContext(parent)
	defer cancelSettle()

	require.NoError(t, settleCtx.Err(), "settle ctx 不应继承父 ctx 的取消")
	deadline, ok := settleCtx.Deadline()
	require.True(t, ok, "settle ctx 应有自己的超时上限")
	require.WithinDuration(t, time.Now().Add(settleTimeout), deadline, time.Second)
}
