package messaging

import (
	"context"
	"time"
)

// Message 定义业务层面的消息接口方法,是handler方法可见的消息
type Message interface {
	Body() []byte
	Accept(ctx context.Context) error
	Requeue(ctx context.Context) error
}

type Handler func(ctx context.Context, msg Message) error

const settleTimeout = 3 * time.Second

// settleContext 去掉父 ctx 的取消信号，只保留超时上限。
// 用于"处理已超时，但确认必须成功"的场景。
func SettleContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
}
