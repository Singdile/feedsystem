package messaging

import "context"

// messageSource 定义信息来源: 从队列中拉取消息，直到被关闭
type messageSource interface {
	Receive(ctx context.Context) (Message, error)
	Close(ctx context.Context) error
}
