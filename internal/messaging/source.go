package messaging

import "context"

// messageSource 定义信息来源: 从队列中拉取消息，直到被关闭
type MessageSource interface {
	Receive(ctx context.Context) (Message, error)
	Close(ctx context.Context) error
}

// SourceFactory 负责按照队列打开信息链路来源
type SourceFactory interface {
	OpenSource(ctx context.Context, queue string, prefetch int32) (MessageSource, error)
}
