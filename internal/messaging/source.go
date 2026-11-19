package messaging

import (
	"context"
	"errors"
)

var (
	// ErrConnectionNotReady 连接正在重连，暂时不能创建 consumer
	ErrConnectionNotReady = errors.New("messaging: connection not ready")
	// ErrConnectionClosed 连接已永久关闭（库放弃恢复）
	ErrConnectionClosed = errors.New("messaging: connection closed")
)

// messageSource 定义信息来源: 从队列中拉取消息，直到被关闭
type MessageSource interface {
	Receive(ctx context.Context) (Message, error)
	Close(ctx context.Context) error
}

// SourceFactory 负责按照队列打开信息链路来源
type SourceFactory interface {
	OpenSource(ctx context.Context, queue string, prefetch int32) (MessageSource, error)
}
