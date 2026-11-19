package messaging

import (
	"context"
	"errors"
)

var ErrNilPublisher = errors.New("messaging: publisher is nil")

// Publisher 是发布能力端口，业务只依赖它，不感知具体 MQ 实现。
type Publisher interface {
	Publish(ctx context.Context, env *Envelope) error
}
