package messaging

import (
	"context"
	"fmt"

	"github.com/Azure/go-amqp"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

// Publisher 是发布能力端口，业务只依赖它，不感知具体 MQ 实现。
type Publisher interface {
	Publish(ctx context.Context, env *Envelope) error
}

// rmqPublisher 是 RabbitMQ 的发布适配器
type rmqPublisher struct {
	pub *rmq.Publisher
}

// Publish 序列化为统一的信封格式
func (p *rmqPublisher) Publish(ctx context.Context, env *Envelope) error {
	if p == nil || p.pub == nil {
		return ErrNilPublisher
	}

	b, err := env.Marshal()
	if err != nil {
		return fmt.Errorf("messaging: marshal envelope: %w", err)
	}

	if _, err := p.pub.Publish(ctx, amqp.NewMessage(b)); err != nil {
		return fmt.Errorf("messaging: publish message: %w", err)
	}
	return nil
}

var _ Publisher = (*rmqPublisher)(nil)
