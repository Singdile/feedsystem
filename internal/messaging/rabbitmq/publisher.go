package rabbitmq

import (
	"context"
	"feedsystem/internal/messaging"
	"fmt"

	"github.com/Azure/go-amqp"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

// rmqPublisher 是 RabbitMQ 的发布适配器
type rmqPublisher struct {
	pub *rmq.Publisher
}

// Publish 序列化为统一的信封格式
func (p *rmqPublisher) Publish(ctx context.Context, env *messaging.Envelope) error {
	if p == nil || p.pub == nil {
		return messaging.ErrNilPublisher
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

var _ messaging.Publisher = (*rmqPublisher)(nil)
