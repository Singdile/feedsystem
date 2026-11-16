package messaging

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Azure/go-amqp"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

type Publisher struct {
	pub *rmq.Publisher
}

// PublishJSON 序列化并发布原始 JSON
//
// Deprecated: 1B 之后统一使用 Publish(*Envelope)，新代码不要调用。
func (p *Publisher) PublishJSON(ctx context.Context, v any) error {
	if p == nil || p.pub == nil {
		return ErrNilPublisher
	}

	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("messaging: marshal message: %w", err)
	}

	if _, err := p.pub.Publish(ctx, amqp.NewMessage(b)); err != nil {
		return fmt.Errorf("messaging: publish message: %w", err)
	}
	return nil
}

// Publish 序列化为统一的信封格式
func (p *Publisher) Publish(ctx context.Context, env *Envelope) error {
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
