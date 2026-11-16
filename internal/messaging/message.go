package messaging

import (
	"context"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

type Message interface {
	Body() []byte
	Accept(ctx context.Context) error
}

type Handler func(ctx context.Context, msg Message) error

type deliveryMessage struct {
	dc rmq.IDeliveryContext
}

func (m deliveryMessage) Body() []byte                     { return m.dc.Message().GetData() }
func (m deliveryMessage) Accept(ctx context.Context) error { return m.dc.Accept(ctx) }
