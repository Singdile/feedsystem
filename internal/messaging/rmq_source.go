package messaging

import (
	"context"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

type rmqConsumerSource struct {
	c *rmq.Consumer
}

func (s rmqConsumerSource) Receive(ctx context.Context) (Message, error) {
	dc, err := s.c.Receive(ctx)
	if err != nil {
		return nil, err
	}

	return rmqDelivery{dc: dc}, nil
}

func (s rmqConsumerSource) Close(ctx context.Context) error {
	return s.c.Close(ctx)
}

// 实现接口rmqConsumerSource
var _ messageSource = rmqConsumerSource{}
