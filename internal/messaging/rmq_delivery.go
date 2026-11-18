package messaging

import (
	"context"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

// rmqDelivery 适配 AMQP delivery
type rmqDelivery struct {
	dc rmq.IDeliveryContext
}

func (d rmqDelivery) Body() []byte { return d.dc.Message().GetData() }
func (d rmqDelivery) Accept(ctx context.Context) error {
	// 处理可能已经超时，但是"确认"这一步必须仍然成功
	settleCtx, cancel := settleContext(ctx)
	defer cancel()
	return d.dc.Accept(settleCtx)
}

// 编译期接口断言
var _ Message = rmqDelivery{}
