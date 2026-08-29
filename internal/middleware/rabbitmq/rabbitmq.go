// Package rabbitmq 连接rabbitmq
package rabbitmq

import (
	"context"
	"feedsystem/internal/config"
	"fmt"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

type RabbitMQClient struct {
    env *rmq.Environment
    conn *rmq.AmqpConnection
}

func NewRabbitMQ(config config.RabbitMQConfig) (*RabbitMQClient,error) {
    brokerURI := fmt.Sprintf("amqp://%s:%s@%s:%d/",config.Username,config.Password,config.Host,config.Port)
    env := rmq.NewEnvironment(brokerURI,nil)
    ctx,cancel := context.WithTimeout(context.Background(),2*time.Second)
    defer cancel()
    conn,err := env.NewConnection(ctx) //马上建立连接
    if err != nil {
	return nil,err
    }

    return &RabbitMQClient{
	env: env,
	conn: conn,
    },nil
}

func (c *RabbitMQClient)NewProducer(queuename string) (*rmq.Publisher,error) {
    ctx,cancel := context.WithTimeout(context.Background(),2*time.Second)
    defer cancel()
    return c.conn.NewPublisher(ctx,&rmq.QueueAddress{Queue:queuename},nil)
}
