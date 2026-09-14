package data

import (
	"context"
	"encoding/json"
	"feedsystem/internal/config"
	"fmt"
	"time"

	"github.com/Azure/go-amqp"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

const (
	TimelineExchange   = "video.timeline.events"
	TimelineQueue      = "video.timeline.update.queue"
	TimelineBindingKey = "video.timeline.*"
	TimelinePublishRK  = "video.timeline.publish"
)

type RabbitMQClient struct {
	env  *rmq.Environment
	conn *rmq.AmqpConnection
}

func NewRabbitMQ(config config.RabbitMQConfig) (*RabbitMQClient, error) {
	brokerURI := fmt.Sprintf("amqp://%s:%s@%s:%d/", config.Username, config.Password, config.Host, config.Port)
	env := rmq.NewEnvironment(brokerURI, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := env.NewConnection(ctx) //马上建立连接
	if err != nil {
		return nil, err
	}

	return &RabbitMQClient{
		env:  env,
		conn: conn,
	}, nil
}

func (c *RabbitMQClient) NewProducer(queuename string) (*rmq.Publisher, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return c.conn.NewPublisher(ctx, &rmq.QueueAddress{Queue: queuename}, nil)
}

func (c *RabbitMQClient) NewConsumer(ctx context.Context, queueName string) (*rmq.Consumer, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return c.conn.NewConsumer(ctx, queueName, nil)
}

// DeclareTimelineTopology 声明时间线相关的交换机和队列
func (c *RabbitMQClient) DeclareTimelineTopology(ctx context.Context) error {
	mgt := c.conn.Management()
	// 声明交换机
	if _, err := mgt.DeclareExchange(ctx, &rmq.TopicExchangeSpecification{
		Name: TimelineExchange,
	}); err != nil {
		return err
	}

	// 声明队列
	if _, err := mgt.DeclareQueue(ctx, &rmq.DefaultQueueSpecification{
		Name: TimelineQueue,
	}); err != nil {
		return err
	}

	// bind
	_, err := mgt.Bind(ctx, &rmq.ExchangeToQueueBindingSpecification{
		SourceExchange:   TimelineExchange,
		DestinationQueue: TimelineQueue,
		BindingKey:       TimelineBindingKey,
	})
	if err != nil {
		return err
	}

	return nil
}

// NewTimelinePublisher 创建发布到时间线 exchange 的 publisher（按路由键 video.timeline.publish）
func (c *RabbitMQClient) NewTimelinePublisher(ctx context.Context) (*rmq.Publisher, error) {
	return c.conn.NewPublisher(ctx, &rmq.ExchangeAddress{
		Exchange: TimelineExchange,
		Key:      TimelinePublishRK,
	}, nil)
}

// PublishJSON 序列化并发布一条 AMQP1.0 消息
func PublishJSON(ctx context.Context, pub *rmq.Publisher, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = pub.Publish(ctx, amqp.NewMessage(b))
	return err
}
