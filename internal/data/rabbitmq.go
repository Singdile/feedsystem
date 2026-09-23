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
	RatingExchange     = "rating.events"
	RatingQueue        = "rating.events"
	RatingBindingKey   = "rating.*"
	RatingPublishRK    = "rating.update"
	CommentExchange    = "comment.events"
	CommentQueue       = "comment.events"
	CommentBindingKey  = "comment.*"
	CommentPublishRK   = "comment.publish" // 唯一 RK，publish/delete 都走它
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

func (c *RabbitMQClient) NewPublisher(ctx context.Context, addr rmq.ExchangeAddress) (*rmq.Publisher, error) {
	return c.conn.NewPublisher(ctx, &addr, nil)
}

func (c *RabbitMQClient) NewConsumer(ctx context.Context, queueName string) (*rmq.Consumer, error) {
	return c.conn.NewConsumer(ctx, queueName, nil)
}

// DeclareTopology 声明交换机和使用bindKey绑定在交换机上的队列
func (c *RabbitMQClient) DeclareTopology(ctx context.Context, topicName, queueName, bindKey string) error {
	mgt := c.conn.Management()
	// 声明交换机
	if _, err := mgt.DeclareExchange(ctx, &rmq.TopicExchangeSpecification{
		Name: topicName,
	}); err != nil {
		return err
	}

	// 声明队列
	if _, err := mgt.DeclareQueue(ctx, &rmq.DefaultQueueSpecification{
		Name: queueName,
	}); err != nil {
		return err
	}

	// bind
	_, err := mgt.Bind(ctx, &rmq.ExchangeToQueueBindingSpecification{
		SourceExchange:   topicName,
		DestinationQueue: queueName,
		BindingKey:       bindKey,
	})
	if err != nil {
		return err
	}

	return nil
}

// DeclareTimelineTopology 声明时间线相关的交换机和队列
func (c *RabbitMQClient) DeclareTimelineTopology(ctx context.Context) error {
	return c.DeclareTopology(ctx, TimelineExchange, TimelineQueue, TimelineBindingKey)
}

// NewTimelinePublisher 创建发布到时间线 exchange 的 publisher（按路由键 video.timeline.publish）
func (c *RabbitMQClient) NewTimelinePublisher(ctx context.Context) (*rmq.Publisher, error) {
	return c.NewPublisher(ctx, rmq.ExchangeAddress{Exchange: TimelineExchange, Key: TimelinePublishRK})
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

// DeclareRatingTopology 声明Rating MQ
func (c *RabbitMQClient) DeclareRatingTopology(ctx context.Context) error {
	return c.DeclareTopology(ctx, RatingExchange, RatingQueue, RatingBindingKey)
}

// NewRatingPublisher 返回一个rating mq的生产者
func (c *RabbitMQClient) NewRatingPublisher(ctx context.Context) (*rmq.Publisher, error) {
	return c.NewPublisher(ctx, rmq.ExchangeAddress{Exchange: RatingExchange, Key: RatingPublishRK})
}

// DeclareCommentTopology 声明comment MQ  (exchange, queue, bindKey)
func (c *RabbitMQClient) DeclareCommentTopology(ctx context.Context) error {
	return c.DeclareTopology(ctx, CommentExchange, CommentQueue, CommentBindingKey)
}

func (c *RabbitMQClient) NewCommentPublisher(ctx context.Context) (*rmq.Publisher, error) {
	return c.NewPublisher(ctx, rmq.ExchangeAddress{Exchange: CommentExchange, Key: CommentPublishRK})
}
