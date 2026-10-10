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
	TimelineExchange     = "video.timeline.events"
	TimelineQueue        = "video.timeline.update.queue"
	TimelineBindingKey   = "video.timeline.*"       //队列绑到交换机时用的 binding key
	TimelinePublishRK    = "video.timeline.publish" //生产者发布信息携带的 routing key
	RatingExchange       = "rating.events"
	RatingQueue          = "rating.events"
	RatingBindingKey     = "rating.*"
	RatingPublishRK      = "rating.update"
	CommentExchange      = "comment.events"
	CommentQueue         = "comment.events"
	CommentBindingKey    = "comment.*"
	CommentPublishRK     = "comment.publish" // 唯一 RK，publish/delete 都走它
	SocialExchange       = "social.events"   // 关注事件 exchange（仅用于通知，无落库消费者）
	SocialBindingKey     = "social.*"
	SocialPublishRK      = "social.follow"
	NotificationQueue    = "notification.events" // 通知队列（API 进程 NotificationWorker 消费）
	PopularityExchange   = "popularity.events"   // 热榜交换机
	PopularityQueue      = "popularity.events"   // 热榜事件队列
	PopularityBindingKey = "popularity.*"        // 队列绑定到交换机的 key
	PopularityPublishRK  = "popularity.update"   // 热榜事件发布者发布到交换机的KEY
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

func (c *RabbitMQClient) DeclareSocialTopology(ctx context.Context) error {
	return c.DeclareTopology(ctx, SocialExchange, NotificationQueue, SocialBindingKey)
}

// NewSocialPublisher 创建发布到 social.events exchange 的 publisher
func (c *RabbitMQClient) NewSocialPublisher(ctx context.Context) (*rmq.Publisher, error) {
	return c.NewPublisher(ctx, rmq.ExchangeAddress{Exchange: SocialExchange, Key: SocialPublishRK})
}

// DeclareNotificationTopology 声明Notification 对列，并绑定到rating\comment\social exchange上
func (c *RabbitMQClient) DeclareNotificationTopology(ctx context.Context) error {
	// 同一个 notification.events 队列，绑定三个来源 exchange，并且绑定key与之前声明的队列的key相同
	// 这样，可以同时接收相同的信息了
	if err := c.DeclareTopology(ctx, RatingExchange, NotificationQueue, RatingBindingKey); err != nil {
		return err
	}
	if err := c.DeclareTopology(ctx, CommentExchange, NotificationQueue, CommentBindingKey); err != nil {
		return err
	}
	if err := c.DeclareTopology(ctx, SocialExchange, NotificationQueue, SocialBindingKey); err != nil {
		return err
	}
	return nil
}

// DeclarePopularityTopology 声明popularity MQ  (exchange, queue, bindKey)
func (c *RabbitMQClient) DeclarePopularityTopology(ctx context.Context) error {
	return c.DeclareTopology(ctx, PopularityExchange, PopularityQueue, PopularityBindingKey)
}

func (c *RabbitMQClient) NewPopularityPublisher(ctx context.Context) (*rmq.Publisher, error) {
	return c.NewPublisher(ctx, rmq.ExchangeAddress{Exchange: PopularityExchange, Key: PopularityPublishRK})
}
