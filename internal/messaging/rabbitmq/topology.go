package rabbitmq

import (
	"context"
	"errors"
	"feedsystem/internal/messaging"
	"fmt"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

const (
	TimelineExchange   = "video.timeline.events"
	TimelineBindingKey = "video.timeline.*"       //队列绑到交换机时用的 binding key
	TimelinePublishRK  = "video.timeline.publish" //生产者发布信息携带的 routing key

	RatingExchange   = "rating.events"
	RatingBindingKey = "rating.*"
	RatingPublishRK  = "rating.update"

	CommentExchange   = "comment.events"
	CommentBindingKey = "comment.*"
	CommentPublishRK  = "comment.publish" // 唯一 RK，publish/delete 都走它

	SocialExchange   = "social.events" // 关注事件 exchange（仅用于通知，无落库消费者）
	SocialBindingKey = "social.*"
	SocialPublishRK  = "social.follow"

	PopularityExchange   = "popularity.events" // 热榜交换机
	PopularityBindingKey = "popularity.*"      // 队列绑定到交换机的 key
	PopularityPublishRK  = "popularity.update" // 热榜事件发布者发布到交换机的KEY
)

var (
	ErrNilClient = errors.New("messaging: rabbitmq client is nil")
)

const declareStepTimeout = 5 * time.Second

// DeclareAll 声明全部的 exchange / queue / binding (幂等)
func DeclareAll(ctx context.Context, c *Client) error {
	if c == nil {
		return ErrNilClient
	}

	steps := []struct {
		name string
		fn   func(context.Context) error
	}{
		{"timeline", c.DeclareTimelineTopology},
		{"rating", c.DeclareRatingTopology},
		{"comment", c.DeclareCommentTopology},
		{"social", c.DeclareSocialTopology},
		{"notification", c.DeclareNotificationTopology},
		{"popularity", c.DeclarePopularityTopology},
	}

	for _, step := range steps {
		stepCtx, cancel := context.WithTimeout(ctx, declareStepTimeout)
		err := step.fn(stepCtx)
		cancel()
		if err != nil {
			return fmt.Errorf("messaging: declare %s topology: %w", step.name, err)
		}
	}
	return nil
}

// declareTopology 声明交换机和使用bindKey绑定在交换机上的队列
func (c *Client) declareTopology(ctx context.Context, exchange, queueName, bindKey string) error {
	mgt := c.conn.Management()
	// 声明交换机
	if _, err := mgt.DeclareExchange(ctx, &rmq.TopicExchangeSpecification{
		Name: exchange,
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
		SourceExchange:   exchange,
		DestinationQueue: queueName,
		BindingKey:       bindKey,
	})
	return err
}

// DeclareTimelineTopology 声明时间线相关的交换机和队列
func (c *Client) DeclareTimelineTopology(ctx context.Context) error {
	return c.declareTopology(ctx, TimelineExchange, messaging.TimelineQueue, TimelineBindingKey)
}

// DeclareRatingTopology 声明Rating MQ
func (c *Client) DeclareRatingTopology(ctx context.Context) error {
	return c.declareTopology(ctx, RatingExchange, messaging.RatingQueue, RatingBindingKey)
}

// DeclareCommentTopology 声明comment MQ  (exchange, queue, bindKey)
func (c *Client) DeclareCommentTopology(ctx context.Context) error {
	return c.declareTopology(ctx, CommentExchange, messaging.CommentQueue, CommentBindingKey)
}

func (c *Client) DeclareSocialTopology(ctx context.Context) error {
	return c.declareTopology(ctx, SocialExchange, messaging.NotificationQueue, SocialBindingKey)
}

// DeclareNotificationTopology 声明Notification 对列，并绑定到rating\comment\social exchange上
func (c *Client) DeclareNotificationTopology(ctx context.Context) error {
	// 同一个 notification.events 队列，绑定三个来源 exchange，并且绑定key与之前声明的队列的key相同
	// 这样，可以同时接收相同的信息了
	if err := c.declareTopology(ctx, RatingExchange, messaging.NotificationQueue, RatingBindingKey); err != nil {
		return err
	}
	if err := c.declareTopology(ctx, CommentExchange, messaging.NotificationQueue, CommentBindingKey); err != nil {
		return err
	}
	if err := c.declareTopology(ctx, SocialExchange, messaging.NotificationQueue, SocialBindingKey); err != nil {
		return err
	}
	return nil
}

// DeclarePopularityTopology 声明popularity MQ  (exchange, queue, bindKey)
func (c *Client) DeclarePopularityTopology(ctx context.Context) error {
	return c.declareTopology(ctx, PopularityExchange, messaging.PopularityQueue, PopularityBindingKey)
}
