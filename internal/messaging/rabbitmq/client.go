package rabbitmq

import (
	"context"
	"feedsystem/internal/config"
	"feedsystem/internal/messaging"
	"fmt"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

// Client 封装 RabbitMQ 连接
type Client struct {
	env  *rmq.Environment
	conn *rmq.AmqpConnection
}

func NewClient(conf config.RabbitMQConfig) (*Client, error) {
	brokerURI := fmt.Sprintf("amqp://%s:%s@%s:%d/", conf.Username, conf.Password, conf.Host, conf.Port)
	env := rmq.NewEnvironment(brokerURI, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := env.NewConnection(ctx) //马上建立连接
	if err != nil {
		return nil, err
	}

	return &Client{
		env:  env,
		conn: conn,
	}, nil
}
func (c *Client) Close(ctx context.Context) error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close(ctx)
}

func (c *Client) newPublisher(ctx context.Context, exchange, routingKey string) (messaging.Publisher, error) {
	addr := rmq.ExchangeAddress{Exchange: exchange, Key: routingKey}
	pub, err := c.conn.NewPublisher(ctx, &addr, nil)
	if err != nil {
		return nil, err
	}
	return &rmqPublisher{pub: pub}, nil
}

func (c *Client) OpenSource(ctx context.Context, queue string, prefetch int32) (messaging.MessageSource, error) {
	switch c.conn.State().(type) {
	case *rmq.StateOpen:
		// 可以创建
	case *rmq.StateClosed:
		return nil, messaging.ErrConnectionClosed
	default:
		return nil, messaging.ErrConnectionNotReady
	}

	cons, err := c.conn.NewConsumer(ctx, queue, &rmq.ConsumerOptions{InitialCredits: prefetch})
	if err != nil {
		return nil, err
	}
	return rmqConsumerSource{c: cons}, nil
}

func (c *Client) NewTimelinePublisher(ctx context.Context) (messaging.Publisher, error) {
	return c.newPublisher(ctx, TimelineExchange, TimelinePublishRK)
}

func (c *Client) NewRatingPublisher(ctx context.Context) (messaging.Publisher, error) {
	return c.newPublisher(ctx, RatingExchange, RatingPublishRK)
}

func (c *Client) NewCommentPublisher(ctx context.Context) (messaging.Publisher, error) {
	return c.newPublisher(ctx, CommentExchange, CommentPublishRK)
}

func (c *Client) NewSocialPublisher(ctx context.Context) (messaging.Publisher, error) {
	return c.newPublisher(ctx, SocialExchange, SocialPublishRK)
}

func (c *Client) NewPopularityPublisher(ctx context.Context) (messaging.Publisher, error) {
	return c.newPublisher(ctx, PopularityExchange, PopularityPublishRK)
}

var _ messaging.SourceFactory = (*Client)(nil)
