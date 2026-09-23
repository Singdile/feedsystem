package video

import (
	"context"
	"errors"
	"feedsystem/internal/data"
	"feedsystem/internal/model/video"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

type commentMQ struct {
	pub *rmq.Publisher
}

func NewCommentMQ(pub *rmq.Publisher) *commentMQ {
	return &commentMQ{
		pub: pub,
	}
}

func (mq *commentMQ) Publish(ctx context.Context, c *video.Comment) error {
	// 参数检验
	if mq == nil || mq.pub == nil {
		return errors.New("comment mq not initialized")
	}

	event := video.CommentEvent{
		EventID:    randHex(16),
		Action:     "publish",
		VideoID:    c.VideoID,
		AuthorID:   c.AuthorID,
		AccountID:  c.AccountID,
		UserName:   c.UserName,
		Content:    c.Content,
		OccurredAt: time.Now(),
	}
	return data.PublishJSON(ctx, mq.pub, event)
}
func (mq *commentMQ) Delete(ctx context.Context, commentID uint) error {
	if mq == nil || mq.pub == nil {
		return errors.New("comment mq not initialized")
	}

	event := video.CommentEvent{
		EventID:    randHex(16),
		Action:     "delete",
		CommentID:  commentID,
		OccurredAt: time.Now(),
	}
	return data.PublishJSON(ctx, mq.pub, event)
}
