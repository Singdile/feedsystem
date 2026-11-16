package video

import (
	"context"
	"errors"
	"feedsystem/internal/messaging"
	"feedsystem/internal/model/video"
	"time"
)

type commentMQ struct {
	pub *messaging.Publisher
}

func NewCommentMQ(pub *messaging.Publisher) *commentMQ {
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
		EventID:    c.EventID,
		Action:     "publish",
		VideoID:    c.VideoID,
		AuthorID:   c.AuthorID,
		AccountID:  c.AccountID,
		UserName:   c.UserName,
		Content:    c.Content,
		OccurredAt: time.Now(),
	}
	return mq.pub.PublishJSON(ctx, event)
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
	return mq.pub.PublishJSON(ctx, event)
}
