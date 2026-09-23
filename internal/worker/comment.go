package worker

import (
	"context"
	"encoding/json"
	"errors"
	"feedsystem/internal/model/video"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

type CommentWriter interface {
	Create(ctx context.Context, c *video.Comment) error
	Delete(ctx context.Context, commentID uint) error
}

// CommentHandler 从commentMQ 中取出并消费comment事件，将comment操作落入数据库中
// commentEvent 可能是publish，可能是 delete，需要判断后执行
// at-least-once 消息一定会送达，只是可能会执行多次。保证多次重复的执行，效果和一次执行是一致的。
func CommentHandler(repo CommentWriter) MQHandler {
	return func(ctx context.Context, dc rmq.IDeliveryContext) error {
		// 解析数据
		var event video.CommentEvent
		if err := json.Unmarshal(dc.Message().GetData(), &event); err != nil { //无法解析，数据本身有问题，直接确认之后丢弃
			_ = dc.Accept(ctx)
			return err
		}

		// 判断action
		if event.Action != "delete" && event.Action != "publish" {
			_ = dc.Accept(ctx)
			return errors.New("invalid action")
		}

		if event.Action == "delete" {
			err := repo.Delete(ctx, event.CommentID)
			if err != nil {
				_ = dc.Requeue(ctx)
				return err
			}
			return dc.Accept(ctx)
		}

		// 组装
		var comment = video.Comment{
			VideoID:   event.VideoID,
			AuthorID:  event.AuthorID,
			AccountID: event.AccountID,
			UserName:  event.UserName,
			Content:   event.Content,
			CreatedAt: event.OccurredAt,
		}

		if err := repo.Create(ctx, &comment); err != nil {
			_ = dc.Requeue(ctx)
			return err
		}
		return dc.Accept(ctx)
	}
}
