package worker

import (
	"context"
	"encoding/json"
	"feedsystem/internal/model/video"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

type ratingSetter interface {
	SetRating(ctx context.Context, videoID, accountID uint, status int8) error
}

// RatingHandler 从ratingMQ 中取出并消费rating事件，将rating操作落入数据库中(更新video_ratings 和 videos rating count)
func RatingHandler(repo ratingSetter) MQHandler {
	return func(ctx context.Context, dc rmq.IDeliveryContext) error {
		var ratingEvent video.RatingEvent
		// 从mq中取出数据并反序列化
		if err := json.Unmarshal(dc.Message().GetData(), &ratingEvent); err != nil {
			_ = dc.Accept(ctx) // 解析失败，说明内容本身有问题，重试也是一样的，所以确认来删除，未来可以放置死信队列
			return nil
		}

		// 调用DB进行写操作
		stat, err := video.StringToStatus(ratingEvent.Action)
		if err != nil {
			_ = dc.Accept(ctx)
			return nil
		}

		if err := repo.SetRating(ctx, ratingEvent.VideoID, ratingEvent.AccountID, stat); err != nil {
			_ = dc.Requeue(ctx)
			return err
		}

		return dc.Accept(ctx)
	}
}
