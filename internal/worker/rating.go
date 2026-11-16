package worker

import (
	"context"
	"feedsystem/internal/messaging"
	"feedsystem/internal/model/video"
	"log"
)

type ratingSetter interface {
	SetRating(ctx context.Context, videoID, accountID uint, status int8) error
}

// RatingHandler 从ratingMQ 中取出并消费rating事件，将rating操作落入数据库中(更新video_ratings 和 videos rating count)
func RatingHandler(repo ratingSetter) messaging.Handler {
	return func(ctx context.Context, msg messaging.Message) error {
		// 从mq中取出数据并反序列化
		var ratingEvent video.RatingEvent
		err := messaging.DecodeEvent(msg.Body(), messaging.EventTypeRating, &ratingEvent)
		if err != nil {
			_ = msg.Accept(ctx)
			return err
		}

		// 调用DB进行写操作
		stat, err := video.StringToStatus(ratingEvent.Action)
		if err != nil {
			_ = msg.Accept(ctx)
			return nil
		}

		err = retryWithBackOff(3, func() error {
			return repo.SetRating(ctx, ratingEvent.VideoID, ratingEvent.AccountID, stat)
		})

		if err != nil {
			log.Printf("rating 处理失败重试之后丢弃: %v", err)
			_ = msg.Accept(ctx)
			return nil
		}
		return msg.Accept(ctx)
	}
}
