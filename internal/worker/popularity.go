package worker

import (
	"context"
	"feedsystem/internal/messaging"
	"feedsystem/internal/model/video"
	"time"
)

type cacheSetter interface {
	UpdatePopularity(ctx context.Context, videoID uint, change float64) error
}

// PopularityHandler 从popularityMQ 中取出并消费 popularity 事件，增加cache中的热度
func PopularityHandler(cache cacheSetter) messaging.Handler {
	return func(ctx context.Context, msg messaging.Message) error {
		var popularityEvent video.PopularityEvent
		// 从mq中取出数据并反序列化
		if err := messaging.DecodeEvent(msg.Body(), messaging.EventTypePopularity, &popularityEvent); err != nil {
			_ = msg.Accept(ctx) // 解析失败，说明内容本身有问题，重试也是一样的，所以确认来删除，未来可以放置死信队列
			return err
		}

		// 调用cache进行写操作
		processErr := cache.UpdatePopularity(ctx, popularityEvent.VideoID, float64(popularityEvent.Change))

		for i := 1; processErr != nil && i <= 3; i++ {
			time.Sleep(time.Duration(1<<(i-1)) * time.Second)
			processErr = cache.UpdatePopularity(ctx, popularityEvent.VideoID, float64(popularityEvent.Change))
		}

		return msg.Accept(ctx)
	}
}
