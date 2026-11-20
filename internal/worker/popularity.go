package worker

import (
	"context"
	"feedsystem/internal/messaging"
	"feedsystem/internal/model/video"
	"log"
	"time"
)

const popularityRequeueBackoff = 500 * time.Millisecond

type cacheSetter interface {
	UpdatePopularity(ctx context.Context, videoID uint, change float64) error
	SetNX(ctx context.Context, key string, val any, ttl time.Duration) (bool, error)
	Del(ctx context.Context, key string) error
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
		key := "idem:popularity:" + popularityEvent.EventID // 热度事件幂等标记
		ok, err := cache.SetNX(ctx, key, "1", 24*time.Hour)
		if err != nil {
			log.Printf("popularity: setnx %s failed, requeue event=%s: %v",
				key, popularityEvent.EventID, err)
			requeuePopularity(ctx, msg)
			return nil
		}

		if !ok { //已经被处理了
			_ = msg.Accept(ctx)
			return nil
		}

		// 首次失败后最多再重试 3 次；仍失败则释放幂等标记并丢弃（热度是近似值）
		if err := cache.UpdatePopularity(ctx, popularityEvent.VideoID, float64(popularityEvent.Change)); err != nil {
			err = retryWithBackOffCtx(ctx, 3, 100*time.Millisecond, func() error {
				return cache.UpdatePopularity(ctx, popularityEvent.VideoID, float64(popularityEvent.Change))
			})
			if err != nil {
				cleanUpCtx, cancel := messaging.SettleContext(ctx)
				if derr := cache.Del(cleanUpCtx, key); derr != nil {
					log.Printf("popularity: release idem key %s failed: %v", key, derr)
				}
				cancel()

				log.Printf("popularity: 重试后丢弃 video=%d event=%s: %v",
					popularityEvent.VideoID, popularityEvent.EventID, err)
				_ = msg.Accept(ctx)
				return nil
			}
		}

		return msg.Accept(ctx)
	}
}

// requeuePopularity 在 requeue 前等待一小段时间，避免 Redis 故障时热循环。
func requeuePopularity(ctx context.Context, msg messaging.Message) {
	// ctx 已超时就没有等待的必要，直接 requeue
	if ctx.Err() == nil {
		select {
		case <-time.After(popularityRequeueBackoff):
		case <-ctx.Done():
		}
	}

	if err := msg.Requeue(ctx); err != nil { // rmqDelivery 内部会用 SettleContext
		log.Printf("popularity: requeue failed: %v", err)
	}
}
