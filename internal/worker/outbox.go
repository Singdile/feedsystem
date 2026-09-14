package worker

import (
	"context"
	"feedsystem/internal/data"
	"feedsystem/internal/model/feed"
	"fmt"
	"log"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	"gorm.io/gorm"
)

// OutboxPoller 将视频发布信息发送到redis的feed:global_timeline
// 轮询 outbox_msgs(status=pending)：优先经 MQ 发布时间线事件；
// MQ 不可用/发布失败时降级直写 Redis ZSET。内部 goroutine，1s 轮询。
func OutboxPoller(ctx context.Context, db *gorm.DB, cache *data.RedisClient, pub *rmq.Publisher) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			// get msgs
			var msgs []feed.OutboxMsg
			err := db.WithContext(ctx).Where("status = ?", "pending").Find(&msgs).Order("create_time ASC").Limit(100).Error
			if err != nil {
				log.Printf("outbox Poller Error: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			// 构造视频发布时间线事件，优先发送到mq；发送mq失败，降级到直接写入redis 缓存；
			// 发送成功之后，删除outbox_msg对应记录
			for _, msg := range msgs {
				event := feed.TimeLineEvent{
					EventID:    fmt.Sprintf("%d", msg.ID),
					VideoID:    msg.VideoID,
					CreateTime: msg.CreateTime.UnixMilli(),
					OccurredAt: time.Now(),
				}

				ok := false
				if pub != nil {
					if err := data.PublishJSON(ctx, pub, event); err != nil {
						log.Printf("outbox publish to mq failed,err: %v,now fallback to write redis directly", err)
					} else {
						ok = true
					}
				}

				// write
				if !ok && cache != nil {
					err := writeTimeline(ctx, cache, event)
					if err != nil {
						log.Printf("outbox fallback ZADD error: %v,video=%d", err, msg.VideoID)
					} else {
						ok = true
					}
				}

				if ok {
					if err := db.WithContext(ctx).Delete(&feed.OutboxMsg{}, msg.ID).Error; err != nil {
						log.Printf("outbox Poller Delete Error: %v", err)
					}
				}
			}
			time.Sleep(1 * time.Second)
		}
	}()
}
