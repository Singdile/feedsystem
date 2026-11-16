package worker

import (
	"context"
	"encoding/json"
	"feedsystem/internal/data"
	"feedsystem/internal/messaging"
	"feedsystem/internal/model/feed"
	"fmt"
	"log"
)

type fanLister interface {
	ListFanIDs(ctx context.Context, vloggerID uint) ([]uint, error)
}

func TimelineHandler(cache *data.RedisClient, fans fanLister) messaging.Handler {
	return func(ctx context.Context, msg messaging.Message) error {
		var event feed.TimeLineEvent
		if err := json.Unmarshal(msg.Body(), &event); err != nil {
			_ = msg.Accept(ctx) // 解析失败，说明内容本身有问题，重试也是一样的，所以确认来删除，未来可以放置死信队列
			return nil
		}

		// msg ok, but write not ok. retry
		// 写全局时间线; 扇出到分析关注流
		err := retryWithBackOff(3, func() error {
			if err := writeTimeline(ctx, cache, event); err != nil {
				return err
			}

			return writeFollowing(ctx, cache, fans, event)
		})

		if err != nil {
			log.Printf("timeline 处理失败重试之后丢弃: %v", err)
			_ = msg.Accept(ctx)
			return nil
		}
		return msg.Accept(ctx)
	}
}

// writeTimeline 写入 feed:global_timeline 并裁剪保留最近 1000 条
func writeTimeline(ctx context.Context, cache *data.RedisClient, event feed.TimeLineEvent) error {
	err := cache.ZAdd(ctx, "feed:global_timeline", feed.ZMember{
		Score:  float64(event.CreateTime),
		Member: fmt.Sprintf("%d", event.VideoID),
	})
	if err != nil {
		return err
	}

	return cache.ZRemRangeByRank(ctx, "feed:global_timeline", 0, -1001)
}

// writeFollowing 扇出： 写所有粉丝的关注流ZSET
func writeFollowing(ctx context.Context, cache *data.RedisClient, fans fanLister, event feed.TimeLineEvent) error {
	if event.AuthorID == 0 { // 旧快照无作者，跳过扇出
		return nil
	}

	fanIDs, err := fans.ListFanIDs(ctx, event.AuthorID)
	if err != nil {
		return err
	}

	return cache.ZAddFanout(ctx, feed.ZMember{
		Score:  float64(event.CreateTime),
		Member: fmt.Sprintf("%d", event.VideoID),
	}, fanIDs)
}
