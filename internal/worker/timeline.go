package worker

import (
	"context"
	"feedsystem/internal/data"
	"feedsystem/internal/model/feed"
	"fmt"
)

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
