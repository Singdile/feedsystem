package video

import (
	"context"
	"feedsystem/internal/data"
	"log"
	"strconv"
)

type videoCacheCleaner struct {
	cache *data.RedisClient
}

func NewVideoCacheCleaner(cache *data.RedisClient) *videoCacheCleaner {
	return &videoCacheCleaner{cache: cache}
}

// RemoveVideoCache 删除视频相关的全部缓存：时间线+实体缓存
func (r *videoCacheCleaner) RemoveVideoCache(ctx context.Context, videoID uint) error {
	member := strconv.FormatUint(uint64(videoID), 10)
	err := r.cache.ZRem(ctx, "feed:global_timeline", member)
	if err != nil {
		log.Printf("删除视频时间线失败,err:%v", err)
	}

	_ = r.cache.Del(ctx, r.cache.Key("video:entity:%d", videoID)) // 删除实体缓存
	return err
}
