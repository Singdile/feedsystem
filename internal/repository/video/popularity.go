package video

import (
	"context"
	"feedsystem/internal/data"
	"strconv"
	"time"
)

type PopularityRepo struct {
	rdb *data.RedisClient
}

func NewPopularityRepo(cache *data.RedisClient) *PopularityRepo {
	return &PopularityRepo{
		rdb: cache,
	}
}

// UpdatePopularity 分钟桶增添元素或者修改元素
func (r *PopularityRepo) UpdatePopularity(ctx context.Context, videoID uint, change float64) error {
	now := time.Now().UTC().Truncate(time.Minute)
	key := r.rdb.Key("hot:video:1m:%s", now.Format("200601021504"))
	err := r.rdb.ZIncrBy(ctx, key, strconv.FormatUint(uint64(videoID), 10), change)
	_ = r.rdb.Expire(ctx, key, time.Duration(2)*time.Hour)
	return err
}
