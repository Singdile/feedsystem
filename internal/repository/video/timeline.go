package video

import (
	"context"
	"feedsystem/internal/data"
)

type timeLineCleaner struct {
	cache *data.RedisClient
}

func NewTimeLineCleaner(cache *data.RedisClient) *timeLineCleaner {
	return &timeLineCleaner{cache: cache}
}

func (r *timeLineCleaner) ZRem(ctx context.Context, key string, member string) error {
	return r.cache.ZRem(ctx, key, member)
}
