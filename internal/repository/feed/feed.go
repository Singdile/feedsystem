// Package feed 为feed提供数据库操作支持
package feed

import (
	"context"
	"feedsystem/internal/data"
	"feedsystem/internal/model/feed"

	"gorm.io/gorm"
)

type feedRepo struct {
	cache *data.RedisClient
	db    *gorm.DB
}

func NewFeedRepo(cache *data.RedisClient, db *gorm.DB) *feedRepo {
	return &feedRepo{cache: cache, db: db}
}

func (r *feedRepo) ZRevRangeByScore(ctx context.Context, key string, maxScore float64, maxID uint, limit int) ([]feed.ZMember, error) {
	return r.cache.ZRevRangeByScore(ctx, key, maxScore, maxID, limit)
}
