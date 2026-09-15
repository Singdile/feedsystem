// Package feed 为feed提供数据库操作支持
package feed

import (
	"context"
	"feedsystem/internal/data"
	"feedsystem/internal/model/feed"
	"feedsystem/internal/model/video"
	"time"

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

// ListLatest 按时间倒序查视频（游标分页）
func (r *feedRepo) ListLatest(ctx context.Context, cursor *video.Cursor, limit int) ([]video.Video, error) {
	items := make([]video.Video, 0)

	query := r.db.WithContext(ctx).Model(&video.Video{}).Order("created_at desc, id DESC")

	if cursor != nil { // cursor 不为空，则继续分页查询
		query = query.Where("(created_at < ?) OR (created_at = ? AND id < ?)", cursor.CreatedAt, cursor.CreatedAt, cursor.ID)
	}

	err := query.Limit(limit).Find(&items).Error
	if err != nil {
		return nil, err
	}
	return items, nil
}

func (r *feedRepo) ZRangeWithScores(ctx context.Context, key string, start, stop int64) ([]feed.ZMember, error) {
	return r.cache.ZRangeWithScores(ctx, key, start, stop)
}

// ZAdd 添加视频信息到时间线
func (r *feedRepo) ZAdd(ctx context.Context, key string, member feed.ZMember) error {
	return r.cache.ZAdd(ctx, key, member)
}

// Key Redis key
func (r *feedRepo) Key(format string, a ...any) string {
	return r.cache.Key(format, a...)
}

// MGet 批量获取redis元素
func (r *feedRepo) MGet(ctx context.Context, keys ...string) ([]any, error) {
	return r.cache.MGet(ctx, keys...)
}

// GetBytes 获取二进制形式的redis元素
func (r *feedRepo) GetBytes(ctx context.Context, key string) ([]byte, error) {
	return r.cache.GetBytes(ctx, key)
}

// SetBytes 设置二进制形式的redis元素
func (r *feedRepo) SetBytes(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	return r.cache.SetBytes(ctx, key, val, ttl)
}
