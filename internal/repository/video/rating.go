package video

import (
	"context"
	"errors"
	"feedsystem/internal/data"
	"feedsystem/internal/model/video"
	"log"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	ratingCacheTTL   = 7 * 24 * time.Hour
	ratingVersionTTL = 7 * 24 * time.Hour
)

type ratingRepo struct {
	db    *gorm.DB
	cache *data.RedisClient
}

func NewRatingRepo(db *gorm.DB, cache *data.RedisClient) *ratingRepo {
	return &ratingRepo{db: db, cache: cache}
}

// SetRating 设置video_ratings （幂等性设置）,同步更新videos里的计数信息
// 只接受版本更新的操作；取消评价写入 status = 0
func (r *ratingRepo) SetRating(ctx context.Context, videoID, accountID uint, status int8, version int64) error {
	changed := false // 计数是否真正发生变更（幂等 no-op 不失效缓存）
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var cur video.VideoRating
		// 悲观锁,读取当前状态
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("video_id = ? AND account_id = ?", videoID, accountID).First(&cur).Error

		notFound := errors.Is(err, gorm.ErrRecordNotFound)
		if err != nil && !notFound { //操作出现错误，并且不是未查找到的错误
			return err
		}

		if !notFound && version <= cur.Version { //乱序，重复直接跳过 (幂等)
			return nil
		}

		var oldStat int8
		if !notFound {
			oldStat = cur.Status
		}

		if notFound { // 首次记录
			if err := tx.Create(&video.VideoRating{VideoID: videoID, AccountID: accountID, Status: status, Version: version}).Error; err != nil {
				return err
			}
		} else { //更新记录
			err := tx.Model(&video.VideoRating{}).Where("video_id = ? AND account_id = ?", videoID, accountID).Updates(map[string]any{"status": status, "version": version}).Error
			if err != nil {
				return err
			}
		}

		// 同步更新video的 like_count disliked_count
		deltaLike, deltaDislike := deltaCount(oldStat, status)
		if deltaDislike != 0 || deltaLike != 0 {
			err = tx.Model(&video.Video{}).
				Where("id = ?", videoID).
				Updates(map[string]any{
					"liked_count":    gorm.Expr("GREATEST(liked_count + ?,0)", deltaLike),
					"disliked_count": gorm.Expr("GREATEST(disliked_count + ?,0)", deltaDislike),
				}).Error
			if err != nil {
				return err
			}
			changed = true // 计数已更新，才失效缓存
		}
		return nil
	})

	if err != nil {
		return err
	}

	if changed && r.cache != nil {
		if err := r.cache.Del(ctx, r.cache.Key("video:entity:%d", videoID)); err != nil {
			log.Printf("failed to cache video:entity:%d", videoID)
		}
	}

	return nil
}

// deltaCount 计算新的stat带来的的likecount 和 dislikecount 的增量变化
func deltaCount(oldstat, newstat int8) (int8, int8) {
	// oldVote
	var likeOldVote int8
	var dislikeOldVote int8

	switch oldstat {
	case 1:
		likeOldVote = 1
		dislikeOldVote = 0

	case 0:
		likeOldVote = 0
		dislikeOldVote = 0

	case -1:
		likeOldVote = 0
		dislikeOldVote = 1
	}

	// newVote
	var likeNewVote int8
	var dislikeNewVote int8
	switch newstat {
	case 1:
		likeNewVote = 1
		dislikeNewVote = 0

	case 0:
		likeNewVote = 0
		dislikeNewVote = 0

	case -1:
		likeNewVote = 0
		dislikeNewVote = 1
	}

	return likeNewVote - likeOldVote, dislikeNewVote - dislikeOldVote
}

// GetRating 获取用户对视频的评价
func (r *ratingRepo) GetRating(ctx context.Context, videoID, accountID uint) (int8, error) {
	if accountID == 0 || videoID == 0 {
		return 0, errors.New("invalid argument")
	}

	var vRating video.VideoRating
	err := r.db.WithContext(ctx).Where("video_id = ? AND account_id = ?", videoID, accountID).Find(&vRating).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, nil
		}
		return 0, err
	}

	return vRating.Status, nil
}

// GetRatings 获取用户对一批视频的评价
func (r *ratingRepo) GetRatings(ctx context.Context, videoIDs []uint, accountID uint) (map[uint]int8, error) {
	if accountID == 0 || len(videoIDs) == 0 {
		return nil, nil
	}

	ratings := make(map[uint]int8)

	var rows []video.VideoRating

	err := r.db.WithContext(ctx).Where("video_id IN (?) AND account_id = ?", videoIDs, accountID).Find(&rows).Error
	if err != nil {
		return nil, err
	}

	for _, v := range rows {
		ratings[v.VideoID] = v.Status
	}

	return ratings, nil
}

// ListLikedVideos 查询用户点赞过的视频
func (r *ratingRepo) ListLikedVideos(ctx context.Context, accountID uint, cursor *video.Cursor, limit int) ([]video.Video, error) {
	if accountID == 0 {
		return nil, errors.New("invalid argument")
	}

	if limit <= 0 || limit > 100 {
		limit = 20
	}

	q := r.db.WithContext(ctx).Model(&video.Video{}).Joins("JOIN video_ratings vr ON vr.video_id = videos.id").Where("vr.account_id = ? AND vr.status = ?", accountID, 1)

	// 游标：上一页最后一条 (created_at, id)，对齐现有 video.List 的写法
	if cursor != nil {
		q = q.Where("(videos.created_at < ?) OR (videos.created_at = ? AND videos.id < ?)",
			cursor.CreatedAt, cursor.CreatedAt, cursor.ID)
	} else {
		q = q.Where("videos.created_at < ?", time.Now())
	}

	var items []video.Video
	err := q.Order("videos.created_at DESC,videos.id DESC").Limit(limit).Find(&items).Error
	return items, err
}

// 预热：从DB中全量注入到cache中并添加TTL
func (r *ratingRepo) WarmupRatingCache(ctx context.Context, accountID uint) error {
	if r.cache == nil {
		return nil
	}

	var rows []video.VideoRating

	// 查询用户所有的评价
	err := r.db.WithContext(ctx).Select("video_id,status").Where("account_id = ?", accountID).Find(&rows).Error
	if err != nil {
		return err
	}

	// 存入缓存
	likedKey := r.cache.Key("account:%d:liked", accountID)
	dislikedKey := r.cache.Key("account:%d:disliked", accountID)
	liked, disliked := []any{}, []any{}
	for _, row := range rows {
		if row.Status == 1 {
			liked = append(liked, row.VideoID)
		}
		if row.Status == -1 {
			disliked = append(disliked, row.VideoID)
		}
	}

	if len(liked) == 0 {
		liked = append(liked, 0)
	}

	if len(disliked) == 0 {
		disliked = append(disliked, 0)
	}

	r.cache.SAdd(ctx, likedKey, liked...)
	r.cache.SAdd(ctx, dislikedKey, disliked...)
	r.cache.Expire(ctx, likedKey, ratingCacheTTL)
	r.cache.Expire(ctx, dislikedKey, ratingCacheTTL)
	return nil
}

// 增量同步
func (r *ratingRepo) UpdateRatingCache(ctx context.Context, accountID, videoID uint, status int8) error {
	if r.cache == nil {
		return nil
	}

	likedKey := r.cache.Key("account:%d:liked", accountID)
	dislikedKey := r.cache.Key("account:%d:disliked", accountID)

	vid := videoID

	switch status {
	case 1:
		r.cache.SAdd(ctx, likedKey, vid)
		r.cache.SRem(ctx, dislikedKey, vid)
	case -1:
		r.cache.SAdd(ctx, dislikedKey, vid)
		r.cache.SRem(ctx, likedKey, vid)
	default:
		r.cache.SRem(ctx, dislikedKey, vid)
		r.cache.SRem(ctx, likedKey, vid)
	}
	return nil
}

// 懒加载重建用户评价信息
func (r *ratingRepo) ensureRatingCache(ctx context.Context, accountID uint) error {
	likedKey := r.cache.Key("account:%d:liked", accountID)
	dislikedKey := r.cache.Key("account:%d:disliked", accountID)

	existsLiked, err := r.cache.Exists(ctx, likedKey)
	if err != nil {
		return err
	}

	existsDisliked, err := r.cache.Exists(ctx, dislikedKey)
	if err != nil {
		return err
	}

	if !existsLiked || !existsDisliked {
		if err := r.WarmupRatingCache(ctx, accountID); err != nil {
			return err
		}
	}

	return nil
}

func (r *ratingRepo) GetRatingCache(ctx context.Context, accountID uint, videoIDs ...uint) ([]int8, error) {
	if len(videoIDs) == 0 {
		return nil, errors.New("videoID empty")
	}
	// 先确保cache可用
	err := r.ensureRatingCache(ctx, accountID)
	if err != nil {
		return nil, err
	}

	// 获取数据
	likedKey := r.cache.Key("account:%d:liked", accountID)
	dislikedKey := r.cache.Key("account:%d:disliked", accountID)

	members := make([]any, len(videoIDs))
	for i, id := range videoIDs {
		members[i] = id
	}
	liked, err := r.cache.SMIsMember(ctx, likedKey, members...)
	if err != nil {
		return nil, err
	}
	disliked, err := r.cache.SMIsMember(ctx, dislikedKey, members...)
	if err != nil {
		return nil, err
	}

	// 返回
	result := make([]int8, len(videoIDs))
	for i := range videoIDs {
		switch {
		case liked[i]:
			result[i] = 1
		case disliked[i]:
			result[i] = -1
		default:
			result[i] = 0
		}
	}
	return result, nil
}

// NextRatingVersion rating事件的发号器
// 当 key 对应的 发号器有效，直接使用；当key 对应的发号器无效，从DB中使用seed重建
func (r *ratingRepo) NextRatingVersion(ctx context.Context, videoID, accountID uint) (int64, error) {
	if r.cache == nil {
		return 0, errors.New("rating version: cache unavailable")
	}

	key := r.cache.Key("rating:ver:%d:%d", videoID, accountID)
	exist, err := r.cache.Exists(ctx, key)
	if err != nil {
		return 0, err
	}

	var seed int64
	if !exist { //需要重建
		var cur video.VideoRating
		err := r.db.WithContext(ctx).Select("version").Where("video_id = ? AND account_id = ?", videoID, accountID).First(&cur).Error
		switch {
		case errors.Is(err, gorm.ErrRecordNotFound):
			seed = 0
		case err != nil:
			return 0, err
		default:
			seed = cur.Version
		}
	}
	return r.cache.IncrFrom(ctx, key, seed, ratingVersionTTL)
}
