package video

import (
	"context"
	"errors"
	"feedsystem/internal/data"
	"feedsystem/internal/model/video"
	"log"

	"gorm.io/gorm"
)

// commentRepo 实现 service/video.CommentRepo
type commentRepo struct {
	db    *gorm.DB
	cache *data.RedisClient
}

func NewCommentRepo(db *gorm.DB, cache *data.RedisClient) *commentRepo {
	return &commentRepo{
		db:    db,
		cache: cache,
	}
}

func (r *commentRepo) Create(ctx context.Context, c *video.Comment) error {
	if c == nil || c.VideoID == 0 || c.AuthorID == 0 || c.AccountID == 0 || c.Content == "" {
		return errors.New("empty comment or invalid params")
	}

	// 调用方未提供幂等键时补一个，避免空串撞唯一索引（正常业务路径由 service 提前生成）
	if c.EventID == "" {
		c.EventID = randHex(16)
	}

	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 幂等：同一 event_id 已存在则跳过（不插入、不计数）
		var cnt int64
		if err := tx.Model(&video.Comment{}).Where("event_id = ?", c.EventID).Count(&cnt).Error; err != nil { //数据库错误
			return err
		}
		if cnt > 0 { // 已经创建过
			return nil
		}

		if err := tx.Create(c).Error; err != nil {
			if errors.Is(err, gorm.ErrDuplicatedKey) { // 并发兜底
				return nil
			}
			return err
		}

		// 只有真正新插入才 comment_count + 1
		err := tx.Model(&video.Video{}).Where("id = ?", c.VideoID).Update("comment_count", gorm.Expr("comment_count + 1")).Error
		return err
	})

	if err != nil {
		return err
	}

	if err := r.cache.Del(ctx, r.cache.Key("video:entity:%d", c.VideoID)); err != nil {
		log.Printf("failed to cache video:entity:%d", c.VideoID)
	}
	return err
}

func (r *commentRepo) Delete(ctx context.Context, commentID uint) error {
	var comment video.Comment
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		err := tx.Model(&video.Comment{}).Where("id = ?", commentID).Find(&comment).Error
		if err != nil {
			return err
		}

		err = tx.WithContext(ctx).Delete(&video.Comment{}, commentID).Error
		if err != nil {
			return err
		}
		err = tx.Model(&video.Video{}).Where("id = ?", comment.VideoID).Update("comment_count", gorm.Expr("GREATEST(comment_count - 1, 0)")).Error
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return err
	}

	if err := r.cache.Del(ctx, r.cache.Key("video:entity:%d", comment.VideoID)); err != nil {
		log.Printf("failed to cache video:entity:%d", comment.VideoID)
	}
	return err
}

func (r *commentRepo) Update(ctx context.Context, c *video.Comment) error {
	if c == nil || c.VideoID == 0 || c.AuthorID == 0 || c.AccountID == 0 || c.Content == "" {
		return errors.New("empty comment or invalid params")
	}
	return r.db.WithContext(ctx).Model(&video.Comment{}).Where("id = ?", c.ID).Update("content", c.Content).Error
}

func (r *commentRepo) GetByID(ctx context.Context, commentID uint) (*video.Comment, error) {
	var comment video.Comment
	err := r.db.WithContext(ctx).First(&comment, commentID).Error
	return &comment, err
}

func (r *commentRepo) ListByVideoID(ctx context.Context, videoID uint, cursor *video.Cursor, limit int8) ([]*video.Comment, error) {
	query := r.db.WithContext(ctx).Model(&video.Comment{}).Where("video_id = ?", videoID)
	if cursor != nil {
		query = query.Where("created_at < ? OR (created_at = ? AND id < ?)", cursor.CreatedAt, cursor.CreatedAt, cursor.ID)
	}
	var comments []*video.Comment
	err := query.Order("created_at desc,id desc").Limit(int(limit)).Find(&comments).Error
	return comments, err
}
