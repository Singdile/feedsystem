package video

import (
	"context"
	"errors"
	"feedsystem/internal/model/video"

	"gorm.io/gorm"
)

// commentRepo 实现 service/video.CommentRepo
type commentRepo struct {
	db *gorm.DB
}

func NewCommentRepo(db *gorm.DB) *commentRepo {
	return &commentRepo{
		db: db,
	}
}

func (r *commentRepo) Create(ctx context.Context, c *video.Comment) error {
	if c == nil || c.VideoID == 0 || c.AuthorID == 0 || c.AccountID == 0 || c.Content == "" {
		return errors.New("empty comment or invalid params")
	}

	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		err := tx.Create(c).Error
		if err != nil {
			return err
		}
		//db.Model(&product).Update("price", gorm.Expr("price * ? + ?", 2, 100))
		err = tx.Model(&video.Video{}).Where("id = ?", c.VideoID).Update("comment_count", gorm.Expr("comment_count + 1")).Error
		if err != nil {
			return err
		}

		return nil
	})
}

func (r *commentRepo) Delete(ctx context.Context, commentID uint) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var comment video.Comment
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
