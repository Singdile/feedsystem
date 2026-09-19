package video

import (
	"context"
	"errors"
	"feedsystem/internal/model/video"

	"gorm.io/gorm"
)

type ratingRepo struct {
	db *gorm.DB
}

func NewRatingRepo(db *gorm.DB) *ratingRepo {
	return &ratingRepo{db: db}
}

// SetRating 设置video_ratings （幂等性设置）,同步更新videos里的计数信息
// 只需要记录状态为like 和 dislike，未评价的直接删除
func (r *ratingRepo) SetRating(ctx context.Context, videoID, accountID uint, status int8) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var cur video.VideoRating
		// 读取当前状态
		err := tx.Where("video_id = ? AND account_id = ?", videoID, accountID).First(&cur).Error
		var oldStat int8

		if err == nil {
			oldStat = cur.Status
		} else if !errors.Is(err, gorm.ErrRecordNotFound) { // 非空以外的错误直接返回
			return err
		}

		// 设置状态
		// 1. 原本未记录
		if errors.Is(err, gorm.ErrRecordNotFound) {
			if status == 0 {
				return nil
			} else {
				err := tx.Model(&video.VideoRating{}).Create(video.VideoRating{VideoID: videoID, AccountID: accountID, Status: status}).Error
				if err != nil {
					return err
				}
			}
		}

		// 2. 原本有记录，更新记录
		if oldStat == status { //幂等性，多次一样的请求实现的最终效果是一样的
			return nil
		}

		if status == 0 { // 取消评价，删除记录
			err := tx.Where("video_id = ? AND account_id = ?", videoID, accountID).
				Delete(&video.VideoRating{}).Error
			if err != nil {
				return err
			}
		} else { // 切换评价状态: update
			err = tx.Model(&video.VideoRating{}).Where("video_id = ? AND account_id = ?", videoID, accountID).Update("status", status).Error
			if err != nil {
				return err
			}

		}

		// 同步更新video的 like_count disliked_count
		deltaLike, deltaDislike := deltaCount(oldStat, status)
		err = tx.Model(&video.Video{}).
			Where("id = ?", videoID).
			Updates(map[string]any{
				"liked_count":    gorm.Expr("GREATEST(liked_count + ?,0)", deltaLike),
				"disliked_count": gorm.Expr("GREATEST(disliked_count + ?,0)", deltaDislike),
			}).Error
		if err != nil {
			return err
		}
		return nil
	})
}

// deltaCount 计算新的stat带来的的likecount 和 dislikecount 的增量变化
func deltaCount(oldstat, newstat int8) (int8, int8) {
	// oldVote
	var likeOldVote int8
	var dislikeOldVote int8

	if oldstat == 1 {
		likeOldVote = 1
		dislikeOldVote = 0
	} else if oldstat == 0 {
		likeOldVote = 0
		dislikeOldVote = 0
	} else if oldstat == -1 {
		likeOldVote = 0
		dislikeOldVote = 1
	}

	// newVote
	var likeNewVote int8
	var dislikeNewVote int8
	if newstat == 1 {
		likeNewVote = 1
		dislikeNewVote = 0
	} else if newstat == 0 {
		likeNewVote = 0
		dislikeNewVote = 0
	} else if newstat == -1 {
		likeNewVote = 0
		dislikeNewVote = 1
	}

	return likeNewVote - likeOldVote, dislikeNewVote - dislikeOldVote
}
