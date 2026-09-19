package video

import "time"

// VideoRating 记录user 与 video 的rating 信息
type VideoRating struct {
	VideoID   uint      `gorm:"primaryKey" json:"video_id"`
	AccountID uint      `gorm:"primaryKey" json:"account_id"`
	Status    int8      `gorm:"default 0" json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

const (
	StatusLike    = int8(1)
	StatusDislike = int8(-1)
	StatusNone    = int8(0)

	StatusLikeStr    = "like"
	StatusDislikeStr = "dislike"
	StatusNoneStr    = "none"
)

func StatusToString(status int8) string {
	switch status {
	case StatusLike:
		return StatusLikeStr
	case StatusDislike:
		return StatusDislikeStr
	case StatusNone:
		return StatusNoneStr
	default:
		return StatusNoneStr
	}
}
