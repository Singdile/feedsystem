package video

import (
	"errors"
	"time"
)

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

// StringToStatus 解析 action 字符串为状态值
func StringToStatus(action string) (int8, error) {
	switch action {
	case StatusLikeStr:
		return StatusLike, nil
	case StatusDislikeStr:
		return StatusDislike, nil
	case StatusNoneStr:
		return StatusNone, nil
	default:
		return 0, errors.New("invalid action")
	}
}

// RatingEvent 发送到mq中的rating事件
type RatingEvent struct {
	EventID    string    `json:"event_id"`
	Action     string    `json:"action"`
	AccountID  uint      `json:"account_id"`
	VideoID    uint      `json:"video_id"`
	OccurredAt time.Time `json:"occurred_at"`
}
