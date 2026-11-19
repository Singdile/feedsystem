package outbox

import (
	"time"
)

const (
	StatusPending    = "pending"
	StatusPublishing = "publishing"
	StatusDone       = "done"
	StatusFailed     = "failed"
)

// EventTypePublish 视频发布事件（当前 outbox 只承载这一种）
const EventTypePublish = "publish"

// Msg 是一条待投递的事件记录。
type Msg struct {
	ID uint `gorm:"primaryKey"`

	EventType string `gorm:"type:varchar(50);not null"`
	VideoID   uint   `gorm:"index"`
	AuthorID  uint   `gorm:"index"`

	CreateTime time.Time `gorm:"autoCreateTime;index:idx_outbox_claim,priority:2"`

	// pending / publishing / done / failed
	Status string `gorm:"type:varchar(20);not null;default:pending;index:idx_outbox_claim,priority:1"`

	// 已被认领发布的次数（认领时 +1，成功/失败/崩溃都计数）
	Attempts int `gorm:"not null;default:0"`

	// 下次允许被认领的时间；nil 表示立即可认领
	NextRetryAt *time.Time `gorm:"type:datetime(3)"`

	// 本次认领的开始时间；仅 publishing 时有值，用于超时回收
	ClaimedAt *time.Time `gorm:"type:datetime(3)"`

	// 最近一次发布失败的原因；认领/成功时清空
	LastError string `gorm:"type:varchar(512)"`
}

func (Msg) TableName() string { return "outbox_msgs" }
