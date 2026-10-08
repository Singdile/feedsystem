package account

import "time"

type Notification struct {
	ID          uint      `gorm:"primaryKey" json:"id"`
	RecipientID uint      `gorm:"index;not null" json:"recipient_id"`    // 接受者ID
	SenderID    uint      `gorm:"not null" json:"sender_id"`             // 发送者ID
	Type        string    `gorm:"type:varchar(50);not null" json:"type"` // like/comment/follow/mention
	TargetID    uint      `json:"target_id"`                             // 关联ID: like/comment/mention→视频id; follow→sender(id)即关注者
	Content     string    `gorm:"type:varchar(255)" json:"content"`      // 内容
	IsRead      bool      `gorm:"default:false" json:"is_read"`          // 接受者是否阅读
	CreatedAt   time.Time `gorm:"autoCreateTime" json:"created_at"`
}
