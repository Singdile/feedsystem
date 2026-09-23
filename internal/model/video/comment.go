package video

import "time"

// Comment 视频评论
type Comment struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	VideoID   uint      `gorm:"index" json:"video_id"`    //视频id
	AuthorID  uint      `gorm:"index" json:"author_id"`   //视频创作者id
	AccountID uint      `gorm:"index" json:"account_id"`  //评价者的id
	UserName  string    `json:"user_name"`                //评价者的名称
	Content   string    `gorm:"type:text" json:"content"` //评论内容
	CreatedAt time.Time `json:"created_at"`
}

// CommentEvent 发送到mq中的comment事件
type CommentEvent struct {
	EventID    string    `json:"event_id"`
	Action     string    `json:"action"`               // 创建评论 或 删除评论
	CommentID  uint      `json:"comment_id,omitempty"` // 删除的时候使用
	VideoID    uint      `json:"video_id,omitempty"`
	AuthorID   uint      `json:"author_id,omitempty"`
	AccountID  uint      `json:"account_id,omitempty"`
	UserName   string    `json:"user_name,omitempty"`
	Content    string    `json:"content,omitempty"`
	OccurredAt time.Time `json:"occurred_at"`
}
