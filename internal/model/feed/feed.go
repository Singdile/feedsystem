package feed

import (
	"time"
)

// OutboxMsg 视频事件表
type OutboxMsg struct {
	ID         uint      `gorm:"primaryKey"`             //信息ID
	VideoID    uint      `gorm:"index"`                  //视频ID
	AuthorID   uint      `gorm:"index"`                  //作者ID
	EventType  string    `gorm:"type:varchar(50)"`       //事件类型
	CreateTime time.Time `gorm:"autoCreateTime"`         //创建时间
	Status     string    `gorm:"type:varchar(50);index"` //状态，pending 表示落库了数据库，但是还没发送到MQ;DONE,表示发送到了MQ
}

// TimeLineEvent 发布事件消息体(outbox→MQ)
type TimeLineEvent struct {
	EventID    string    `json:"event_id"`
	VideoID    uint      `json:"video_id"`
	AuthorID   uint      `json:"author_id"`
	CreateTime int64     `json:"create_time"` // unix ms 作为 zset 的score
	OccurredAt time.Time `json:"occurred_at"`
}

// ZMember zset元素
type ZMember struct {
	Score  float64 `json:"score"`  // createTime
	Member string  `json:"member"` // videoID
}

type ListByTagReq struct {
	TagName string `json:"tag_name"`
	Cursor  string `json:"cursor"`
}
