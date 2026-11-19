package feed

import (
	"time"
)

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
