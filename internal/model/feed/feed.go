package feed

import (
	"feedsystem/internal/model/video"
	"time"
)

// OutboxMsg 视频事件表
type OutboxMsg struct {
	ID         uint      `gorm:"primaryKey"`             //信息ID
	VideoID    uint      `gorm:"index"`                  //视频ID
	EventType  string    `gorm:"type:varchar(50)"`       //事件类型
	CreateTime time.Time `gorm:"autoCreateTime"`         //创建时间
	Status     string    `gorm:"type:varchar(50);index"` //状态，pending 表示落库了数据库，但是还没发送到MQ;DONE,表示发送到了MQ
}

// TimeLineEvent 发布事件消息体(outbox→MQ)
type TimeLineEvent struct {
	EventID    string
	VideoID    uint
	CreateTime int64 // unix ms 作为 zset 的score
	OccurredAt time.Time
}

// ZMember zset元素
type ZMember struct {
	Score  float64 `json:"score"`  // createTime
	Member string  `json:"member"` // videoID
}

// VideoEntityCache 实体缓存 DTO。
// video.Video 的 VideoKey/CoverKey 带 json:"-" 标签（避免泄露到 API 响应），
// 直接用 json.Marshal(video.Video) 缓存会丢失这两个 key，导致 BuildViews 无法生成 URL。
// 因此缓存使用本 DTO（含内部 key），读回时再转回 video.Video。
type VideoEntityCache struct {
	ID          uint      `json:"id"`
	AuthorID    uint      `json:"author_id"`
	Username    string    `json:"username"`
	Title       string    `json:"title"`
	Description string    `json:"description,omitempty"`
	VideoKey    string    `json:"video_key"`
	CoverKey    string    `json:"cover_key"`
	CreatedAt   time.Time `json:"created_at"`
}

func ToVideoEntityCache(v video.Video) VideoEntityCache {
	return VideoEntityCache{
		ID:          v.ID,
		AuthorID:    v.AuthorID,
		Username:    v.Username,
		Title:       v.Title,
		Description: v.Description,
		VideoKey:    v.VideoKey,
		CoverKey:    v.CoverKey,
		CreatedAt:   v.CreatedAt,
	}
}

func (c VideoEntityCache) ToVideo() video.Video {
	return video.Video{
		ID:          c.ID,
		AuthorID:    c.AuthorID,
		Username:    c.Username,
		Title:       c.Title,
		Description: c.Description,
		VideoKey:    c.VideoKey,
		CoverKey:    c.CoverKey,
		CreatedAt:   c.CreatedAt,
	}
}
