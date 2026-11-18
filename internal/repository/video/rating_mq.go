package video

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"feedsystem/internal/messaging"
	"feedsystem/internal/model/video"
	"time"
)

// ratingMQ 实现 service/video.RatingMQ
type ratingMQ struct {
	pub messaging.Publisher
}

func NewRatingMQ(pub messaging.Publisher) *ratingMQ {
	return &ratingMQ{pub: pub}
}

func (m *ratingMQ) PublishRating(ctx context.Context, action string, accountID, videoID uint, version int64) error {
	if m == nil || m.pub == nil {
		return errors.New("rating mq not initialized")
	}
	msg := video.RatingEvent{
		EventID:    randHex(16),
		Version:    version,
		Action:     action,
		AccountID:  accountID,
		VideoID:    videoID,
		OccurredAt: time.Now(),
	}

	envelope, err := messaging.NewEnvelopeAt(msg.EventID, messaging.EventTypeRating,
		messaging.CurrentEventVersion, msg.OccurredAt, msg)
	if err != nil {
		return err
	}

	return m.pub.Publish(ctx, envelope)
}

// randHex 生成随机的n*2位16进制
func randHex(n int) string {
	b := make([]byte, n)

	// 准备n byte的随机数据
	rand.Read(b)
	// 转换未 16 进制数，4 bit 表示一个16进制数。一个byte表示2个16进制数
	return hex.EncodeToString(b)
}
