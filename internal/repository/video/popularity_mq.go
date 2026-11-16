package video

import (
	"context"
	"errors"
	"feedsystem/internal/messaging"
	"feedsystem/internal/model/video"
)

// popularityMQ
type popularityMQ struct {
	pub *messaging.Publisher
}

func NewPopularityMQ(pub *messaging.Publisher) *popularityMQ {
	return &popularityMQ{pub: pub}
}

func (m *popularityMQ) UpdatePopularity(ctx context.Context, videoID uint, change float64) error {
	if m == nil || m.pub == nil {
		return errors.New("rating mq not initialized")
	}
	msg := video.PopularityEvent{
		EventID: randHex(16),
		VideoID: videoID,
		Change:  int64(change),
	}

	return m.pub.PublishJSON(ctx, msg)
}
