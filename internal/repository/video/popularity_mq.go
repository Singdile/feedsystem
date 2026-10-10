package video

import (
	"context"
	"errors"
	"feedsystem/internal/data"
	"feedsystem/internal/model/video"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

// popularityMQ
type popularityMQ struct {
	pub *rmq.Publisher
}

func NewPopularityMQ(pub *rmq.Publisher) *popularityMQ {
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

	return data.PublishJSON(ctx, m.pub, msg)
}
