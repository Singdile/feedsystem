package social

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"feedsystem/internal/data"
	"feedsystem/internal/model/account"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

// socialMQ 实现 service/social  的 SocialMQ 接口
type socialMQ struct {
	pub *rmq.Publisher
}

func NewSocialMQ(pub *rmq.Publisher) *socialMQ {
	return &socialMQ{pub: pub}
}

func (m *socialMQ) PublishFollow(ctx context.Context, fanID, vloggerID uint) error {
	if m == nil || m.pub == nil {
		return errors.New("mq not initialized")
	}

	event := account.SocialEvent{
		EventID:    randHex(16),
		Action:     "follow",
		FollowerID: fanID,
		VloggerID:  vloggerID,
	}
	return data.PublishJSON(ctx, m.pub, event)
}

// randHex 生成随机的n*2位16进制
func randHex(n int) string {
	b := make([]byte, n)

	// 准备n byte的随机数据
	rand.Read(b)
	// 转换未 16 进制数，4 bit 表示一个16进制数。一个byte表示2个16进制数
	return hex.EncodeToString(b)
}
