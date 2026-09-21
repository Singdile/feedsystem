package worker

import (
	"context"
	"encoding/json"
	"errors"
	"feedsystem/internal/model/video"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type MockRatingSetter struct {
	mock.Mock
}

func (m *MockRatingSetter) SetRating(ctx context.Context, videoID, accountID uint, status int8) error {
	args := m.Called(ctx, videoID, accountID, status)
	return args.Error(0)
}

type FakeIDeliveryContext struct {
	msg      *amqp.Message
	accepted bool
	requeued bool
}

func (dc *FakeIDeliveryContext) Message() *amqp.Message {
	return dc.msg
}

func (dc *FakeIDeliveryContext) Accept(ctx context.Context) error {
	dc.accepted = true
	return nil
}

func (dc *FakeIDeliveryContext) Discard(ctx context.Context, e *amqp.Error) error {
	return nil
}

func (dc *FakeIDeliveryContext) DiscardWithAnnotations(ctx context.Context, annotations amqp.Annotations) error {
	return nil
}
func (dc *FakeIDeliveryContext) Requeue(ctx context.Context) error {
	dc.requeued = true
	return nil
}
func (dc *FakeIDeliveryContext) RequeueWithAnnotations(ctx context.Context, annotations amqp.Annotations) error {
	return nil
}
func (dc *FakeIDeliveryContext) RequeueWithAnnotationsAndDeliveryFailed(ctx context.Context, annotations amqp.Annotations, deliveryFailed bool) error {
	return nil
}
func (dc *FakeIDeliveryContext) DelayRetry(ctx context.Context, delay time.Duration, deliveryFailed bool) error {
	return nil
}

// TestRatingHandler 测试RatingHandler的运行逻辑是否正确
func TestRatingHandler(t *testing.T) {
	likeBody, _ := json.Marshal(video.RatingEvent{
		EventID:   "",
		Action:    "like",
		AccountID: uint(1),
		VideoID:   uint(1),
	})

	tests := []struct {
		name          string                  // 测试名称
		body          []byte                  // 测试数据
		setup         func(*MockRatingSetter) //设置
		wantErr       bool                    //预期
		wantAccepted  bool                    //预期
		wantRequeued  bool                    //预期
		wantSetRating bool                    //预期
	}{
		{"合法like", likeBody, func(m *MockRatingSetter) { m.On("SetRating", mock.Anything, uint(1), uint(1), int8(1)).Return(nil) }, false, true, false, true},
		{"JSON非法", []byte("not-json"), func(m *MockRatingSetter) {}, false, true, false, false},
		{"action非法", []byte(`{"action":"praise","account_id":1,"video_id":3}`), func(m *MockRatingSetter) {}, false, true, false, false},
		{"SetRating失败 → Requeue", likeBody, func(m *MockRatingSetter) {
			m.On("SetRating", mock.Anything, uint(1), uint(1), int8(1)).Return(errors.New("db down"))
		},
			true, false, true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := new(MockRatingSetter)
			tt.setup(repo)
			dc := &FakeIDeliveryContext{msg: amqp.NewMessage(tt.body)}

			err := RatingHandler(repo)(context.Background(), dc)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.wantAccepted, dc.accepted) // ← 断言 settle
			require.Equal(t, tt.wantRequeued, dc.requeued)

			if tt.wantSetRating {
				repo.AssertExpectations(t)
			} else {
				repo.AssertNotCalled(t, "SetRating")
			}
			repo.AssertExpectations(t)
		})
	}
}
