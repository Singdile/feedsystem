package worker

import (
	"context"
	"encoding/json"
	"errors"
	"feedsystem/internal/messaging"
	"feedsystem/internal/model/video"
	"testing"

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

// fakeMessage 是 messaging.Message 的测试实现，用来替代真实的 AMQP delivery
type fakeMessage struct {
	body     []byte
	accepted bool
}

func (m *fakeMessage) Body() []byte { return m.body }

func (m *fakeMessage) Accept(ctx context.Context) error {
	m.accepted = true
	return nil
}

var _ messaging.Message = (*fakeMessage)(nil)

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
		wantSetRating bool                    //预期
	}{
		{"合法like", likeBody, func(m *MockRatingSetter) { m.On("SetRating", mock.Anything, uint(1), uint(1), int8(1)).Return(nil) }, false, true, true},
		{"JSON非法", []byte("not-json"), func(m *MockRatingSetter) {}, false, true, false},
		{"action非法", []byte(`{"action":"praise","account_id":1,"video_id":3}`), func(m *MockRatingSetter) {}, false, true, false},
		{"SetRating失败 → 重试3次后Accept丢弃", likeBody, func(m *MockRatingSetter) {
			m.On("SetRating", mock.Anything, uint(1), uint(1), int8(1)).Return(errors.New("db down"))
		},
			false, true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := new(MockRatingSetter)
			tt.setup(repo)
			msg := &fakeMessage{body: tt.body}

			err := RatingHandler(repo)(context.Background(), msg)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.wantAccepted, msg.accepted) // ← 断言 settle

			if tt.wantSetRating {
				repo.AssertExpectations(t)
			} else {
				repo.AssertNotCalled(t, "SetRating")
			}
			repo.AssertExpectations(t)
		})
	}
}
