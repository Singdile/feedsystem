package worker

import (
	"context"
	"errors"
	"testing"

	"feedsystem/internal/messaging"
	"feedsystem/internal/model/video"

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

// TestRatingHandler 测试RatingHandler的运行逻辑是否正确
func TestRatingHandler(t *testing.T) {
	likeEvent := video.RatingEvent{
		EventID:   "evt-rating-like",
		Action:    "like",
		AccountID: 1,
		VideoID:   1,
	}

	tests := []struct {
		name          string                  // 测试名称
		msg           *fakeMessage            // 测试消息
		setup         func(*MockRatingSetter) // 设置
		wantErr       bool                    // 是否期望 handler 返回 error
		wantAccepted  bool                    // 是否期望消息被 settle
		wantSetRating bool                    // 是否期望调用 SetRating
	}{
		{
			name: "合法 like",
			msg:  envelopeMessage(t, messaging.EventTypeRating, likeEvent),
			setup: func(m *MockRatingSetter) {
				m.On("SetRating", mock.Anything, uint(1), uint(1), int8(1)).Return(nil)
			},
			wantAccepted:  true,
			wantSetRating: true,
		},
		{
			name:         "非法 JSON",
			msg:          rawMessage([]byte("not-json")),
			wantErr:      true,
			wantAccepted: true,
		},
		{
			name:         "事件类型不匹配",
			msg:          envelopeMessage(t, messaging.EventTypeComment, likeEvent),
			wantErr:      true,
			wantAccepted: true,
		},
		{
			name: "action 非法",
			msg: envelopeMessage(t, messaging.EventTypeRating, video.RatingEvent{
				EventID:   "evt-rating-bad",
				Action:    "praise",
				AccountID: 1,
				VideoID:   3,
			}),
			wantAccepted: true,
		},
		{
			name: "SetRating 失败 → 重试后 Accept 丢弃",
			msg:  envelopeMessage(t, messaging.EventTypeRating, likeEvent),
			setup: func(m *MockRatingSetter) {
				m.On("SetRating", mock.Anything, uint(1), uint(1), int8(1)).Return(errors.New("db down"))
			},
			wantAccepted:  true,
			wantSetRating: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := new(MockRatingSetter)
			if tt.setup != nil {
				tt.setup(repo)
			}

			err := RatingHandler(repo)(context.Background(), tt.msg)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.wantAccepted, tt.msg.accepted) // ← 断言 settle

			if tt.wantSetRating {
				repo.AssertExpectations(t)
			} else {
				repo.AssertNotCalled(t, "SetRating")
			}
		})
	}
}
