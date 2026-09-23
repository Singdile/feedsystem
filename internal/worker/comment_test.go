package worker

import (
	"context"
	"encoding/json"
	"errors"
	"feedsystem/internal/model/video"
	"testing"

	"github.com/Azure/go-amqp"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type MockCommentWriter struct {
	mock.Mock
}

func (m *MockCommentWriter) Create(ctx context.Context, c *video.Comment) error {
	args := m.Called(ctx, c)
	return args.Error(0)
}
func (m *MockCommentWriter) Delete(ctx context.Context, commentID uint) error {
	args := m.Called(ctx, commentID)
	return args.Error(0)
}

// TestCommentHandle 测试CommentHandle的处理是否正确
// commentHandle 从mq中取出数据，到数据库中执行操作
func TestCommentHandle(t *testing.T) {

	commentEvent := []video.CommentEvent{
		{
			EventID:   "1",
			Action:    "publish",
			VideoID:   uint(1),
			AuthorID:  uint(1),
			AccountID: uint(1),
			UserName:  "user1",
			Content:   "content1",
		},
		{
			EventID:   "2",
			Action:    "delete",
			CommentID: uint(2),
			VideoID:   uint(1),
			AuthorID:  uint(1),
			AccountID: uint(1),
			UserName:  "user2",
			Content:   "content2",
		},
		{
			EventID:   "3",
			Action:    "random comment",
			CommentID: uint(2),
			VideoID:   uint(1),
			AuthorID:  uint(1),
			AccountID: uint(1),
			UserName:  "user3",
			Content:   "content3",
		},
	}

	bodys := [][]byte{}
	for _, event := range commentEvent {
		body, _ := json.Marshal(&event)
		bodys = append(bodys, body)
	}

	tests := []struct {
		Name         string
		body         []byte                          // 测试数据
		setup        func(writer *MockCommentWriter) //设置
		wantErr      bool
		wantAccepted bool //预期
		wantRequeued bool //预期
	}{
		{
			Name: "success publish",
			body: bodys[0],
			setup: func(writer *MockCommentWriter) {
				writer.On("Create", mock.Anything, mock.Anything).Return(nil)
			},
			wantErr:      false,
			wantAccepted: true,
			wantRequeued: false,
		},
		{
			Name: "success delete",
			body: bodys[1],
			setup: func(writer *MockCommentWriter) {
				writer.On("Delete", mock.Anything, uint(2)).Return(nil)
			},
			wantErr:      false,
			wantAccepted: true,
			wantRequeued: false,
		},
		{
			Name: "fail publish",
			body: bodys[0],
			setup: func(writer *MockCommentWriter) {
				writer.On("Create", mock.Anything, mock.Anything).Return(errors.New("publish failed"))
			},
			wantErr:      true,
			wantAccepted: false,
			wantRequeued: true,
		},
		{
			Name: "fail delete",
			body: bodys[1],
			setup: func(writer *MockCommentWriter) {
				writer.On("Delete", mock.Anything, uint(2)).Return(errors.New("delete failed"))
			},
			wantErr:      true,
			wantAccepted: false,
			wantRequeued: true,
		},
		{
			Name: "fail delete cuz wrong data",
			body: []byte("wrong data"),
			setup: func(writer *MockCommentWriter) {
				writer.On("Delete", mock.Anything, uint(2)).Return(errors.New("delete failed"))
			},
			wantErr:      true,
			wantAccepted: true,
			wantRequeued: false,
		},
		{
			Name: "fail publish cuz wrong data",
			body: []byte("wrong data"),
			setup: func(writer *MockCommentWriter) {
				writer.On("Create", mock.Anything, mock.Anything).Return(errors.New("publish failed"))
			},
			wantErr:      true,
			wantAccepted: true,
			wantRequeued: false,
		},
		{
			Name:         "unkown action",
			body:         bodys[2],
			wantErr:      true,
			wantAccepted: true,
			wantRequeued: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			mockRepo := MockCommentWriter{}
			if tt.setup != nil {
				tt.setup(&mockRepo)
			}
			dc := &FakeIDeliveryContext{msg: amqp.NewMessage(tt.body)}
			err := CommentHandler(&mockRepo)(context.Background(), dc)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.wantAccepted, dc.accepted)
			require.Equal(t, tt.wantRequeued, dc.requeued)
		})
	}
}
