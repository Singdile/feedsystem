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
	publishEvent := video.CommentEvent{
		EventID:   "evt-comment-publish",
		Action:    "publish",
		VideoID:   1,
		AuthorID:  1,
		AccountID: 1,
		UserName:  "user1",
		Content:   "content1",
	}

	deleteEvent := video.CommentEvent{
		EventID:   "evt-comment-delete",
		Action:    "delete",
		CommentID: 2,
		VideoID:   1,
		AuthorID:  1,
		AccountID: 1,
		UserName:  "user2",
		Content:   "content2",
	}

	unknownActionEvent := video.CommentEvent{
		EventID:   "evt-comment-unknown",
		Action:    "random comment",
		CommentID: 2,
		VideoID:   1,
		AuthorID:  1,
		AccountID: 1,
		UserName:  "user3",
		Content:   "content3",
	}

	tests := []struct {
		Name         string
		msg          *fakeMessage
		setup        func(writer *MockCommentWriter) // 设置
		wantErr      bool
		wantAccepted bool // 预期
	}{
		{
			Name: "success publish",
			msg:  envelopeMessage(t, messaging.EventTypeComment, publishEvent),
			setup: func(writer *MockCommentWriter) {
				writer.On("Create", mock.Anything, mock.Anything).Return(nil)
			},
			wantAccepted: true,
		},
		{
			Name: "success delete",
			msg:  envelopeMessage(t, messaging.EventTypeComment, deleteEvent),
			setup: func(writer *MockCommentWriter) {
				writer.On("Delete", mock.Anything, uint(2)).Return(nil)
			},
			wantAccepted: true,
		},
		{
			Name: "fail publish",
			msg:  envelopeMessage(t, messaging.EventTypeComment, publishEvent),
			setup: func(writer *MockCommentWriter) {
				writer.On("Create", mock.Anything, mock.Anything).Return(errors.New("publish failed"))
			},
			wantAccepted: true,
		},
		{
			Name: "fail delete",
			msg:  envelopeMessage(t, messaging.EventTypeComment, deleteEvent),
			setup: func(writer *MockCommentWriter) {
				writer.On("Delete", mock.Anything, uint(2)).Return(errors.New("delete failed"))
			},
			wantAccepted: true,
		},
		{
			Name:         "非法 JSON",
			msg:          rawMessage([]byte("wrong data")),
			wantErr:      true,
			wantAccepted: true,
		},
		{
			Name:         "事件类型不匹配",
			msg:          envelopeMessage(t, messaging.EventTypeRating, publishEvent),
			wantErr:      true,
			wantAccepted: true,
		},
		{
			Name:         "unkown action",
			msg:          envelopeMessage(t, messaging.EventTypeComment, unknownActionEvent),
			wantErr:      true,
			wantAccepted: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			mockRepo := MockCommentWriter{}
			if tt.setup != nil {
				tt.setup(&mockRepo)
			}

			err := CommentHandler(&mockRepo)(context.Background(), tt.msg)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.wantAccepted, tt.msg.accepted)

			if tt.setup != nil {
				mockRepo.AssertExpectations(t)
			} else {
				mockRepo.AssertNotCalled(t, "Create")
				mockRepo.AssertNotCalled(t, "Delete")
			}
		})
	}
}
