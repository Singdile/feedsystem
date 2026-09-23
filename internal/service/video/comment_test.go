package video

import (
	"context"
	"feedsystem/internal/model/video"
	apperrors "feedsystem/internal/pkg/errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type MockCommentRepo struct {
	mock.Mock
}

func (m *MockCommentRepo) Create(ctx context.Context, c *video.Comment) error {
	args := m.Called(ctx, c)
	return args.Error(0)
}
func (m *MockCommentRepo) Delete(ctx context.Context, commentID uint) error {
	args := m.Called(ctx, commentID)
	return args.Error(0)
}
func (m *MockCommentRepo) Update(ctx context.Context, c *video.Comment) error {
	args := m.Called(ctx, c)
	return args.Error(0)
}

func (m *MockCommentRepo) GetByID(ctx context.Context, commentID uint) (*video.Comment, error) {
	args := m.Called(ctx, commentID)
	c, _ := args.Get(0).(*video.Comment)
	return c, args.Error(1)
}

func (m *MockCommentRepo) ListByVideoID(ctx context.Context, videoID uint, cursor *video.Cursor, limit int8) ([]*video.Comment, error) {
	args := m.Called(ctx, videoID, cursor, limit)
	comments := args.Get(0).([]*video.Comment)
	return comments, args.Error(1)
}

type MockCommentMQ struct {
	mock.Mock
}

func (m *MockCommentMQ) Publish(ctx context.Context, c *video.Comment) error {
	args := m.Called(ctx, c)
	return args.Error(0)
}
func (m *MockCommentMQ) Delete(ctx context.Context, commentID uint) error {
	args := m.Called(ctx, commentID)
	return args.Error(0)
}

type MockVideoChecker struct {
	mock.Mock
}

func (m *MockVideoChecker) FindByID(ctx context.Context, videoID uint) (*video.Video, error) {
	args := m.Called(ctx, videoID)
	v, _ := args.Get(0).(*video.Video)
	return v, args.Error(1)
}

// TestPublish 测试发布评论
func TestPublishComment(t *testing.T) {
	// 准备测试材料
	tests := []struct {
		Name      string
		VideoID   uint
		AuthorID  uint
		AccountID uint
		UserName  string
		Content   string
		WantErr   bool
		ErrCode   int
		Setup     func(*MockCommentRepo, *MockCommentMQ, *MockVideoChecker)
	}{
		{
			Name:      "成功走MQ",
			VideoID:   1,
			AuthorID:  1,
			AccountID: 1,
			UserName:  "user1",
			Content:   "成功走MQ content",
			WantErr:   false,
			ErrCode:   0,
			Setup: func(m *MockCommentRepo, c *MockCommentMQ, v *MockVideoChecker) {
				v.On("FindByID", mock.Anything, uint(1)).Return(&video.Video{ID: uint(1), AuthorID: uint(1)}, nil)
				c.On("Publish", mock.Anything, &video.Comment{VideoID: uint(1), AuthorID: uint(1), AccountID: uint(1), UserName: "user1", Content: "成功走MQ content"}).Return(nil)
			},
		},
		{
			Name:      "走MQ失败,降级DB成功",
			VideoID:   2,
			AuthorID:  2,
			AccountID: 2,
			UserName:  "user2",
			Content:   "走MQ失败,降级DB成功",
			WantErr:   false,
			ErrCode:   0,
			Setup: func(m *MockCommentRepo, c *MockCommentMQ, v *MockVideoChecker) {
				v.On("FindByID", mock.Anything, uint(2)).Return(&video.Video{ID: uint(1), AuthorID: uint(2)}, nil)
				c.On("Publish", mock.Anything, mock.Anything).Return(apperrors.NewAppError(http.StatusInternalServerError, "MQ投递失败"))
				m.On("Create", mock.Anything, mock.Anything).Return(nil)
			},
		},
		{
			Name:      "走MQ失败，走DB失败",
			VideoID:   3,
			AuthorID:  3,
			AccountID: 3,
			UserName:  "user3",
			Content:   "走MQ失败，走DB失败",
			WantErr:   true,
			ErrCode:   500,
			Setup: func(m *MockCommentRepo, c *MockCommentMQ, v *MockVideoChecker) {
				v.On("FindByID", mock.Anything, uint(3)).Return(&video.Video{ID: uint(3), AuthorID: uint(3)}, nil)
				c.On("Publish", mock.Anything, mock.Anything).Return(apperrors.NewAppError(http.StatusInternalServerError, "MQ投递失败"))
				m.On("Create", mock.Anything, mock.Anything).Return(apperrors.NewAppError(http.StatusInternalServerError, "创建失败"))
			},
		},
		{
			Name:      "参数校验错误-videoID 0 ",
			VideoID:   0,
			AuthorID:  4,
			AccountID: 4,
			UserName:  "user4",
			Content:   "参数校验错误-videoID 0",
			WantErr:   true,
			ErrCode:   400,
		},
		{
			Name:      "参数校验错误-accountID 0 ",
			VideoID:   4,
			AuthorID:  5,
			AccountID: 0,
			UserName:  "user4",
			Content:   "参数校验错误-accountID 0",
			WantErr:   true,
			ErrCode:   400,
		},
		{
			Name:      "参数校验错误-content 为空 ",
			VideoID:   4,
			AuthorID:  6,
			AccountID: 4,
			UserName:  "user4",
			Content:   "",
			WantErr:   true,
			ErrCode:   400,
		},
		{
			Name:      "查询对应视频不存在 ",
			VideoID:   5,
			AuthorID:  6,
			AccountID: 4,
			UserName:  "user4",
			Content:   "查询对应视频不存在 ",
			WantErr:   true,
			ErrCode:   400,
			Setup: func(m *MockCommentRepo, c *MockCommentMQ, v *MockVideoChecker) {
				v.On("FindByID", mock.Anything, uint(5)).Return(nil, gorm.ErrRecordNotFound)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			commentRepo := &MockCommentRepo{}
			commentMQ := &MockCommentMQ{}
			videoChecker := &MockVideoChecker{}
			svc := NewCommentService(commentRepo, commentMQ, videoChecker)

			if tt.Setup != nil {
				tt.Setup(commentRepo, commentMQ, videoChecker)
			}

			// 调用者输入 videoID,accountID,userName,content,返回error
			err := svc.Publish(t.Context(), tt.VideoID, tt.AccountID, tt.UserName, tt.Content)

			if tt.WantErr {
				require.Error(t, err)
				require.Equal(t, tt.ErrCode, err.(*apperrors.AppError).Status)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestDeleteComment 测试删除评论
// 视频发布者可以任意删除自己视频的任何评论；评论者只能删除自己发送的评论
func TestDeleteComment(t *testing.T) {
	tests := []struct {
		Name      string
		AccountID uint
		CommentID uint
		WantErr   bool
		ErrCode   int
		Setup     func(*MockCommentRepo, *MockCommentMQ)
	}{
		{
			Name:      "作者走MQ删除视频评论成功",
			AccountID: uint(1),
			CommentID: uint(1),
			WantErr:   false,
			ErrCode:   0,
			Setup: func(m *MockCommentRepo, c *MockCommentMQ) {
				m.On("GetByID", mock.Anything, uint(1)).Return(&video.Comment{ID: uint(1), AuthorID: 1, VideoID: uint(1), AccountID: uint(1)}, nil)
				c.On("Delete", mock.Anything, uint(1)).Return(nil)
			},
		},
		{
			Name:      "作者走MQ删除视频失败,走DB删除成功",
			AccountID: uint(1),
			CommentID: uint(1),
			WantErr:   false,
			ErrCode:   0,
			Setup: func(m *MockCommentRepo, c *MockCommentMQ) {
				c.On("Delete", mock.Anything, uint(1)).Return(apperrors.NewAppError(http.StatusInternalServerError, "mq失效"))
				m.On("Delete", mock.Anything, uint(1)).Return(nil)
				m.On("GetByID", mock.Anything, uint(1)).Return(&video.Comment{ID: uint(1), AuthorID: 1, VideoID: uint(1), AccountID: uint(1)}, nil)
			},
		},
		{
			Name:      "作者走MQ删除视频失败,走DB删除失败",
			AccountID: uint(1),
			CommentID: uint(1),
			WantErr:   true,
			ErrCode:   http.StatusInternalServerError,
			Setup: func(m *MockCommentRepo, c *MockCommentMQ) {
				m.On("GetByID", mock.Anything, uint(1)).Return(&video.Comment{ID: uint(1), AuthorID: 1, VideoID: uint(1), AccountID: uint(1)}, nil)
				c.On("Delete", mock.Anything, uint(1)).Return(apperrors.NewAppError(http.StatusInternalServerError, "mq失效"))
				m.On("Delete", mock.Anything, uint(1)).Return(apperrors.NewAppError(http.StatusInternalServerError, "删除失败"))
			},
		},
		{
			Name:      "用户删除视频评论失败",
			AccountID: uint(4),
			CommentID: uint(2),
			WantErr:   true,
			ErrCode:   http.StatusUnauthorized,
			Setup: func(m *MockCommentRepo, c *MockCommentMQ) {
				m.On("GetByID", mock.Anything, uint(2)).Return(&video.Comment{ID: uint(2), AuthorID: uint(1), VideoID: uint(2), AccountID: uint(3)}, nil)
			},
		},
		{
			Name:      "用户删除视频评论成功",
			AccountID: uint(4),
			CommentID: uint(2),
			WantErr:   false,
			ErrCode:   0,
			Setup: func(m *MockCommentRepo, c *MockCommentMQ) {
				m.On("GetByID", mock.Anything, uint(2)).Return(&video.Comment{ID: uint(2), AuthorID: uint(1), VideoID: uint(2), AccountID: uint(4)}, nil)
				c.On("Delete", mock.Anything, uint(2)).Return(nil)
			},
		},
		{
			Name:      "用户删除不存在的评论",
			AccountID: uint(4),
			CommentID: uint(2),
			WantErr:   true,
			ErrCode:   http.StatusBadRequest,
			Setup: func(m *MockCommentRepo, c *MockCommentMQ) {
				m.On("GetByID", mock.Anything, uint(2)).Return(nil, gorm.ErrRecordNotFound)
				c.On("Delete", mock.Anything, uint(2)).Return(nil)
			},
		},
	}

	// 优先发送删除事件到mq；mq失效的时候，直接DB删除
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			commentRepo := &MockCommentRepo{}
			commentMQ := &MockCommentMQ{}
			videoChecker := &MockVideoChecker{}
			svc := NewCommentService(commentRepo, commentMQ, videoChecker)
			if tt.Setup != nil {
				tt.Setup(commentRepo, commentMQ)
			}

			err := svc.Delete(t.Context(), tt.AccountID, tt.CommentID)

			if tt.WantErr {
				require.Error(t, err)
				require.Equal(t, tt.ErrCode, err.(*apperrors.AppError).Status)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestListComments 测试批量显示视频comments,并且支持游标滑动显示
func TestListComments(t *testing.T) {
	// 准备测试
	// 视频1 对应的comments
	now := time.Now()
	comments := []*video.Comment{
		&video.Comment{ID: uint(4), AuthorID: uint(1), VideoID: uint(1), AccountID: uint(4), CreatedAt: now.Add(time.Millisecond * 4)},
		&video.Comment{ID: uint(3), AuthorID: uint(1), VideoID: uint(1), AccountID: uint(3), CreatedAt: now.Add(time.Millisecond * 3)},
		&video.Comment{ID: uint(2), AuthorID: uint(1), VideoID: uint(1), AccountID: uint(2), CreatedAt: now.Add(time.Millisecond * 2)},
		&video.Comment{ID: uint(1), AuthorID: uint(1), VideoID: uint(1), AccountID: uint(1), CreatedAt: now.Add(time.Millisecond)},
	}

	tests := []struct {
		Name          string
		VideoID       uint
		CursorStr     string
		Limit         int8
		WantComment   []*video.Comment
		WantCursorStr string
		WantErr       bool
		ErrorCode     int
		Setup         func(m *MockCommentRepo)
	}{
		{
			Name:          "首次查询成功",
			VideoID:       uint(1),
			CursorStr:     "",
			Limit:         int8(2),
			WantComment:   comments[0:2],
			WantCursorStr: video.EncodeCursor(video.Cursor{ID: comments[1].ID, CreatedAt: comments[1].CreatedAt}),
			WantErr:       false,
			ErrorCode:     0,
			Setup: func(m *MockCommentRepo) {
				m.On("ListByVideoID", mock.Anything, uint(1), mock.Anything, int8(3)).Return(comments[0:3], nil)
			},
		},
		{
			Name:          "带游标查询",
			VideoID:       uint(1),
			CursorStr:     video.EncodeCursor(video.Cursor{ID: comments[1].ID, CreatedAt: comments[1].CreatedAt}),
			Limit:         int8(2),
			WantComment:   comments[2:],
			WantCursorStr: "",
			WantErr:       false,
			ErrorCode:     0,
			Setup: func(m *MockCommentRepo) {
				cursor, _ := video.DecodeCursor(video.EncodeCursor(video.Cursor{ID: comments[1].ID, CreatedAt: comments[1].CreatedAt}))
				m.On("ListByVideoID", mock.Anything, uint(1), &cursor, int8(3)).Return(comments[2:4], nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			commentRepo := &MockCommentRepo{}
			commentMQ := &MockCommentMQ{}
			videoChecker := &MockVideoChecker{}
			svc := NewCommentService(commentRepo, commentMQ, videoChecker)
			if tt.Setup != nil {
				tt.Setup(commentRepo)
			}

			// 输入videoID，cursorstr，limit，返回对应[]comments,cursorstr,error
			commentS, cursorStr, err := svc.ListComments(t.Context(), tt.VideoID, tt.CursorStr, tt.Limit)

			if tt.WantErr {
				require.Error(t, err)
				require.Equal(t, tt.ErrorCode, err.(*apperrors.AppError).Status)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.WantComment, commentS)
				require.Equal(t, tt.WantCursorStr, cursorStr)
			}

		})
	}

}
