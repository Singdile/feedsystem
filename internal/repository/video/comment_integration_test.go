package video

import (
	"feedsystem/internal/model/video"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func seedComment(db *gorm.DB, comment video.Comment) error {
	return db.Create(&comment).Error
}

func TestCommentRepo_Create(t *testing.T) {
	// comments
	comments := []video.Comment{
		{
			VideoID:   1,
			AuthorID:  1,
			AccountID: 1,
			Content:   "test comment fail",
			UserName:  "test",
		},
		{
			VideoID:   1,
			AuthorID:  1,
			AccountID: 1,
			Content:   "test comment success",
			UserName:  "test",
		},
		{
			VideoID:   1,
			AuthorID:  1,
			AccountID: 1,
			Content:   "",
			UserName:  "test",
		},
	}

	tests := []struct {
		Name    string
		Comment *video.Comment
		WantErr bool
	}{
		{
			Name:    "success insert and read back",
			Comment: &comments[0],
			WantErr: false,
		},
		{
			Name:    "success when comment has not existed",
			Comment: &comments[1],
			WantErr: false,
		},
		{
			Name:    "fail when content is empty",
			Comment: &comments[2],
			WantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			db := newTestDB(t)
			repo := NewCommentRepo(db)

			err := repo.Create(t.Context(), tt.Comment) //db create 会回填主键id的

			if tt.WantErr {
				require.Error(t, err)
				return
			} else {
				require.NoError(t, err)
			}

			// 回读断言：真的存进去了
			var saved video.Comment
			require.NoError(t, db.First(&saved, tt.Comment.ID).Error)
			require.Equal(t, tt.Comment.VideoID, saved.VideoID)
			require.Equal(t, tt.Comment.AuthorID, saved.AuthorID)
			require.Equal(t, tt.Comment.AccountID, saved.AccountID)
			require.Equal(t, tt.Comment.UserName, saved.UserName)
			require.Equal(t, tt.Comment.Content, saved.Content)
			require.False(t, saved.CreatedAt.IsZero())
		})
	}
}

func TestCommentRepo_ListByVideoID(t *testing.T) {
	db := newTestDB(t)

	// video1 3 条评论、video2 2 条评论（用于隔离验证）
	v1 := []*video.Comment{
		{VideoID: 1, AuthorID: 1, AccountID: 1, Content: "comment-1", UserName: "test"},
		{VideoID: 1, AuthorID: 1, AccountID: 2, Content: "comment-2", UserName: "test"},
		{VideoID: 1, AuthorID: 1, AccountID: 3, Content: "comment-3", UserName: "test"},
	}
	v2 := []*video.Comment{
		{VideoID: 2, AuthorID: 1, AccountID: 1, Content: "video2-1", UserName: "test"},
		{VideoID: 2, AuthorID: 1, AccountID: 2, Content: "video2-2", UserName: "test"},
	}
	for _, c := range v1 {
		require.NoError(t, db.Create(c).Error) // db.Create 会回填 ID/CreatedAt
	}
	for _, c := range v2 {
		require.NoError(t, db.Create(c).Error)
	}

	repo := NewCommentRepo(db)

	// 首页：DESC 排序（最新在前）+ limit 生效
	t.Run("首页 DESC 排序 + limit", func(t *testing.T) {
		got, err := repo.ListByVideoID(t.Context(), 1, nil, 2)
		require.NoError(t, err)
		require.Len(t, got, 2)
		require.Equal(t, v1[2].ID, got[0].ID) // 最新（id 最大）在前
		require.Equal(t, v1[1].ID, got[1].ID)
	})

	// 游标翻页：上一页最后一条 (created_at,id) 之后，返回更早的
	t.Run("cursor 翻页", func(t *testing.T) {
		cursor := &video.Cursor{CreatedAt: v1[1].CreatedAt, ID: v1[1].ID}
		got, err := repo.ListByVideoID(t.Context(), 1, cursor, 10)
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.Equal(t, v1[0].ID, got[0].ID)
	})

	// 多视频隔离：video1 查询结果只含 video1 的评论
	t.Run("多视频隔离", func(t *testing.T) {
		got, err := repo.ListByVideoID(t.Context(), 1, nil, 100)
		require.NoError(t, err)
		require.Len(t, got, 3)
		for _, c := range got {
			require.Equal(t, uint(1), c.VideoID)
		}
	})

	// 不存在视频返回空
	t.Run("不存在视频返回空", func(t *testing.T) {
		got, err := repo.ListByVideoID(t.Context(), 99, nil, 10)
		require.NoError(t, err)
		require.Len(t, got, 0)
	})
}
