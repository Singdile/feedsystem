package video

import (
	"feedsystem/internal/config"
	"feedsystem/internal/data"
	"feedsystem/internal/model/video"

	"net"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"
)

func newTestRedis(t *testing.T) (*data.RedisClient, *miniredis.Miniredis) {
	t.Helper()

	mr := miniredis.RunT(t)
	host, portStr, _ := net.SplitHostPort(mr.Addr())

	port, _ := strconv.Atoi(portStr)
	cache, err := data.NewRedis(config.RedisConfig{
		Host: host,
		Port: port,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = cache.Close()
	})

	return cache, mr
}

// 用户设置视频评价，会使得redis存储的实体失效
func TestRatingRepo_SetRating_InvalidateCache(t *testing.T) {
	// 准备数据
	db := newTestDB(t)           // 连接测试数据库
	cache, mr := newTestRedis(t) // 连接测试cache
	repo := NewRatingRepo(db, cache)

	// 向数据库以及cache中插入数据
	seedVideo(t, db, video.Video{
		ID:       1,
		AuthorID: 1,
	})

	key := cache.Key("video:entity:%d", uint(1))
	_, err := cache.Set(t.Context(), key, "stale_entity", time.Hour)
	require.NoError(t, err)
	require.True(t, mr.Exists(key))

	// 执行用户对视频点赞
	require.NoError(t, repo.SetRating(t.Context(), uint(1), uint(1), 1, 1))

	// redis中的实体被删除了
	require.False(t, mr.Exists(key))
}

// 重复rating，不删除缓存
func TestRatingRepo_SetRating_NoChange(t *testing.T) {
	// 准备数据
	db := newTestDB(t)           // 连接测试数据库
	cache, mr := newTestRedis(t) // 连接测试cache
	repo := NewRatingRepo(db, cache)

	// 向数据库以及cache中插入数据
	seedVideo(t, db, video.Video{
		ID:       1,
		AuthorID: 1,
	})

	key := cache.Key("video:entity:%d", uint(1))
	_, err := cache.Set(t.Context(), key, "stale_entity", time.Hour)
	require.NoError(t, err)
	require.True(t, mr.Exists(key))

	// 执行用户对视频点赞
	require.NoError(t, repo.SetRating(t.Context(), uint(1), uint(1), 1, 1))
	// redis中的实体被删除了
	require.False(t, mr.Exists(key))

	// 重新cache 插入
	_, err = cache.Set(t.Context(), key, "stale_entity", time.Hour)
	require.NoError(t, err)
	require.True(t, mr.Exists(key))

	// 再次执行相同的rating 操作，应该不影响cache
	require.NoError(t, repo.SetRating(t.Context(), uint(1), uint(1), 1, 2))

	// cache 中的实体不应该被删除
	require.True(t, mr.Exists(key))
}

// 旧版本事件（乱序/重复）不应改动计数，也不应失效缓存
func TestRatingRepo_SetRating_StaleVersionKeepsCache(t *testing.T) {
	db := newTestDB(t)
	cache, mr := newTestRedis(t)
	repo := NewRatingRepo(db, cache)

	seedVideo(t, db, video.Video{ID: 1, AuthorID: 1})

	// 先应用 version=2 的点赞
	require.NoError(t, repo.SetRating(t.Context(), uint(1), uint(1), 1, 2))

	key := cache.Key("video:entity:%d", uint(1))
	_, err := cache.Set(t.Context(), key, "stale_entity", time.Hour)
	require.NoError(t, err)
	require.True(t, mr.Exists(key))

	// 再来一条 version=1 的旧事件：应被跳过
	require.NoError(t, repo.SetRating(t.Context(), uint(1), uint(1), -1, 1))
	require.True(t, mr.Exists(key))

	// 状态仍为点赞
	var vr video.VideoRating
	require.NoError(t, db.Where("video_id = ? AND account_id = ?", uint(1), uint(1)).First(&vr).Error)
	require.Equal(t, video.StatusLike, vr.Status)
	require.Equal(t, int64(2), vr.Version)

	var v video.Video
	require.NoError(t, db.First(&v, uint(1)).Error)
	require.Equal(t, uint(1), v.LikedCount)
	require.Equal(t, uint(0), v.DislikedCount)
}

// 评论创建，删除 会使得cache中的视频实体缓存失效
func TestCommentRepo_Create_InvalidateCache(t *testing.T) {
	// 准备数据
	db := newTestDB(t)
	cache, mr := newTestRedis(t)
	repo := NewCommentRepo(db, cache)

	seedVideo(t, db, video.Video{
		ID:       1,
		AuthorID: 1,
	})

	key := cache.Key("video:entity:%d", uint(1))
	_, err := cache.Set(t.Context(), key, "stale_entity", time.Hour)
	require.NoError(t, err)
	require.True(t, mr.Exists(key))

	// 执行comment 创建操作
	err = repo.Create(t.Context(), &video.Comment{
		ID:        1,
		VideoID:   1,
		AuthorID:  1,
		AccountID: 1,
		Content:   "sdfsadf",
	})

	require.NoError(t, err)
	require.False(t, mr.Exists(key))

	// 执行comment 删除操作
	_, err = cache.Set(t.Context(), key, "stale_entity", time.Hour)
	require.NoError(t, err)
	require.True(t, mr.Exists(key))

	err = repo.Delete(t.Context(), uint(1))
	require.NoError(t, err)
	require.False(t, mr.Exists(key))

}
