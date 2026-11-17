package video

import (
	"feedsystem/internal/config"
	"feedsystem/internal/data"
	"feedsystem/internal/model/video"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// 测试ratingRepo 在真实 DB 上 SetRating 的行为是否正确

// newTestDB 建立测试使用的临时数据库
func newTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	base := config.DBConfig{
		User:     "root",
		Password: "123456",
		Host:     "localhost",
		Port:     3306,
		Dbname:   "mysql",
	}

	admin, err := data.NewDB(base) // 连接系统数据库
	if err != nil {
		t.Skipf("mysql 不可达，跳过集成测试:%v", err)
	}

	name := fmt.Sprintf("feedtest_rating_%d", time.Now().UnixNano())
	base.Dbname = name                                            // 临时库
	require.NoError(t, admin.Exec("CREATE DATABASE "+name).Error) //建立测试库
	data.CloseDB(admin)                                           // 关闭系统库的连接

	db, err := data.NewDB(base) // 临时库建立连接
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&video.Video{}, &video.VideoRating{}, &video.Comment{}))

	t.Cleanup(func() {
		data.CloseDB(db)
		if a, e := data.NewDB(base); e == nil {
			a.Exec("DROP DATABASE " + name)
			data.CloseDB(a)
		}
	})

	return db
}

// 插入预设的videos表中的行
func seedVideo(t *testing.T, db *gorm.DB, origin video.Video) {
	t.Helper()
	db.Model(&video.Video{}).Create(&origin)
}

// 插入预设的video_ratings表中的行
func seedVideoRatings(t *testing.T, db *gorm.DB, origin video.VideoRating) {
	t.Helper()
	if origin.Status == video.StatusNone {
		return
	}
	db.Model(&video.VideoRating{}).Create(&origin)
}

// TestSetRating_StateChange 测试SetRating 对于like count 和 dislike count的计算是否正确
func TestSetRating_StateChange(t *testing.T) {
	db := newTestDB(t) // 准备临时数据库表
	cache, _ := newTestRedis(t)
	repo := NewRatingRepo(db, cache) // repo 装配

	// 准备测试数据
	tests := []struct {
		Name              string            // 测试名称
		OriginVideo       video.Video       // 起始的video状态
		OriginVideoRating video.VideoRating //起始的用户rating状态
		WantVideo         video.Video       //预期的video状态
		WantVideoRating   video.VideoRating //预期的VideoRating状态
	}{
		{
			Name: "none->like",
			OriginVideo: video.Video{
				ID:            uint(1),
				AuthorID:      uint(1),
				LikedCount:    1,
				DislikedCount: 1,
			},
			OriginVideoRating: video.VideoRating{
				VideoID:   uint(1),
				AccountID: uint(1),
				Status:    int8(0),
			},
			WantVideo: video.Video{
				ID:            uint(1),
				AuthorID:      uint(1),
				LikedCount:    2,
				DislikedCount: 1,
			},
			WantVideoRating: video.VideoRating{
				VideoID:   uint(1),
				AccountID: uint(1),
				Status:    int8(1),
			},
		},
		{
			Name: "none->dislike",
			OriginVideo: video.Video{
				ID:            uint(2),
				AuthorID:      uint(2),
				LikedCount:    1,
				DislikedCount: 1,
			},
			OriginVideoRating: video.VideoRating{
				VideoID:   uint(2),
				AccountID: uint(2),
				Status:    int8(0),
			},
			WantVideo: video.Video{
				ID:            uint(2),
				AuthorID:      uint(2),
				LikedCount:    1,
				DislikedCount: 2,
			},
			WantVideoRating: video.VideoRating{
				VideoID:   uint(2),
				AccountID: uint(2),
				Status:    int8(-1),
			},
		},
		{
			Name: "like->none",
			OriginVideo: video.Video{
				ID:            uint(3),
				AuthorID:      uint(3),
				LikedCount:    1,
				DislikedCount: 1,
			},
			OriginVideoRating: video.VideoRating{
				VideoID:   uint(3),
				AccountID: uint(3),
				Status:    int8(1),
			},
			WantVideo: video.Video{
				ID:            uint(3),
				AuthorID:      uint(3),
				LikedCount:    0,
				DislikedCount: 1,
			},
			WantVideoRating: video.VideoRating{
				VideoID:   uint(3),
				AccountID: uint(3),
				Status:    int8(0),
			},
		},
		{
			Name: "like->dislike",
			OriginVideo: video.Video{
				ID:            uint(4),
				AuthorID:      uint(4),
				LikedCount:    1,
				DislikedCount: 1,
			},
			OriginVideoRating: video.VideoRating{
				VideoID:   uint(4),
				AccountID: uint(4),
				Status:    int8(1),
			},
			WantVideo: video.Video{
				ID:            uint(4),
				AuthorID:      uint(4),
				LikedCount:    0,
				DislikedCount: 2,
			},
			WantVideoRating: video.VideoRating{
				VideoID:   uint(4),
				AccountID: uint(4),
				Status:    int8(-1),
			},
		},
		{
			Name: "dislike->like",
			OriginVideo: video.Video{
				ID:            uint(5),
				AuthorID:      uint(5),
				LikedCount:    1,
				DislikedCount: 1,
			},
			OriginVideoRating: video.VideoRating{
				VideoID:   uint(5),
				AccountID: uint(5),
				Status:    int8(-1),
			},
			WantVideo: video.Video{
				ID:            uint(5),
				AuthorID:      uint(5),
				LikedCount:    2,
				DislikedCount: 0,
			},
			WantVideoRating: video.VideoRating{
				VideoID:   uint(5),
				AccountID: uint(5),
				Status:    int8(1),
			},
		},
		{
			Name: "dislike->none",
			OriginVideo: video.Video{
				ID:            uint(6),
				AuthorID:      uint(6),
				LikedCount:    1,
				DislikedCount: 1,
			},
			OriginVideoRating: video.VideoRating{
				VideoID:   uint(6),
				AccountID: uint(6),
				Status:    int8(-1),
			},
			WantVideo: video.Video{
				ID:            uint(6),
				AuthorID:      uint(6),
				LikedCount:    1,
				DislikedCount: 0,
			},
			WantVideoRating: video.VideoRating{
				VideoID:   uint(6),
				AccountID: uint(6),
				Status:    int8(0),
			},
		},
		{
			Name: "like->like",
			OriginVideo: video.Video{
				ID:            uint(7),
				AuthorID:      uint(7),
				LikedCount:    1,
				DislikedCount: 1,
			},
			OriginVideoRating: video.VideoRating{
				VideoID:   uint(7),
				AccountID: uint(7),
				Status:    int8(1),
			},
			WantVideo: video.Video{
				ID:            uint(7),
				AuthorID:      uint(7),
				LikedCount:    1,
				DislikedCount: 1,
			},
			WantVideoRating: video.VideoRating{
				VideoID:   uint(7),
				AccountID: uint(7),
				Status:    int8(1),
			},
		},
	}

	const wantVersion = int64(2)

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			// 首先先插入预设的videos表中的行,以及对应的video_ratings表中的行
			seedVideo(t, db, tt.OriginVideo)
			seedVideoRatings(t, db, tt.OriginVideoRating)
			// 执行操作
			err := repo.SetRating(t.Context(), tt.WantVideoRating.VideoID, tt.WantVideoRating.AccountID, tt.WantVideoRating.Status, wantVersion)
			require.NoError(t, err)

			// 对操作结果进行断言
			var v video.Video

			// 断言videos表数据的改动
			require.NoError(t, db.First(&v, tt.WantVideoRating.VideoID).Error)
			require.Equal(t, tt.WantVideo.LikedCount, v.LikedCount)
			require.Equal(t, tt.WantVideo.DislikedCount, v.DislikedCount)

			// 断言video_ratings 表数据的改动：取消评价保留 tombstone（status=0），不删除行
			var vr video.VideoRating
			require.NoError(t, db.Where("video_id = ? AND account_id = ?", tt.WantVideoRating.VideoID, tt.WantVideoRating.AccountID).First(&vr).Error)
			require.Equal(t, tt.WantVideoRating.Status, vr.Status)
			require.Equal(t, wantVersion, vr.Version)
		})
	}
}

// TestSetRating_VersionSemantics 验证乱序/重复/取消后迟到事件/并发写入的最终一致性
func TestSetRating_VersionSemantics(t *testing.T) {
	db := newTestDB(t)
	cache, _ := newTestRedis(t)
	repo := NewRatingRepo(db, cache)

	first := func(t *testing.T, videoID, accountID uint, status int8, version int64) {
		t.Helper()
		require.NoError(t, repo.SetRating(t.Context(), videoID, accountID, status, version))
	}

	assertRating := func(t *testing.T, videoID, accountID uint, wantStatus int8, wantVersion int64, wantLike, wantDislike uint) {
		t.Helper()

		var vr video.VideoRating
		require.NoError(t, db.Where("video_id = ? AND account_id = ?", videoID, accountID).First(&vr).Error)
		require.Equal(t, wantStatus, vr.Status)
		require.Equal(t, wantVersion, vr.Version)

		var v video.Video
		require.NoError(t, db.First(&v, videoID).Error)
		require.Equal(t, wantLike, v.LikedCount)
		require.Equal(t, wantDislike, v.DislikedCount)
	}

	t.Run("乱序事件只接受较新版本", func(t *testing.T) {
		seedVideo(t, db, video.Video{ID: 10, AuthorID: 10})

		first(t, 10, 10, video.StatusDislike, 2) // 用户最后一次操作
		first(t, 10, 10, video.StatusLike, 1)    // 迟到的旧事件，应被跳过

		assertRating(t, 10, 10, video.StatusDislike, 2, 0, 1)
	})

	t.Run("重复投递不重复计数", func(t *testing.T) {
		seedVideo(t, db, video.Video{ID: 11, AuthorID: 11})

		for i := 0; i < 3; i++ {
			first(t, 11, 11, video.StatusLike, 3)
		}

		assertRating(t, 11, 11, video.StatusLike, 3, 1, 0)
	})

	t.Run("取消评价后迟到的点赞不会复活", func(t *testing.T) {
		seedVideo(t, db, video.Video{ID: 12, AuthorID: 12})

		first(t, 12, 12, video.StatusLike, 4)
		first(t, 12, 12, video.StatusNone, 5) // 取消：写 tombstone
		first(t, 12, 12, video.StatusLike, 4) // 迟到的旧点赞

		assertRating(t, 12, 12, video.StatusNone, 5, 0, 0)
	})

	t.Run("并发写入最终收敛到较大版本", func(t *testing.T) {
		seedVideo(t, db, video.Video{ID: 13, AuthorID: 13})
		// 先建立 tombstone 行，避免并发首次插入触发 gap lock 竞争
		require.NoError(t, db.Create(&video.VideoRating{
			VideoID: 13, AccountID: 13, Status: video.StatusNone, Version: 0,
		}).Error)

		var wg sync.WaitGroup
		errs := make([]error, 2)
		wg.Add(2)
		go func() {
			defer wg.Done()
			errs[0] = repo.SetRating(t.Context(), 13, 13, video.StatusLike, 1)
		}()
		go func() {
			defer wg.Done()
			errs[1] = repo.SetRating(t.Context(), 13, 13, video.StatusDislike, 2)
		}()
		wg.Wait()

		require.NoError(t, errs[0])
		require.NoError(t, errs[1])
		assertRating(t, 13, 13, video.StatusDislike, 2, 0, 1)
	})
}

// TestNextRatingVersion_ReseedsFromDB 验证发号器丢失（TTL 过期 / Redis 重启）后从 DB 续号
func TestNextRatingVersion_ReseedsFromDB(t *testing.T) {
	db := newTestDB(t)
	cache, mr := newTestRedis(t)
	repo := NewRatingRepo(db, cache)

	key := cache.Key("rating:ver:%d:%d", uint(1), uint(1))

	v, err := repo.NextRatingVersion(t.Context(), 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(1), v)

	v, err = repo.NextRatingVersion(t.Context(), 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(2), v)

	// 模拟 TTL 过期 / Redis 丢数据：DB 已应用到 version=5
	require.NoError(t, db.Create(&video.VideoRating{
		VideoID: 1, AccountID: 1, Status: video.StatusLike, Version: 5,
	}).Error)
	mr.Del(key)

	v, err = repo.NextRatingVersion(t.Context(), 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(6), v, "发号器应基于 DB 的 version 续号，而不是从 1 重新开始")

	v, err = repo.NextRatingVersion(t.Context(), 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(7), v)
}
