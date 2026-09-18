package feed

import (
	"context"
	"encoding/json"
	"feedsystem/internal/config"
	"feedsystem/internal/data"
	"feedsystem/internal/model/feed"
	"feedsystem/internal/model/video"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockFeedRepo: zset 缓存相关的方法走 miniredis，ListLatest 模拟DB
type MockFeedRepo struct {
	mock.Mock
	cache *data.RedisClient
}

// 时间线操作,负责索引拿到显示vidoID
func (m *MockFeedRepo) ZRevRangeByScore(ctx context.Context, key string, maxScore float64, maxID uint, limit int) ([]feed.ZMember, error) {
	return m.cache.ZRevRangeByScore(ctx, key, maxScore, maxID, limit)
}
func (m *MockFeedRepo) ZRangeWithScores(ctx context.Context, key string, start, stop int64) ([]feed.ZMember, error) {
	return m.cache.ZRangeWithScores(ctx, key, start, stop)
}
func (m *MockFeedRepo) ZAdd(ctx context.Context, key string, member feed.ZMember) error {
	return m.cache.ZAdd(ctx, key, member)
}

// 实体缓存 (L2 redis)
func (m *MockFeedRepo) Key(format string, a ...any) string {
	return m.cache.Key(format, a...)
}
func (m *MockFeedRepo) MGet(ctx context.Context, keys ...string) ([]any, error) {
	return m.cache.MGet(ctx, keys...)
}
func (m *MockFeedRepo) GetBytes(ctx context.Context, key string) ([]byte, error) {
	return m.cache.GetBytes(ctx, key)
}
func (m *MockFeedRepo) SetBytes(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return m.cache.SetBytes(ctx, key, value, ttl)
}

// 冷数据查询  mock 的方法
func (m *MockFeedRepo) ListLatest(ctx context.Context, cursor *video.Cursor, limit int) ([]video.Video, error) {
	args := m.Called(ctx, cursor, limit)
	return args.Get(0).([]video.Video), args.Error(1)
}

type MockVideoProvider struct {
	mock.Mock
}

func (m *MockVideoProvider) ListByTag(ctx context.Context, tagName string, cursor *video.Cursor, limit int) ([]video.Video, error) {
	args := m.Called(ctx, tagName, cursor, limit)
	return args.Get(0).([]video.Video), args.Error(1)
}
func (m *MockVideoProvider) GetVideoEntitiesByIDs(ctx context.Context, ids []uint) ([]video.Video, error) {
	args := m.Called(ctx, ids)
	return args.Get(0).([]video.Video), args.Error(1)
}
func (m *MockVideoProvider) BuildViews(ctx context.Context, vs []video.Video) ([]video.VideoView, error) {
	args := m.Called(ctx, vs)
	return args.Get(0).([]video.VideoView), args.Error(1)
}

func newTestCache(t *testing.T) (*data.RedisClient, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t) //启动miniredis，测试结束之后，自动关闭 server
	host, portStr, err := net.SplitHostPort(mr.Addr())
	require.NoError(t, err)
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)
	cache, err := data.NewRedis(config.RedisConfig{
		Host: host,
		Port: port,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() }) // close client connect
	return cache, mr
}

// TestListFeed_EmptyCacheANDDB cache 和 db 都为空，查询应该返回空的item，而不是错误
func TestListFeed_EmptyCacheANDDB(t *testing.T) {
	// 准备
	cache, _ := newTestCache(t)
	repo := &MockFeedRepo{cache: cache}
	videoProvider := &MockVideoProvider{}

	// 规定行为
	// On 指定特定的函数，当该函数被调用且输入参数匹配的时候，则会返回Return 指定的值
	repo.On("ListLatest", mock.Anything, mock.Anything, 1000).Return([]video.Video{}, nil)

	// 执行
	svc := NewFeedService(repo, videoProvider)
	res, err := svc.ListFeed(context.Background(), "", 10)

	// 断言 参数匹配的特定函数被执行过了
	repo.AssertExpectations(t)

	// 断言,假如有bug，那么输出就会和我们预想的不一样
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Empty(t, res.Items) // 长度为0
	require.Equal(t, "", res.NextCursor)
}

// TestListFeed_DBFallback 测试redis失效，DB兜底
func TestListFeed_DBFallback(t *testing.T) {
	// 准备
	cache, mr := newTestCache(t)
	repo := &MockFeedRepo{cache: cache}
	videoProvider := &MockVideoProvider{}

	// 模拟redis 失效
	mr.Close() //直接关闭server

	// 规定行为
	dbVideos := []video.Video{
		{ID: 1, Title: "V1"},
		{ID: 2, Title: "V2"},
	}
	dbViews := []video.VideoView{
		{ID: 1, Title: "V1"},
		{ID: 2, Title: "V2"},
	}

	repo.On("ListLatest", mock.Anything, mock.Anything, 11).Return(dbVideos, nil)
	videoProvider.On("BuildViews", mock.Anything, dbVideos).Return(dbViews, nil)
	// 执行
	svc := NewFeedService(repo, videoProvider)
	res, err := svc.ListFeed(context.Background(), "", 10)

	// 断言
	repo.AssertExpectations(t)
	videoProvider.AssertExpectations(t)
	require.NoError(t, err)
	require.Len(t, res.Items, len(dbVideos))
	require.Equal(t, "", res.NextCursor) //没有更多了
}

// TestListFeed_Rebuild 测试redis为空，从DB重建的的功能
func TestListFeed_Rebuild(t *testing.T) {
	cache, _ := newTestCache(t)
	repo := &MockFeedRepo{cache: cache}
	videoProvider := &MockVideoProvider{}
	ctx := context.Background()

	// DB 有 2 条，ZSET 为空
	v1 := video.Video{ID: 1, Title: "V1", CreatedAt: time.Now().Add(-2 * time.Second)}
	v2 := video.Video{ID: 2, Title: "V2", CreatedAt: time.Now().Add(-time.Second)}

	// 重建：ListLatest(nil, 1000) 返回 2 条 → ZAdd 回填
	repo.On("ListLatest", mock.Anything, mock.Anything, 1000).Return([]video.Video{v1, v2}, nil).Once()

	// 重建后递归热路径：per-id L3 水合 + BuildViews
	videoProvider.On("GetVideoEntitiesByIDs", mock.Anything, []uint{2}).Return([]video.Video{v2}, nil).Once()
	videoProvider.On("GetVideoEntitiesByIDs", mock.Anything, []uint{1}).Return([]video.Video{v1}, nil).Once()
	videoProvider.On("BuildViews", mock.Anything, mock.Anything).Return([]video.VideoView{
		{ID: 2, Title: "V2", CreatedAt: v2.CreatedAt},
		{ID: 1, Title: "V1", CreatedAt: v1.CreatedAt},
	}, nil).Once()

	svc := NewFeedService(repo, videoProvider)
	res, err := svc.ListFeed(ctx, "", 2)

	require.NoError(t, err)
	require.Len(t, res.Items, 2)
	require.Equal(t, uint(2), res.Items[0].ID) // 重建后时间倒序：v2 最新
	require.Equal(t, uint(1), res.Items[1].ID)

	// 重建后 ZSET 被回填
	members, err := cache.ZRangeWithScores(ctx, timelineKey, 0, -1)
	require.NoError(t, err)
	require.Len(t, members, 2)

	repo.AssertExpectations(t)
	videoProvider.AssertExpectations(t)
}

// TestListFeed_CursorInHotPath 测试游标分页在热区中的表现
func TestListFeed_CursorInHotPath(t *testing.T) {
	cache, _ := newTestCache(t)
	repo := &MockFeedRepo{cache: cache}
	videoProvider := &MockVideoProvider{}

	svc := NewFeedService(repo, videoProvider)

	// 准备缓存数据
	dbVideos := []video.Video{
		{ID: 1, Title: "V1", CreatedAt: time.Now().Add(-time.Second * 2)},
		{ID: 2, Title: "V2", CreatedAt: time.Now().Add(-time.Second)},
		{ID: 3, Title: "V3", CreatedAt: time.Now()},
	}
	dbViews := []video.VideoView{
		{ID: 1, Title: "V1", CreatedAt: dbVideos[0].CreatedAt},
		{ID: 2, Title: "V2", CreatedAt: dbVideos[1].CreatedAt},
		{ID: 3, Title: "V3", CreatedAt: dbVideos[2].CreatedAt},
	}

	arr := []feed.ZMember{
		{
			Score:  float64(dbVideos[0].CreatedAt.UnixMilli()),
			Member: "1",
		},
		{
			Score:  float64(dbVideos[1].CreatedAt.UnixMilli()),
			Member: "2",
		},
		{
			Score:  float64(dbVideos[2].CreatedAt.UnixMilli()),
			Member: "3",
		},
	}

	// 热区数据
	for _, elem := range arr {
		_ = repo.ZAdd(context.Background(), timelineKey, elem)
	}

	// 规划行为
	videoProvider.On("GetVideoEntitiesByIDs", mock.Anything, []uint{3}).Return(dbVideos[2:], nil).Once()
	videoProvider.On("BuildViews", mock.Anything, dbVideos[2:]).Return(dbViews[2:], nil).Once()
	videoProvider.On("GetVideoEntitiesByIDs", mock.Anything, []uint{2}).Return(dbVideos[1:2], nil).Once()
	videoProvider.On("BuildViews", mock.Anything, dbVideos[1:2]).Return(dbViews[1:2], nil).Once()
	videoProvider.On("GetVideoEntitiesByIDs", mock.Anything, []uint{1}).Return(dbVideos[0:0], nil).Once()
	videoProvider.On("BuildViews", mock.Anything, dbVideos[0:1]).Return(dbViews[0:1], nil).Once()

	res, err := svc.ListFeed(context.Background(), "", 1)

	next := video.EncodeCursor(video.Cursor{
		CreatedAt: dbVideos[2].CreatedAt,
		ID:        3,
	})

	// 断言
	require.NoError(t, err)
	require.Len(t, res.Items, 1)
	require.Equal(t, res.NextCursor, next)

	// 热区翻页
	next2 := video.EncodeCursor(video.Cursor{
		CreatedAt: dbVideos[1].CreatedAt,
		ID:        2,
	})
	res2, err := svc.ListFeed(context.Background(), res.NextCursor, 1)
	require.NoError(t, err)
	require.Len(t, res2.Items, 1)
	require.Equal(t, next2, res2.NextCursor)
}

// TestListFeed_CursorINColdPath 测试游标分页在冷区中的表现
func TestListFeed_CursorINColdPath(t *testing.T) {
	// 准备数据
	cache, _ := newTestCache(t)
	repo := &MockFeedRepo{cache: cache}
	videoProvider := &MockVideoProvider{}
	ctx := context.Background()

	// 准备缓存数据：video3 在热区(ZSET)，video1/video2 只在 DB(冷区)
	// 时间用毫秒级过去值，保证"调用时刻 reqTime > watermark"稳定走热路径（避免同 ms 竞态）
	dbVideos := []video.Video{
		{ID: 1, Title: "V1", CreatedAt: time.Now().Add(-300 * time.Millisecond)},
		{ID: 2, Title: "V2", CreatedAt: time.Now().Add(-200 * time.Millisecond)},
		{ID: 3, Title: "V3", CreatedAt: time.Now().Add(-100 * time.Millisecond)},
	}
	dbViews := []video.VideoView{
		{ID: 1, Title: "V1", CreatedAt: dbVideos[0].CreatedAt},
		{ID: 2, Title: "V2", CreatedAt: dbVideos[1].CreatedAt},
		{ID: 3, Title: "V3", CreatedAt: dbVideos[2].CreatedAt},
	}

	// 热区只放 video3
	repo.ZAdd(ctx, timelineKey, feed.ZMember{
		Score:  float64(dbVideos[2].CreatedAt.UnixMilli()),
		Member: "3",
	})

	// Page1（热路径）：video3 走 L3 水合 + BuildViews
	videoProvider.On("GetVideoEntitiesByIDs", mock.Anything, []uint{3}).Return(dbVideos[2:3], nil).Once()
	videoProvider.On("BuildViews", mock.Anything, dbVideos[2:3]).Return(dbViews[2:3], nil).Once()

	svc := NewFeedService(repo, videoProvider)

	// Page1：热区命中，返回 video3 + next_cursor
	next1 := video.EncodeCursor(video.Cursor{CreatedAt: dbVideos[2].CreatedAt, ID: 3})
	res1, err := svc.ListFeed(ctx, "", 1)
	require.NoError(t, err)
	require.Len(t, res1.Items, 1)
	require.Equal(t, uint(3), res1.Items[0].ID)
	require.Equal(t, next1, res1.NextCursor)

	// Page2（冷路径）：游标=video3(=watermark) → reqTime<=watermark → 走 listLatestFromDB
	// 冷区数据按时间倒序：[video2, video1]，limit+1=2 返回 2 条 → hasmore → next 非空
	next2 := video.EncodeCursor(video.Cursor{CreatedAt: dbVideos[1].CreatedAt, ID: 2})
	next1Cursor, _ := video.DecodeCursor(res1.NextCursor)
	repo.On("ListLatest", mock.Anything, &next1Cursor, 2).Return([]video.Video{dbVideos[1], dbVideos[0]}, nil).Once()
	videoProvider.On("BuildViews", mock.Anything, dbVideos[1:2]).Return(dbViews[1:2], nil).Once()

	res2, err := svc.ListFeed(ctx, res1.NextCursor, 1)
	require.NoError(t, err)
	require.Len(t, res2.Items, 1)
	require.Equal(t, uint(2), res2.Items[0].ID)
	require.Equal(t, next2, res2.NextCursor)

	// Page3（冷路径）：只剩 video1，返回 1 条 → 无更多 → next=""
	next2Cursor, _ := video.DecodeCursor(res2.NextCursor)
	repo.On("ListLatest", mock.Anything, &next2Cursor, 2).Return(dbVideos[0:1], nil).Once()
	videoProvider.On("BuildViews", mock.Anything, dbVideos[0:1]).Return(dbViews[0:1], nil).Once()

	res3, err := svc.ListFeed(ctx, res2.NextCursor, 1)
	require.NoError(t, err)
	require.Len(t, res3.Items, 1)
	require.Equal(t, uint(1), res3.Items[0].ID)
	require.Equal(t, "", res3.NextCursor)

	repo.AssertExpectations(t)
	videoProvider.AssertExpectations(t)
}

// TestListFeed_HotAndColdPath 测试热区数据不够limit，到DB中补充到limit的情况
func TestListFeed_HotAndColdPath(t *testing.T) {
	// 准备数据
	cache, _ := newTestCache(t)
	repo := &MockFeedRepo{cache: cache}
	videoProvider := &MockVideoProvider{}
	ctx := context.Background()

	// 准备缓存数据：video3 在热区(ZSET)，video1/video2 只在 DB(冷区)
	// 时间用毫秒级过去值，保证"调用时刻 reqTime > watermark"稳定走热路径（避免同 ms 竞态）
	dbVideos := []video.Video{
		{ID: 1, Title: "V1", CreatedAt: time.Now().Add(-300 * time.Millisecond)},
		{ID: 2, Title: "V2", CreatedAt: time.Now().Add(-200 * time.Millisecond)},
		{ID: 3, Title: "V3", CreatedAt: time.Now().Add(-100 * time.Millisecond)},
	}
	dbViews := []video.VideoView{
		{ID: 1, Title: "V1", CreatedAt: dbVideos[0].CreatedAt},
		{ID: 2, Title: "V2", CreatedAt: dbVideos[1].CreatedAt},
		{ID: 3, Title: "V3", CreatedAt: dbVideos[2].CreatedAt},
	}

	// 热区只放 video3
	repo.ZAdd(ctx, timelineKey, feed.ZMember{
		Score:  float64(dbVideos[2].CreatedAt.UnixMilli()),
		Member: "3",
	})

	// 一次查询2条记录，热区有会命中一条数据，然后发现热区数据不够，需要从冷区数据补充
	videoProvider.On("GetVideoEntitiesByIDs", mock.Anything, []uint{3}).Return(dbVideos[2:3], nil).Once()
	repo.On("ListLatest", mock.Anything, &video.Cursor{
		CreatedAt: dbVideos[2].CreatedAt,
		ID:        3,
	}, 2).Return([]video.Video{dbVideos[1], dbVideos[0]}, nil).Once()

	videos := []video.Video{
		dbVideos[2],
		dbVideos[1],
		dbVideos[0],
	}
	views := []video.VideoView{
		dbViews[2],
		dbViews[1],
		dbViews[0],
	}
	videoProvider.On("BuildViews", mock.Anything, videos[0:2]).Return(views[0:2], nil).Once()

	svc := NewFeedService(repo, videoProvider)
	res, err := svc.ListFeed(ctx, "", 2)
	require.NoError(t, err)
	require.Len(t, res.Items, 2)
	require.Equal(t, uint(3), res.Items[0].ID)
	require.Equal(t, uint(2), res.Items[1].ID)

	next := video.EncodeCursor(video.Cursor{CreatedAt: videos[1].CreatedAt, ID: videos[1].ID})
	require.Equal(t, next, res.NextCursor)

	repo.AssertExpectations(t)
	videoProvider.AssertExpectations(t)
}

// TestGetVideoByIDs_L1Cache 测试三层缓存中的L1
func TestGetVideoByIDs_L1Cache(t *testing.T) {
	cache, _ := newTestCache(t)
	repo := &MockFeedRepo{cache: cache}
	videoProvider := &MockVideoProvider{}
	svc := NewFeedService(repo, videoProvider)
	ctx := context.Background()

	// 预置 L1 本地缓存（进程内 go-cache）
	v1 := video.Video{ID: 1, Title: "V1", VideoKey: "videos/1/1.mp4", CoverKey: "covers/1/1.png"}
	svc.localcache.Set(cache.Key("video:entity:1"), v1, l1CacheTTL)

	// 执行：应 L1 命中，不碰 L2/L3（若碰 L3，未 mock 的 GetVideoEntitiesByIDs 会 panic）
	res, err := svc.GetVideoByIDs(ctx, []uint{1})
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Equal(t, uint(1), res[0].ID)
	require.Equal(t, "V1", res[0].Title)
	require.Equal(t, v1.VideoKey, res[0].VideoKey)
}

// TestGetVideoByIDs_L2Redis 测试三层缓存中的L2
func TestGetVideoByIDs_L2Redis(t *testing.T) {
	cache, _ := newTestCache(t)
	repo := &MockFeedRepo{cache: cache}
	videoProvider := &MockVideoProvider{}
	svc := NewFeedService(repo, videoProvider)
	ctx := context.Background()

	// 预置 L2 实体缓存（Redis，DTO 序列化）
	v1 := video.Video{ID: 1, Title: "V1", VideoKey: "videos/1/1.mp4", CoverKey: "covers/1/1.png"}
	b, _ := json.Marshal(v1.ToVideoEntityCache())
	cache.SetBytes(ctx, cache.Key("video:entity:1"), b, time.Hour)

	// 执行：L1 miss → L2 命中（不调 L3）
	res, err := svc.GetVideoByIDs(ctx, []uint{1})
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Equal(t, uint(1), res[0].ID)
	require.Equal(t, "V1", res[0].Title)
	require.Equal(t, v1.VideoKey, res[0].VideoKey) // 反序列化后带内部 key
}

// TestGetVideoByIDs_L3DB 测试三层缓存中的L3
func TestGetVideoByIDs_L3DB(t *testing.T) {
	cache, _ := newTestCache(t)
	repo := &MockFeedRepo{cache: cache}
	videoProvider := &MockVideoProvider{}
	svc := NewFeedService(repo, videoProvider)
	ctx := context.Background()

	v1 := video.Video{ID: 1, Title: "V1", VideoKey: "videos/1/1.mp4", CoverKey: "covers/1/1.png"}
	videoProvider.On("GetVideoEntitiesByIDs", mock.Anything, []uint{1}).Return([]video.Video{v1}, nil).Once()

	// 执行：L1/L2 miss → L3 回源 + 回填 L2
	res, err := svc.GetVideoByIDs(ctx, []uint{1})
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Equal(t, uint(1), res[0].ID)

	// L3 查完回填 L2
	b, err := cache.GetBytes(ctx, cache.Key("video:entity:1"))
	require.NoError(t, err)
	require.NotEmpty(t, b)

	videoProvider.AssertExpectations(t)
}

// TestListByTag 测试功能是否实现了
func TestListByTag(t *testing.T) {
	cache, _ := newTestCache(t)
	repo := &MockFeedRepo{cache: cache}
	videoProvider := &MockVideoProvider{}
	svc := NewFeedService(repo, videoProvider)
	ctx := context.Background()

	dbVideos := []video.Video{
		{ID: 1, Title: "V1", CreatedAt: time.Now().Add(-2 * time.Second)},
		{ID: 2, Title: "V2", CreatedAt: time.Now().Add(-time.Second)},
	}
	dbViews := []video.VideoView{
		{ID: 1, Title: "V1", CreatedAt: dbVideos[0].CreatedAt},
		{ID: 2, Title: "V2", CreatedAt: dbVideos[1].CreatedAt},
	}

	// FeedService.ListByTag：limit+1=3 探测 → 返回 2 条 → hasMore=false → next=""
	videoProvider.On("ListByTag", mock.Anything, "学习", mock.Anything, 3).Return(dbVideos, nil).Once()
	videoProvider.On("BuildViews", mock.Anything, mock.Anything).Return(dbViews, nil).Once()

	res, err := svc.ListByTag(ctx, "学习", "", 2)
	require.NoError(t, err)
	require.Len(t, res.Items, 2)
	require.Equal(t, uint(1), res.Items[0].ID) // 按 ListByTag 返回顺序
	require.Equal(t, uint(2), res.Items[1].ID)
	require.Equal(t, "", res.NextCursor)

	videoProvider.AssertExpectations(t)
}
