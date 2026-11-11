package feed

import (
	"context"
	"errors"
	"feedsystem/internal/config"
	"feedsystem/internal/data"
	"feedsystem/internal/model/feed"
	"feedsystem/internal/model/video"
	apperrors "feedsystem/internal/pkg/errors"
	videosvc "feedsystem/internal/service/video"
	"net"
	"net/http"
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
func (m *MockFeedRepo) ZAddBatch(ctx context.Context, key string, members []feed.ZMember) error {
	return m.cache.ZAddBatch(ctx, key, members)
}
func (m *MockFeedRepo) ZUnionStore(ctx context.Context, dest string, keys []string, aggregate string) error {
	return m.cache.ZUnionStore(ctx, dest, keys, aggregate)
}
func (m *MockFeedRepo) Expire(ctx context.Context, key string, expire time.Duration) error {
	return m.cache.Expire(ctx, key, expire)
}
func (m *MockFeedRepo) ZRevRange(ctx context.Context, key string, start, stop int64) ([]feed.ZMember, error) {
	return m.cache.ZRevRange(ctx, key, start, stop)
}
func (m *MockFeedRepo) Exists(ctx context.Context, key string) (bool, error) {
	return m.cache.Exists(ctx, key)
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
func (m *MockFeedRepo) Del(ctx context.Context, key string) error {
	return m.cache.Del(ctx, key)
}

// 冷数据查询  mock 的方法
func (m *MockFeedRepo) ListLatest(ctx context.Context, cursor *video.Cursor, limit int) ([]video.Video, error) {
	args := m.Called(ctx, cursor, limit)
	v, _ := args.Get(0).([]video.Video)
	return v, args.Error(1)
}

// ListLatestByAuthors 按作者列表查冷数据（关注流用）
func (m *MockFeedRepo) ListLatestByAuthors(ctx context.Context, authorIDs []uint, cursor *video.Cursor, limit int) ([]video.Video, error) {
	args := m.Called(ctx, authorIDs, cursor, limit)
	v, _ := args.Get(0).([]video.Video)
	return v, args.Error(1)
}

type MockVideoProvider struct {
	mock.Mock
}

// MockVideoReader 视频实体读取（关注流/Feed 使用，字段版：按 ids 返回 Videos 中的匹配项）
type MockVideoReader struct {
	Videos []video.Video
	Err    error
}

func (m *MockVideoReader) GetByIDs(ctx context.Context, ids []uint) ([]video.Video, error) {
	if m.Err != nil {
		return nil, m.Err
	}
	byID := make(map[uint]video.Video, len(m.Videos))
	for _, v := range m.Videos {
		byID[v.ID] = v
	}
	out := make([]video.Video, 0, len(ids))
	for _, id := range ids {
		if v, ok := byID[id]; ok {
			out = append(out, v)
		}
	}
	return out, nil
}

// FillTagIDs Feed 层只关心"补齐 TagIDs"，测试里默认 no-op；
// 需要验证标签逻辑时用 video 包的 TagsProvider 单测覆盖。
func (m *MockVideoReader) FillTagIDs(ctx context.Context, videos []video.Video) error {
	return nil
}

func (m *MockVideoProvider) ListByTag(ctx context.Context, tagName string, cursor *video.Cursor, limit int) ([]video.Video, error) {
	args := m.Called(ctx, tagName, cursor, limit)
	return args.Get(0).([]video.Video), args.Error(1)
}
func (m *MockVideoProvider) GetVideoEntitiesByIDs(ctx context.Context, ids []uint) ([]video.Video, error) {
	args := m.Called(ctx, ids)
	v, _ := args.Get(0).([]video.Video)
	return v, args.Error(1)
}

// MockViewBuilder 将展示依赖与视频查询依赖分开。
type MockViewBuilder struct {
	mock.Mock
}

func newMockViewBuilder(t *testing.T) *MockViewBuilder {
	t.Helper()
	builder := &MockViewBuilder{}
	builder.Test(t)
	t.Cleanup(func() { builder.AssertExpectations(t) })
	return builder
}

func (m *MockViewBuilder) BuildSome(ctx context.Context, videos []video.Video) videosvc.ViewBuildResult {
	return m.Called(ctx, videos).Get(0).(videosvc.ViewBuildResult)
}

func (m *MockViewBuilder) BuildOne(ctx context.Context, v video.Video) (video.VideoView, error) {
	args := m.Called(ctx, v)
	return args.Get(0).(video.VideoView), args.Error(1)
}

var _ videosvc.ViewBuilder = (*MockViewBuilder)(nil)

type MockRatingProvider struct {
	mock.Mock
}

func (m *MockRatingProvider) GetUserRatings(ctx context.Context, accountID uint, videoIDs []uint) (map[uint]int8, error) {
	args := m.Called(ctx, accountID, videoIDs)
	statuses, _ := args.Get(0).(map[uint]int8)
	return statuses, args.Error(1)
}

var _ RatingProvider = (*MockRatingProvider)(nil)

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
	viewBuilder := newMockViewBuilder(t)
	ratingProvider := &MockRatingProvider{}

	// 规定行为
	// On 指定特定的函数，当该函数被调用且输入参数匹配的时候，则会返回Return 指定的值
	repo.On("ListLatest", mock.Anything, mock.Anything, 1000).Return([]video.Video{}, nil)
	// 空库时热区(哨兵)后走冷区补充，兜底 ListLatest
	repo.On("ListLatest", mock.Anything, mock.Anything, mock.Anything).Return([]video.Video{}, nil)
	// 空列表也会走 buildViews → BuildSome([])
	viewBuilder.On("BuildSome", mock.Anything, mock.Anything).Return(videosvc.ViewBuildResult{Items: []video.VideoView{}})

	// 执行
	svc := NewFeedService(repo, &MockVideoReader{}, videoProvider, ratingProvider, nil, viewBuilder)
	res, err := svc.GlobalFeed(context.Background(), 0, "", 10)

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
	viewBuilder := newMockViewBuilder(t)
	ratingProvider := &MockRatingProvider{}

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
	viewBuilder.On("BuildSome", mock.Anything, dbVideos).Return(videosvc.ViewBuildResult{Items: dbViews})
	// 执行
	svc := NewFeedService(repo, &MockVideoReader{Videos: dbVideos}, videoProvider, ratingProvider, nil, viewBuilder)
	res, err := svc.GlobalFeed(context.Background(), 0, "", 10)

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
	viewBuilder := newMockViewBuilder(t)
	ratingProvider := &MockRatingProvider{}
	ctx := context.Background()

	// DB 有 2 条，ZSET 为空
	v1 := video.Video{ID: 1, Title: "V1", CreatedAt: time.Now().Add(-2 * time.Second)}
	v2 := video.Video{ID: 2, Title: "V2", CreatedAt: time.Now().Add(-time.Second)}

	// 重建：ListLatest(nil, 1000) 返回 2 条 → ZAdd 回填
	repo.On("ListLatest", mock.Anything, mock.Anything, 1000).Return([]video.Video{v1, v2}, nil).Once()

	// 重建后递归热路径：per-id L3 水合 + BuildSome
	viewBuilder.On("BuildSome", mock.Anything, mock.Anything).Return(videosvc.ViewBuildResult{Items: []video.VideoView{
		{ID: 2, Title: "V2", CreatedAt: v2.CreatedAt},
		{ID: 1, Title: "V1", CreatedAt: v1.CreatedAt},
	}}).Once()

	svc := NewFeedService(repo, &MockVideoReader{Videos: []video.Video{v1, v2}}, videoProvider, ratingProvider, nil, viewBuilder)
	res, err := svc.GlobalFeed(ctx, 0, "", 2)

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
	viewBuilder := newMockViewBuilder(t)
	ratingProvider := &MockRatingProvider{}

	// 准备缓存数据
	dbVideos := []video.Video{
		{ID: 1, Title: "V1", CreatedAt: time.Now().Add(-3 * time.Second).Truncate(time.Millisecond)},
		{ID: 2, Title: "V2", CreatedAt: time.Now().Add(-2 * time.Second).Truncate(time.Millisecond)},
		{ID: 3, Title: "V3", CreatedAt: time.Now().Add(-time.Second).Truncate(time.Millisecond)},
	}
	dbViews := []video.VideoView{
		{ID: 1, Title: "V1", CreatedAt: dbVideos[0].CreatedAt},
		{ID: 2, Title: "V2", CreatedAt: dbVideos[1].CreatedAt},
		{ID: 3, Title: "V3", CreatedAt: dbVideos[2].CreatedAt},
	}

	svc := NewFeedService(repo, &MockVideoReader{Videos: dbVideos}, videoProvider, ratingProvider, nil, viewBuilder)
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
	viewBuilder.On("BuildSome", mock.Anything, dbVideos[2:]).Return(videosvc.ViewBuildResult{Items: dbViews[2:]}).Once()
	viewBuilder.On("BuildSome", mock.Anything, dbVideos[1:2]).Return(videosvc.ViewBuildResult{Items: dbViews[1:2]}).Once()

	res, err := svc.GlobalFeed(context.Background(), 0, "", 1)

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
	res2, err := svc.GlobalFeed(context.Background(), 0, res.NextCursor, 1)
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
	viewBuilder := newMockViewBuilder(t)
	ratingProvider := &MockRatingProvider{}
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

	// Page1（热路径）：video3 走 L3 水合 + BuildSome
	viewBuilder.On("BuildSome", mock.Anything, dbVideos[2:3]).Return(videosvc.ViewBuildResult{Items: dbViews[2:3]}).Once()

	svc := NewFeedService(repo, &MockVideoReader{Videos: dbVideos}, videoProvider, ratingProvider, nil, viewBuilder)

	// Page1：热区命中，返回 video3 + next_cursor
	next1 := video.EncodeCursor(video.Cursor{CreatedAt: dbVideos[2].CreatedAt, ID: 3})
	res1, err := svc.GlobalFeed(ctx, 0, "", 1)
	require.NoError(t, err)
	require.Len(t, res1.Items, 1)
	require.Equal(t, uint(3), res1.Items[0].ID)
	require.Equal(t, next1, res1.NextCursor)

	// Page2（冷路径）：游标=video3(=watermark) → reqTime<=watermark → 走 listLatestFromDB
	// 冷区数据按时间倒序：[video2, video1]，limit+1=2 返回 2 条 → hasmore → next 非空
	next2 := video.EncodeCursor(video.Cursor{CreatedAt: dbVideos[1].CreatedAt, ID: 2})
	next1Cursor, _ := video.DecodeCursor(res1.NextCursor)
	repo.On("ListLatest", mock.Anything, &next1Cursor, 2).Return([]video.Video{dbVideos[1], dbVideos[0]}, nil).Once()
	viewBuilder.On("BuildSome", mock.Anything, dbVideos[1:2]).Return(videosvc.ViewBuildResult{Items: dbViews[1:2]}).Once()

	res2, err := svc.GlobalFeed(ctx, 0, res1.NextCursor, 1)
	require.NoError(t, err)
	require.Len(t, res2.Items, 1)
	require.Equal(t, uint(2), res2.Items[0].ID)
	require.Equal(t, next2, res2.NextCursor)

	// Page3（冷路径）：只剩 video1，返回 1 条 → 无更多 → next=""
	next2Cursor, _ := video.DecodeCursor(res2.NextCursor)
	repo.On("ListLatest", mock.Anything, &next2Cursor, 2).Return(dbVideos[0:1], nil).Once()
	viewBuilder.On("BuildSome", mock.Anything, dbVideos[0:1]).Return(videosvc.ViewBuildResult{Items: dbViews[0:1]}).Once()

	res3, err := svc.GlobalFeed(ctx, 0, res2.NextCursor, 1)
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
	viewBuilder := newMockViewBuilder(t)
	ratingProvider := &MockRatingProvider{}
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
	viewBuilder.On("BuildSome", mock.Anything, videos[0:2]).Return(videosvc.ViewBuildResult{Items: views[0:2]}).Once()

	svc := NewFeedService(repo, &MockVideoReader{Videos: dbVideos}, videoProvider, ratingProvider, nil, viewBuilder)
	res, err := svc.GlobalFeed(ctx, 0, "", 2)
	require.NoError(t, err)
	require.Len(t, res.Items, 2)
	require.Equal(t, uint(3), res.Items[0].ID)
	require.Equal(t, uint(2), res.Items[1].ID)

	next := video.EncodeCursor(video.Cursor{CreatedAt: videos[1].CreatedAt, ID: videos[1].ID})
	require.Equal(t, next, res.NextCursor)

	repo.AssertExpectations(t)
	videoProvider.AssertExpectations(t)
}

// TestListByTag 测试功能是否实现了
func TestListByTag(t *testing.T) {
	cache, _ := newTestCache(t)
	repo := &MockFeedRepo{cache: cache}
	videoProvider := &MockVideoProvider{}
	viewBuilder := newMockViewBuilder(t)
	ratingProvider := &MockRatingProvider{}
	ctx := context.Background()

	dbVideos := []video.Video{
		{ID: 1, Title: "V1", CreatedAt: time.Now().Add(-2 * time.Second)},
		{ID: 2, Title: "V2", CreatedAt: time.Now().Add(-time.Second)},
	}
	dbViews := []video.VideoView{
		{ID: 1, Title: "V1", CreatedAt: dbVideos[0].CreatedAt},
		{ID: 2, Title: "V2", CreatedAt: dbVideos[1].CreatedAt},
	}

	svc := NewFeedService(repo, &MockVideoReader{Videos: dbVideos}, videoProvider, ratingProvider, nil, viewBuilder)
	// FeedService.ListByTag：limit+1=3 探测 → 返回 2 条 → hasMore=false → next=""
	videoProvider.On("ListByTag", mock.Anything, "学习", mock.Anything, 3).Return(dbVideos, nil).Once()
	viewBuilder.On("BuildSome", mock.Anything, mock.Anything).Return(videosvc.ViewBuildResult{Items: dbViews}).Once()

	res, err := svc.ListByTag(ctx, 0, "学习", "", 2)
	require.NoError(t, err)
	require.Len(t, res.Items, 2)
	require.Equal(t, uint(1), res.Items[0].ID) // 按 ListByTag 返回顺序
	require.Equal(t, uint(2), res.Items[1].ID)
	require.Equal(t, "", res.NextCursor)

	videoProvider.AssertExpectations(t)
}

// Feed 只验证业务查询及状态填充，缓存回退由 RatingService 测试覆盖。
func TestStampStatus(t *testing.T) {
	queryErr := errors.New("rating service unavailable")
	for _, tc := range []struct {
		name         string
		accountID    uint
		views        []video.VideoView
		statuses     map[uint]int8
		queryErr     error
		wantStatuses []int8
		skipQuery    bool
	}{
		{name: "anonymous", views: []video.VideoView{{ID: 30}, {ID: 10}}, wantStatuses: []int8{0, 0}, skipQuery: true},
		{name: "nil_input", accountID: 7, skipQuery: true},
		{name: "empty_input", accountID: 7, views: []video.VideoView{}, skipQuery: true},
		{
			name: "map_by_id_not_order", accountID: 7,
			views:    []video.VideoView{{ID: 30, Title: "A"}, {ID: 10, Title: "B"}, {ID: 20, Title: "C"}},
			statuses: map[uint]int8{10: -1, 30: 1}, wantStatuses: []int8{1, -1, 0},
		},
		{name: "no_ratings", accountID: 7, views: []video.VideoView{{ID: 30}}, statuses: map[uint]int8{}, wantStatuses: []int8{0}},
		{name: "nil_status_map", accountID: 7, views: []video.VideoView{{ID: 30}}, wantStatuses: []int8{0}},
		{
			name: "query_failure_preserves_views", accountID: 7,
			views:    []video.VideoView{{ID: 30, Title: "A", Status: 1}, {ID: 10, Title: "B", Status: -1}},
			queryErr: queryErr, wantStatuses: []int8{1, -1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider := &MockRatingProvider{}
			provider.Test(t)
			t.Cleanup(func() { provider.AssertExpectations(t) })
			svc := &FeedService{ratingSvc: provider}
			views := make([]video.VideoView, len(tc.views))
			copy(views, tc.views)
			if tc.views == nil {
				views = nil
			}
			if !tc.skipQuery {
				ids := make([]uint, len(views))
				for i := range views {
					ids[i] = views[i].ID
				}
				provider.On("GetUserRatings", t.Context(), tc.accountID, ids).
					Return(tc.statuses, tc.queryErr).Once()
			}

			result, err := svc.stampStatus(t.Context(), tc.accountID, views)

			if tc.queryErr != nil {
				require.ErrorIs(t, err, tc.queryErr)
			} else {
				require.NoError(t, err)
			}
			require.Len(t, result, len(tc.views))
			for i := range result {
				want := tc.views[i]
				want.Status = tc.wantStatuses[i]
				require.Equal(t, want, result[i])
			}
			if tc.skipQuery {
				provider.AssertNotCalled(t, "GetUserRatings", mock.Anything, mock.Anything, mock.Anything)
			}
		})
	}
}

// 测试当热区 zset 里面有数据，但是三层缓存查询出来的视频为空的时候
// 能否自动查询DB返回
func TestListFeed_DeletedVideo(t *testing.T) {
	cache, _ := newTestCache(t)
	repo := &MockFeedRepo{cache: cache}
	videoProvider := &MockVideoProvider{}
	viewBuilder := newMockViewBuilder(t)
	ratingProvider := &MockRatingProvider{}
	svc := NewFeedService(repo, &MockVideoReader{}, videoProvider, ratingProvider, nil, viewBuilder)
	ctx := context.Background()

	// 准备zset数据
	past := time.Now().Add(-time.Minute).UnixMilli()
	cache.ZAdd(t.Context(), "feed:global_timeline", feed.ZMember{
		Score:  float64(past),
		Member: "999",
	}) // 最老
	cache.ZAdd(t.Context(), "feed:global_timeline", feed.ZMember{
		Score:  float64(past + 1000),
		Member: "1000",
	}) // 最新的

	// 选中热区数据三层查询之后为空
	// 转到直接查询数据库
	repo.On("ListLatest", mock.Anything, mock.Anything, mock.Anything).Return([]video.Video{
		{
			ID:       2,
			AuthorID: 2,
		},
	}, nil).Once()
	viewBuilder.On("BuildSome", mock.Anything, []video.Video{{ID: 2, AuthorID: 2}}).Return(videosvc.ViewBuildResult{Items: []video.VideoView{
		{
			ID:    2,
			Title: "video2",
		},
	}})

	res, err := svc.GlobalFeed(ctx, 0, "", 1)

	// 断言
	require.NoError(t, err)
	require.Equal(t, 1, len(res.Items))
	require.Equal(t, uint(2), res.Items[0].ID)
	repo.AssertExpectations(t)
	videoProvider.AssertExpectations(t)
}

// 路径矩阵验证各 Feed 入口均使用共享 Builder，并遵守同一部分成功策略。
// 页尾视频构建失败时，游标仍按已消费实体推进，不能退回最后一条成功项。
func TestFeedViewBuilderResults(t *testing.T) {
	paths := []string{"global_hot", "global_cold", "tag", "popular", "following_hot", "following_cold"}
	outcomes := []string{"success", "partial", "all_failed"}

	for _, path := range paths {
		for _, outcome := range outcomes {
			t.Run(path+"/"+outcome, func(t *testing.T) {
				ctx := t.Context()
				cache, _ := newTestCache(t)
				repo := &MockFeedRepo{cache: cache}
				provider := &MockVideoProvider{}
				ratings := &MockRatingProvider{}
				follows := &MockFollowProvider{}
				builder := newMockViewBuilder(t)
				t.Cleanup(func() {
					repo.AssertExpectations(t)
					provider.AssertExpectations(t)
					follows.AssertExpectations(t)
				})

				base := time.Now().UTC().Truncate(time.Second).Add(-time.Minute)
				videos := []video.Video{
					{ID: 3, Title: "newest", CreatedAt: base},
					{ID: 2, Title: "page tail", CreatedAt: base.Add(-time.Second)},
					{ID: 1, Title: "next page", CreatedAt: base.Add(-2 * time.Second)},
				}
				views := []video.VideoView{
					{ID: 3, Title: "newest", CreatedAt: videos[0].CreatedAt, HashTags: []string{"旅行"}},
					{ID: 2, Title: "page tail", CreatedAt: videos[1].CreatedAt},
				}
				svc := NewFeedService(repo, &MockVideoReader{Videos: videos}, provider, ratings, follows, builder)
				buildResult := videosvc.ViewBuildResult{Items: views}
				signErr := errors.New("signing failed")
				switch outcome {
				case "partial":
					buildResult.Items = views[:1]
					buildResult.Failures = []videosvc.ViewBuilFailure{{Index: 1, VideoID: 2, Err: signErr}}
				case "all_failed":
					buildResult.Items = []video.VideoView{}
					buildResult.Failures = []videosvc.ViewBuilFailure{
						{Index: 0, VideoID: 3, Err: signErr},
						{Index: 1, VideoID: 2, Err: signErr},
					}
				}
				builder.On("BuildSome", mock.Anything, videos[:2]).Return(buildResult).Once()

				seedTimeline := func(key string, popularity bool) {
					for i, v := range videos {
						score := float64(v.CreatedAt.UnixMilli())
						if popularity {
							score = float64(100 - i)
						}
						require.NoError(t, cache.ZAdd(ctx, key, feed.ZMember{
							Score: score, Member: strconv.FormatUint(uint64(v.ID), 10),
						}))
					}
				}

				var res *FeedListResult
				var err error
				switch path {
				case "global_hot":
					seedTimeline(timelineKey, false)
					res, err = svc.GlobalFeed(ctx, 0, "", 2)
				case "global_cold":
					// 游标等于热区最老位置，触发公开入口的数据库降级路径。
					cursor := video.Cursor{CreatedAt: base.Add(time.Second), ID: 99}
					require.NoError(t, cache.ZAdd(ctx, timelineKey, feed.ZMember{
						Score: float64(cursor.CreatedAt.UnixMilli()), Member: "99",
					}))
					repo.On("ListLatest", mock.Anything, mock.MatchedBy(func(actual *video.Cursor) bool {
						return actual != nil && actual.ID == cursor.ID && actual.CreatedAt.Equal(cursor.CreatedAt)
					}), 3).Return(videos, nil).Once()
					res, err = svc.GlobalFeed(ctx, 0, video.EncodeCursor(cursor), 2)
				case "tag":
					provider.On("ListByTag", mock.Anything, "旅行", (*video.Cursor)(nil), 3).
						Return(videos, nil).Once()
					res, err = svc.ListByTag(ctx, 0, "旅行", "", 2)
				case "following_hot":
					seedTimeline(cache.Key("feed:following:%d", 7), false)
					// 只为真正展示的项查询用户状态，失败项不能错位填入。
					if outcome != "all_failed" {
						ids := []uint{3, 2}
						statuses := map[uint]int8{3: 1, 2: -1}
						if outcome == "partial" {
							ids = ids[:1]
							delete(statuses, 2)
						}
						ratings.On("GetUserRatings", mock.Anything, uint(7), ids).Return(statuses, nil).Once()
					}
					res, err = svc.ListByFollowing(ctx, 7, "", 2)
				case "following_cold":
					ratings.On("GetUserRatings", mock.Anything, uint(7), mock.Anything).Return(map[uint]int8{3: 1, 2: -1}, nil).Maybe()
					cursor := video.Cursor{CreatedAt: base.Add(time.Second), ID: 99}
					require.NoError(t, cache.ZAdd(ctx, cache.Key("feed:following:%d", 7), feed.ZMember{
						Score: float64(cursor.CreatedAt.UnixMilli()), Member: "99",
					}))
					follows.On("ListFollowingIDs", mock.Anything, uint(7)).Return([]uint{42}, nil).Once()
					repo.On("ListLatestByAuthors", mock.Anything, []uint{42}, mock.MatchedBy(func(actual *video.Cursor) bool {
						return actual != nil && actual.ID == cursor.ID && actual.CreatedAt.Equal(cursor.CreatedAt)
					}), 3).
						Return(videos, nil).Once()
					res, err = svc.ListByFollowing(ctx, 7, video.EncodeCursor(cursor), 2)
				case "popular":
					asOf := base.UTC().Truncate(time.Minute)
					seedTimeline(cache.Key("hot:video:60m:%s", asOf.Format("200601021504")), true)
					popular, popularErr := svc.ListByPopularity(ctx, 0, asOf.Unix(), 0, 2)
					err = popularErr
					if outcome == "all_failed" {
						require.Nil(t, popular)
					} else {
						require.NoError(t, err)
						require.NotNil(t, popular)
						require.Equal(t, asOf.Unix(), popular.AsOf)
						require.Equal(t, int64(2), popular.Offset) // 按排名消费数，不按成功数。
						require.True(t, popular.HasMore)
						res = &FeedListResult{Items: popular.Items}
					}
				}

				if outcome == "all_failed" {
					require.Error(t, err)
					require.Nil(t, res)
					var appErr *apperrors.AppError
					require.ErrorAs(t, err, &appErr)
					require.Equal(t, http.StatusInternalServerError, appErr.Status)
					// 所有流统一走 buildViews
					wantMsg := "视频展示无法播放"
					require.Equal(t, wantMsg, appErr.Msg)
				} else {
					require.NoError(t, err)
					require.NotNil(t, res)
					require.Len(t, res.Items, len(buildResult.Items))
					for i, item := range res.Items {
						require.Equal(t, buildResult.Items[i].ID, item.ID)
					}
					require.Equal(t, []string{"旅行"}, res.Items[0].HashTags)
					if path != "popular" {
						cursor, decodeErr := video.DecodeCursor(res.NextCursor)
						require.NoError(t, decodeErr)
						require.Equal(t, uint(2), cursor.ID) // 页尾失败也应越过它。
						require.True(t, videos[1].CreatedAt.Equal(cursor.CreatedAt))
					}
					if path == "following_hot" {
						require.Equal(t, int8(1), res.Items[0].Status)
					}
				}
				ratings.AssertExpectations(t)
				builder.AssertNotCalled(t, "BuildOne", mock.Anything, mock.Anything)
			})
		}
	}
}

type MockFollowProvider struct {
	mock.Mock
}

func (m *MockFollowProvider) ListFollowingIDs(ctx context.Context, accountID uint) ([]uint, error) {
	args := m.Called(ctx, accountID)
	return args.Get(0).([]uint), args.Error(1)
}

func TestFeedViewBuilderEmptyResults(t *testing.T) {
	for _, path := range []string{"global", "tag", "following", "popular"} {
		t.Run(path, func(t *testing.T) {
			cache, _ := newTestCache(t)
			repo := &MockFeedRepo{cache: cache}
			provider := &MockVideoProvider{}
			builder := newMockViewBuilder(t)
			follows := &MockFollowProvider{}
			svc := NewFeedService(repo, &MockVideoReader{}, provider, &MockRatingProvider{}, follows, builder)
			var res *FeedListResult
			var err error
			switch path {
			case "global":
				repo.On("ListLatest", mock.Anything, (*video.Cursor)(nil), 3).Return([]video.Video{}, nil).Once()
				vs, hasMore, dbErr := svc.listFromDB(t.Context(), 0, nil, 2)
				err = dbErr
				require.NoError(t, dbErr)
				require.Empty(t, vs)
				require.False(t, hasMore)
				res = &FeedListResult{Items: []video.VideoView{}}
			case "following":
				follows.On("ListFollowingIDs", mock.Anything, uint(7)).Return([]uint{}, nil).Once()
				repo.On("ListLatestByAuthors", mock.Anything, []uint{}, (*video.Cursor)(nil), 3).
					Return([]video.Video{}, nil).Once()
				vs, hasMore, dbErr := svc.listFollowingFromDB(t.Context(), 7, nil, 2)
				err = dbErr
				require.NoError(t, dbErr)
				require.Empty(t, vs)
				require.False(t, hasMore)
				res = &FeedListResult{Items: []video.VideoView{}}
			case "tag":
				provider.On("ListByTag", mock.Anything, "旅行", (*video.Cursor)(nil), 3).
					Return([]video.Video{}, nil).Once()
				builder.On("BuildSome", mock.Anything, mock.MatchedBy(func(vs []video.Video) bool {
					return len(vs) == 0
				})).Return(videosvc.ViewBuildResult{Items: []video.VideoView{}}).Once()
				res, err = svc.ListByTag(t.Context(), 0, "旅行", "", 2)
			case "popular":
				builder.On("BuildSome", mock.Anything, mock.MatchedBy(func(vs []video.Video) bool {
					return len(vs) == 0
				})).Return(videosvc.ViewBuildResult{Items: []video.VideoView{}}).Once()
				popular, popularErr := svc.ListByPopularity(t.Context(), 0, 0, 0, 2)
				err = popularErr
				require.NoError(t, err)
				require.NotNil(t, popular)
				require.False(t, popular.HasMore)
				res = &FeedListResult{Items: popular.Items}
			}
			require.NoError(t, err)
			require.NotNil(t, res)
			require.NotNil(t, res.Items)
			require.Empty(t, res.Items)
			require.Empty(t, res.NextCursor)
			repo.AssertExpectations(t)
			provider.AssertExpectations(t)
			follows.AssertExpectations(t)
			if path == "global" || path == "following" {
				builder.AssertNotCalled(t, "BuildSome", mock.Anything, mock.Anything)
			}
		})
	}
}

// 通过真实 Builder 验证标签属于可降级信息：查询失败仍保留视频与计数。
func TestListByTag_RealBuilderTagFallback(t *testing.T) {
	for _, tagFailure := range []bool{false, true} {
		name := "tags_available"
		if tagFailure {
			name = "tags_unavailable"
		}
		t.Run(name, func(t *testing.T) {
			cache, _ := newTestCache(t)
			repo := &MockFeedRepo{cache: cache}
			provider := &MockVideoProvider{}
			videos := []video.Video{
				{ID: 2, VideoKey: "videos/2.mp4", CoverKey: "covers/2.jpg", LikedCount: 5},
				{ID: 1, VideoKey: "videos/1.mp4", CoverKey: "covers/1.jpg", CommentCount: 3},
			}
			provider.On("ListByTag", mock.Anything, "旅行", (*video.Cursor)(nil), 3).
				Return(videos, nil).Once()
			tags := &testTagsGetter{tags: map[uint][]string{2: {"旅行"}}}
			if tagFailure {
				tags.err = errors.New("tag database unavailable")
			}
			builder := videosvc.NewVideoViewBuilder(testURLSigner{}, tags)
			svc := NewFeedService(repo, &MockVideoReader{Videos: videos}, provider, &MockRatingProvider{}, nil, builder)

			res, err := svc.ListByTag(t.Context(), 0, "旅行", "", 2)

			require.NoError(t, err)
			require.Len(t, res.Items, 2)
			require.Equal(t, uint(2), res.Items[0].ID)
			require.Equal(t, uint(1), res.Items[1].ID)
			require.Equal(t, uint(5), res.Items[0].LikedCount)
			require.Equal(t, uint(3), res.Items[1].CommentCount)
			require.NotEmpty(t, res.Items[0].PlayURL)
			require.Empty(t, res.NextCursor)
			require.Equal(t, 1, tags.calls)
			require.Equal(t, []uint{2, 1}, tags.ids)
			if tagFailure {
				require.Empty(t, res.Items[0].HashTags)
			} else {
				require.Equal(t, []string{"旅行"}, res.Items[0].HashTags)
			}
			require.Empty(t, res.Items[1].HashTags)
			provider.AssertExpectations(t)
		})
	}
}

type testURLSigner struct{}

func (testURLSigner) PresignedGetObject(_ context.Context, key string, _ time.Duration) (string, error) {
	return "https://media.example/" + key, nil
}

type testTagsGetter struct {
	tags  map[uint][]string
	err   error
	calls int
	ids   []uint
}

func (g *testTagsGetter) GetTags(_ context.Context, videos []video.Video) (map[uint][]string, error) {
	g.calls++
	ids := make([]uint, 0, len(videos))
	for _, v := range videos {
		ids = append(ids, v.ID)
	}
	g.ids = ids
	if g.err != nil {
		return nil, g.err
	}
	return g.tags, nil
}

// 评价服务不可用时，HTTP 用例仍可返回视频，同时 stampStatus 保留可记录的错误。
func TestListByTag_RatingFailureKeepsVideosAndCursor(t *testing.T) {
	provider := &MockVideoProvider{}
	ratings := &MockRatingProvider{}
	builder := newMockViewBuilder(t)
	provider.Test(t)
	ratings.Test(t)
	t.Cleanup(func() {
		provider.AssertExpectations(t)
		ratings.AssertExpectations(t)
	})
	base := time.Date(2026, time.November, 4, 8, 0, 0, 0, time.UTC)
	videos := []video.Video{
		{ID: 30, CreatedAt: base},
		{ID: 10, CreatedAt: base.Add(-time.Second)},
		{ID: 20, CreatedAt: base.Add(-2 * time.Second)},
	}
	views := []video.VideoView{
		{ID: 30, Title: "A", PlayURL: "https://media.example/30", HashTags: []string{"旅行"}},
		{ID: 10, Title: "B", PlayURL: "https://media.example/10"},
	}
	provider.On("ListByTag", t.Context(), "旅行", (*video.Cursor)(nil), 3).Return(videos, nil).Once()
	builder.On("BuildSome", t.Context(), videos[:2]).Return(videosvc.ViewBuildResult{Items: views}).Once()
	ratings.On("GetUserRatings", t.Context(), uint(7), []uint{30, 10}).
		Return(nil, errors.New("rating service unavailable")).Once()
	svc := NewFeedService(nil, &MockVideoReader{Videos: videos}, provider, ratings, nil, builder)

	res, err := svc.ListByTag(t.Context(), 7, "旅行", "", 2)

	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, views, res.Items)
	cursor, err := video.DecodeCursor(res.NextCursor)
	require.NoError(t, err)
	require.Equal(t, uint(10), cursor.ID)
	require.True(t, videos[1].CreatedAt.Equal(cursor.CreatedAt))
}
