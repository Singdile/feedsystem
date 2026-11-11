package video

import (
	"context"
	"encoding/json"
	"errors"
	videomodel "feedsystem/internal/model/video"
	apperrors "feedsystem/internal/pkg/errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	localcache "github.com/patrickmn/go-cache"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// 替身只实现 Reader 的小接口，不连接 MySQL 或 Redis。
type readerTestRepo struct {
	mu         sync.Mutex
	findCalls  []uint
	batchCalls [][]uint
	tagCalls   [][]uint
	find       func(context.Context, uint) (*videomodel.Video, error)
	batch      func(context.Context, []uint) ([]videomodel.Video, error)
	tags       map[uint][]uint
	tagErr     error
}

func (r *readerTestRepo) FindByID(ctx context.Context, id uint) (*videomodel.Video, error) {
	r.mu.Lock()
	r.findCalls = append(r.findCalls, id)
	r.mu.Unlock()
	if r.find == nil {
		return nil, errors.New("unexpected FindByID call")
	}
	return r.find(ctx, id)
}

func (r *readerTestRepo) GetVideosByIDs(ctx context.Context, ids []uint) ([]videomodel.Video, error) {
	r.mu.Lock()
	r.batchCalls = append(r.batchCalls, append([]uint(nil), ids...))
	r.mu.Unlock()
	if r.batch == nil {
		return nil, errors.New("unexpected GetVideosByIDs call")
	}
	return r.batch(ctx, ids)
}

// GetTagIDsByVideoIDs 默认给每个请求的 id 返回空切片（表示"已加载、无标签"）。
func (r *readerTestRepo) GetTagIDsByVideoIDs(_ context.Context, ids []uint) (map[uint][]uint, error) {
	r.mu.Lock()
	r.tagCalls = append(r.tagCalls, append([]uint(nil), ids...))
	tags, err := r.tags, r.tagErr
	r.mu.Unlock()
	if err != nil {
		return nil, err
	}

	out := make(map[uint][]uint, len(ids))
	for _, id := range ids {
		if v, ok := tags[id]; ok {
			out[id] = append([]uint{}, v...)
		} else {
			out[id] = []uint{}
		}
	}
	return out, nil
}

func (r *readerTestRepo) tagIDCalls() [][]uint {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]uint(nil), r.tagCalls...)
}

type readerTestCache struct {
	mu       sync.Mutex
	values   map[string]any
	getKeys  []string
	mgetKeys [][]string
	setTTLs  map[string]time.Duration
	readErr  error
	writeErr error
	afterGet func(int)
}

func newReaderTestCache() *readerTestCache {
	return &readerTestCache{values: make(map[string]any), setTTLs: make(map[string]time.Duration)}
}

func (c *readerTestCache) Key(format string, args ...any) string {
	return fmt.Sprintf(format, args...)
}

func (c *readerTestCache) GetBytes(_ context.Context, key string) ([]byte, error) {
	c.mu.Lock()
	c.getKeys = append(c.getKeys, key)
	call := len(c.getKeys)
	value, exists := c.values[key]
	err := c.readErr
	hook := c.afterGet
	c.mu.Unlock()
	if hook != nil {
		hook(call)
	}
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, redis.Nil
	}
	if text, ok := value.(string); ok {
		return []byte(text), nil
	}
	return nil, errors.New("unexpected cache type")
}

func (c *readerTestCache) MGet(_ context.Context, keys ...string) ([]any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mgetKeys = append(c.mgetKeys, append([]string(nil), keys...))
	if c.readErr != nil {
		return nil, c.readErr
	}
	result := make([]any, len(keys))
	for i, key := range keys {
		result[i] = c.values[key]
	}
	return result, nil
}

func (c *readerTestCache) SetBytes(_ context.Context, key string, value []byte, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setTTLs[key] = ttl
	if c.writeErr != nil {
		return c.writeErr
	}
	c.values[key] = string(value)
	return nil
}

var (
	_ VideoReadRepo    = (*readerTestRepo)(nil)
	_ VideoEntityCache = (*readerTestCache)(nil)
)

func readerTestVideo(id uint) videomodel.Video {
	return videomodel.Video{
		ID: id, AuthorID: 7, Username: "author", Title: fmt.Sprintf("video-%d", id),
		VideoKey: fmt.Sprintf("videos/%d.mp4", id), CoverKey: fmt.Sprintf("covers/%d.jpg", id),
		LikedCount: 5, DislikedCount: 2, CommentCount: 8,
		CreatedAt: time.Date(2026, time.November, 5, 8, 0, 0, 0, time.UTC),
		TagIDs:    []uint{},
	}
}

func readerTestJSON(t *testing.T, v videomodel.Video) string {
	t.Helper()
	b, err := json.Marshal(v.ToVideoEntityCache())
	require.NoError(t, err)
	return string(b)
}

func assertReaderL1(t *testing.T, reader *VideoReader, v videomodel.Video) {
	t.Helper()
	value, expiration, ok := reader.localcache.GetWithExpiration(reader.cache.Key("video:entity:%d", v.ID))
	require.True(t, ok)
	entity, ok := value.(videomodel.VideoEntityCache)
	require.True(t, ok, "L1 必须保存缓存 DTO，而不是字节或视频实体")
	require.Equal(t, v, entity.ToVideo())
	require.WithinDuration(t, time.Now().Add(5*time.Second), expiration, time.Second)
}

func TestVideoReaderGetByIDCacheHits(t *testing.T) {
	for _, layer := range []string{"L1", "L2"} {
		t.Run(layer, func(t *testing.T) {
			repo, cache := &readerTestRepo{}, newReaderTestCache()
			reader := NewVideoReader(repo, cache)
			v := readerTestVideo(42)
			key := cache.Key("video:entity:%d", v.ID)
			if layer == "L1" {
				reader.localcache.Set(key, v.ToVideoEntityCache(), l1CacheTTL)
			} else {
				cache.values[key] = readerTestJSON(t, v)
			}

			result, err := reader.GetByID(t.Context(), v.ID)

			require.NoError(t, err)
			require.Equal(t, v, *result)
			assertReaderL1(t, reader, v)
			require.Empty(t, repo.findCalls)
			require.Empty(t, repo.batchCalls)
			require.Empty(t, cache.setTTLs)
			if layer == "L1" {
				require.Empty(t, cache.getKeys)
			} else {
				require.Equal(t, []string{key}, cache.getKeys)
			}
		})
	}
}

func TestVideoReaderGetByIDDatabaseRefill(t *testing.T) {
	for _, scenario := range []string{"miss", "corrupt_json", "redis_unavailable", "refill_failed"} {
		t.Run(scenario, func(t *testing.T) {
			v := readerTestVideo(42)
			cache := newReaderTestCache()
			key := cache.Key("video:entity:%d", v.ID)
			switch scenario {
			case "corrupt_json":
				cache.values[key] = "{invalid json"
			case "redis_unavailable":
				cache.readErr = errors.New("redis unavailable")
			case "refill_failed":
				cache.writeErr = errors.New("redis write failed")
			}
			repo := &readerTestRepo{find: func(ctx context.Context, id uint) (*videomodel.Video, error) {
				require.Equal(t, t.Context(), ctx)
				require.Equal(t, uint(42), id)
				return &v, nil
			}}
			reader := NewVideoReader(repo, cache)

			result, err := reader.GetByID(t.Context(), 42)

			require.NoError(t, err)
			require.Equal(t, v, *result)
			require.NotSame(t, &v, result)
			require.Equal(t, []uint{42}, repo.findCalls)
			assertReaderL1(t, reader, v)
			require.Equal(t, time.Hour, cache.setTTLs[key])
			if cache.writeErr == nil {
				var cached videomodel.VideoEntityCache
				require.NoError(t, json.Unmarshal([]byte(cache.values[key].(string)), &cached))
				require.Equal(t, v, cached.ToVideo())
			}
			// 再次读取必须命中 L1，也能发现回填类型错误导致的 panic。
			result.Title = "caller-only change"
			second, err := reader.GetByID(t.Context(), 42)
			require.NoError(t, err)
			require.Equal(t, v, *second)
			require.Len(t, repo.findCalls, 1)
		})
	}
}

func TestVideoReaderGetByIDErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		id     uint
		err    error
		status int
	}{
		{name: "invalid_id", status: http.StatusBadRequest},
		{name: "not_found", id: 42, err: gorm.ErrRecordNotFound, status: http.StatusNotFound},
		{name: "wrapped_not_found", id: 42, err: fmt.Errorf("query: %w", gorm.ErrRecordNotFound), status: http.StatusNotFound},
		{name: "database_failed", id: 42, err: errors.New("database unavailable"), status: http.StatusInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache := newReaderTestCache()
			repo := &readerTestRepo{find: func(context.Context, uint) (*videomodel.Video, error) { return nil, tc.err }}
			reader := NewVideoReader(repo, cache)

			result, err := reader.GetByID(t.Context(), tc.id)

			require.Nil(t, result)
			require.Error(t, err)
			var appErr *apperrors.AppError
			require.ErrorAs(t, err, &appErr)
			require.Equal(t, tc.status, appErr.Status)
			require.Empty(t, cache.setTTLs)
			if tc.id == 0 {
				require.Empty(t, cache.getKeys)
				require.Empty(t, repo.findCalls)
			}
		})
	}
}

func TestVideoReaderGetByIDRechecksRedis(t *testing.T) {
	cache := newReaderTestCache()
	v := readerTestVideo(42)
	key := cache.Key("video:entity:%d", v.ID)
	payload := readerTestJSON(t, v)
	cache.afterGet = func(call int) {
		if call == 1 {
			cache.mu.Lock()
			cache.values[key] = payload // 第一次读取已判定 miss，模拟另一个请求随后完成回填。
			cache.mu.Unlock()
		}
	}
	repo := &readerTestRepo{}
	reader := NewVideoReader(repo, cache)

	result, err := reader.GetByID(t.Context(), 42)

	require.NoError(t, err)
	require.Equal(t, v, *result)
	require.Len(t, cache.getKeys, 2)
	require.Empty(t, repo.findCalls)
	assertReaderL1(t, reader, v)
}

func TestVideoReaderGetByIDsCacheHits(t *testing.T) {
	for _, layer := range []string{"L1", "L2"} {
		t.Run(layer, func(t *testing.T) {
			cache, repo := newReaderTestCache(), &readerTestRepo{}
			reader := NewVideoReader(repo, cache)
			videos := []videomodel.Video{readerTestVideo(30), readerTestVideo(10)}
			for _, v := range videos {
				key := cache.Key("video:entity:%d", v.ID)
				if layer == "L1" {
					reader.localcache.Set(key, v.ToVideoEntityCache(), l1CacheTTL)
				} else {
					cache.values[key] = readerTestJSON(t, v)
				}
			}

			result, err := reader.GetByIDs(t.Context(), []uint{30, 10})

			require.NoError(t, err)
			require.Equal(t, videos, result)
			require.Empty(t, repo.batchCalls)
			require.Empty(t, repo.findCalls)
			if layer == "L1" {
				require.Empty(t, cache.mgetKeys)
			} else {
				require.Equal(t, [][]string{{"video:entity:30", "video:entity:10"}}, cache.mgetKeys)
			}
			for _, v := range videos {
				assertReaderL1(t, reader, v)
			}
		})
	}
}

func TestVideoReaderGetByIDsMixedLayers(t *testing.T) {
	cache := newReaderTestCache()
	v30, v10, v20 := readerTestVideo(30), readerTestVideo(10), readerTestVideo(20)
	repo := &readerTestRepo{batch: func(ctx context.Context, ids []uint) ([]videomodel.Video, error) {
		require.Equal(t, t.Context(), ctx)
		require.Equal(t, []uint{20, 99}, ids)
		return []videomodel.Video{v20}, nil // 视频 99 不存在。
	}}
	reader := NewVideoReader(repo, cache)
	reader.localcache.Set("video:entity:30", v30.ToVideoEntityCache(), l1CacheTTL)
	cache.values["video:entity:10"] = readerTestJSON(t, v10)
	ids := []uint{30, 10, 20, 99, 10}

	result, err := reader.GetByIDs(t.Context(), ids)

	require.NoError(t, err)
	require.Equal(t, []videomodel.Video{v30, v10, v20, v10}, result)
	require.Equal(t, []uint{30, 10, 20, 99, 10}, ids)
	require.Equal(t, [][]uint{{20, 99}}, repo.batchCalls)
	require.Empty(t, repo.findCalls)
	require.Equal(t, [][]string{{"video:entity:10", "video:entity:20", "video:entity:99", "video:entity:10"}}, cache.mgetKeys)
	assertReaderL1(t, reader, v20)
	require.Equal(t, time.Hour, cache.setTTLs["video:entity:20"])
}

func TestVideoReaderGetByIDsDatabaseFallback(t *testing.T) {
	for _, scenario := range []string{"miss", "invalid_cache", "redis_unavailable", "refill_failed", "database_failed", "none_exist"} {
		t.Run(scenario, func(t *testing.T) {
			cache := newReaderTestCache()
			v30, v10, v20 := readerTestVideo(30), readerTestVideo(10), readerTestVideo(20)
			dbErr := errors.New("database unavailable")
			switch scenario {
			case "invalid_cache":
				cache.values["video:entity:30"] = "{invalid json"
				cache.values["video:entity:10"] = 123 // 非字符串缓存也应回源。
			case "redis_unavailable":
				cache.readErr = errors.New("redis unavailable")
			case "refill_failed":
				cache.writeErr = errors.New("redis write failed")
			}
			repo := &readerTestRepo{batch: func(context.Context, []uint) ([]videomodel.Video, error) {
				if scenario == "database_failed" {
					return nil, dbErr
				}
				if scenario == "none_exist" {
					return nil, nil
				}
				return []videomodel.Video{v10, v20, v30}, nil // 数据库顺序故意与请求不同。
			}}
			reader := NewVideoReader(repo, cache)

			result, err := reader.GetByIDs(t.Context(), []uint{30, 10, 20})

			require.Equal(t, [][]uint{{30, 10, 20}}, repo.batchCalls, "多个未命中只做一次批量查询")
			require.Empty(t, repo.findCalls)
			if scenario == "database_failed" {
				require.ErrorIs(t, err, dbErr)
				require.Nil(t, result)
				require.Empty(t, cache.setTTLs)
				return
			}
			require.NoError(t, err)
			if scenario == "none_exist" {
				require.NotNil(t, result)
				require.Empty(t, result)
				return
			}
			require.Equal(t, []videomodel.Video{v30, v10, v20}, result)
			for _, v := range result {
				assertReaderL1(t, reader, v)
				require.Equal(t, time.Hour, cache.setTTLs[cache.Key("video:entity:%d", v.ID)])
			}
			second, err := reader.GetByIDs(t.Context(), []uint{20, 30})
			require.NoError(t, err)
			require.Equal(t, []videomodel.Video{v20, v30}, second)
			require.Len(t, repo.batchCalls, 1)
		})
	}
}

func TestVideoReaderGetByIDsEmpty(t *testing.T) {
	for _, ids := range [][]uint{nil, {}} {
		cache, repo := newReaderTestCache(), &readerTestRepo{}
		reader := NewVideoReader(repo, cache)
		result, err := reader.GetByIDs(t.Context(), ids)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Empty(t, result)
		require.Empty(t, repo.batchCalls)
		require.Empty(t, cache.mgetKeys)
	}
}

func TestVideoReaderExpiredL1FallsBack(t *testing.T) {
	cache, repo := newReaderTestCache(), &readerTestRepo{}
	reader := NewVideoReader(repo, cache)
	old := readerTestVideo(42)
	fresh := old
	fresh.LikedCount = 6
	// 注入已过期条目，不使用 Sleep，也不引入主动失效行为。
	reader.localcache = localcache.NewFrom(l1CacheTTL, 0, map[string]localcache.Item{
		"video:entity:42": {Object: old.ToVideoEntityCache(), Expiration: time.Now().Add(-time.Second).UnixNano()},
	})
	cache.values["video:entity:42"] = readerTestJSON(t, fresh)

	result, err := reader.GetByID(t.Context(), 42)

	require.NoError(t, err)
	require.Equal(t, fresh, *result)
	require.Empty(t, repo.findCalls)
	assertReaderL1(t, reader, fresh)
}

func TestVideoReaderConcurrentGetByID(t *testing.T) {
	const requests = 16
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cache := newReaderTestCache()
	lookups := make(chan struct{}, requests*2)
	cache.afterGet = func(int) { lookups <- struct{}{} }
	entered := make(chan struct{}, requests)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	v := readerTestVideo(42)
	repo := &readerTestRepo{find: func(ctx context.Context, id uint) (*videomodel.Video, error) {
		entered <- struct{}{}
		select {
		case <-release:
			return &v, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}
	reader := NewVideoReader(repo, cache)
	type answer struct {
		video *videomodel.Video
		err   error
	}
	answers := make(chan answer, requests)
	start := make(chan struct{})
	for range requests {
		go func() {
			<-start
			result, err := reader.GetByID(ctx, 42)
			answers <- answer{video: result, err: err}
		}()
	}
	close(start)
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("数据库回源未启动")
	}
	// 回源保持阻塞，确保所有调用都经过缓存读取，再允许回填。
	for range requests {
		select {
		case <-lookups:
		case <-ctx.Done():
			t.Fatal("并发请求未全部开始读取")
		}
	}
	releaseOnce.Do(func() { close(release) })
	seen := make(map[*videomodel.Video]bool)
	results := make([]*videomodel.Video, 0, requests)
	for range requests {
		select {
		case ans := <-answers:
			require.NoError(t, ans.err)
			require.NotNil(t, ans.video)
			require.Equal(t, v, *ans.video)
			require.False(t, seen[ans.video], "不同调用者不应共享返回指针")
			seen[ans.video] = true
			results = append(results, ans.video)
		case <-ctx.Done():
			t.Fatal("并发读取未结束")
		}
	}
	repo.mu.Lock()
	findCalls := append([]uint(nil), repo.findCalls...)
	repo.mu.Unlock()
	require.Equal(t, []uint{42}, findCalls)
	results[0].Title = "caller-only change"
	for _, result := range results[1:] {
		require.Equal(t, "video-42", result.Title)
	}
	require.Equal(t, "video-42", v.Title)
}

func TestVideoReaderGetByIDLoadsTagIDs(t *testing.T) {
	cache := newReaderTestCache()
	v := readerTestVideo(42)
	v.TagIDs = nil
	repo := &readerTestRepo{
		find: func(context.Context, uint) (*videomodel.Video, error) { return &v, nil },
		tags: map[uint][]uint{42: {7, 8}},
	}
	reader := NewVideoReader(repo, cache)

	result, err := reader.GetByID(t.Context(), 42)

	require.NoError(t, err)
	require.Equal(t, []uint{7, 8}, result.TagIDs)
	require.Equal(t, [][]uint{{42}}, repo.tagIDCalls())

	expected := v
	expected.TagIDs = []uint{7, 8}
	assertReaderL1(t, reader, expected)
}

func TestVideoReaderGetByIDsLoadsTagIDs(t *testing.T) {
	cache := newReaderTestCache()
	v30, v10 := readerTestVideo(30), readerTestVideo(10)
	v30.TagIDs, v10.TagIDs = nil, nil
	repo := &readerTestRepo{
		batch: func(context.Context, []uint) ([]videomodel.Video, error) {
			return []videomodel.Video{v10, v30}, nil
		},
		tags: map[uint][]uint{
			30: {1, 2},
			10: {3},
		},
	}
	reader := NewVideoReader(repo, cache)

	result, err := reader.GetByIDs(t.Context(), []uint{30, 10})

	require.NoError(t, err)
	require.Len(t, result, 2)
	require.Equal(t, []uint{1, 2}, result[0].TagIDs)
	require.Equal(t, []uint{3}, result[1].TagIDs)
	tagCalls := repo.tagIDCalls()
	require.Len(t, tagCalls, 1)
	require.ElementsMatch(t, []uint{30, 10}, tagCalls[0], "批量查询的 ID 顺序不影响结果")

	expected30 := readerTestVideo(30)
	expected30.TagIDs = []uint{1, 2}
	expected10 := readerTestVideo(10)
	expected10.TagIDs = []uint{3}
	assertReaderL1(t, reader, expected30)
	assertReaderL1(t, reader, expected10)
}

func TestVideoReaderFillTagIDs(t *testing.T) {
	repo := &readerTestRepo{tags: map[uint][]uint{1: {4, 5}}}
	reader := NewVideoReader(repo, newReaderTestCache())

	videos := []videomodel.Video{
		{ID: 1, TagIDs: nil},
		{ID: 2, TagIDs: []uint{}},
		{ID: 3, TagIDs: []uint{9}},
	}

	require.NoError(t, reader.FillTagIDs(t.Context(), videos))
	require.Equal(t, []uint{4, 5}, videos[0].TagIDs)
	require.NotNil(t, videos[1].TagIDs)
	require.Empty(t, videos[1].TagIDs)
	require.Equal(t, []uint{9}, videos[2].TagIDs)
	require.Equal(t, [][]uint{{1}}, repo.tagIDCalls(), "只查询 TagIDs 为 nil 的视频")

	// 已经加载过的 TagIDs 不应重复查询。
	require.NoError(t, reader.FillTagIDs(t.Context(), videos))
	require.Equal(t, [][]uint{{1}}, repo.tagIDCalls())
}

func TestVideoReaderFillTagIDsError(t *testing.T) {
	dbErr := errors.New("tag db down")
	repo := &readerTestRepo{tagErr: dbErr}
	reader := NewVideoReader(repo, newReaderTestCache())

	videos := []videomodel.Video{{ID: 1}}

	err := reader.FillTagIDs(t.Context(), videos)

	require.ErrorIs(t, err, dbErr)
	require.Nil(t, videos[0].TagIDs, "查询失败时应保持 nil，留待下次重试")
}
