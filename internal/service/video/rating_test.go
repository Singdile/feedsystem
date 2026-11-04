package video

import (
	"context"
	"errors"
	videomodel "feedsystem/internal/model/video"
	apperrors "feedsystem/internal/pkg/errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// 查询测试只替换存储依赖，不涉及真实数据库、Redis、MQ 或热度更新。
type ratingTestRepo struct{ mock.Mock }

func (r *ratingTestRepo) SetRating(ctx context.Context, videoID, accountID uint, status int8) error {
	return r.Called(ctx, videoID, accountID, status).Error(0)
}

func (r *ratingTestRepo) GetRating(ctx context.Context, videoID, accountID uint) (int8, error) {
	args := r.Called(ctx, videoID, accountID)
	return args.Get(0).(int8), args.Error(1)
}

func (r *ratingTestRepo) GetRatings(ctx context.Context, videoIDs []uint, accountID uint) (map[uint]int8, error) {
	args := r.Called(ctx, videoIDs, accountID)
	statuses, _ := args.Get(0).(map[uint]int8)
	return statuses, args.Error(1)
}

func (r *ratingTestRepo) ListLikedVideos(ctx context.Context, accountID uint, cursor *videomodel.Cursor, limit int) ([]videomodel.Video, error) {
	args := r.Called(ctx, accountID, cursor, limit)
	videos, _ := args.Get(0).([]videomodel.Video)
	return videos, args.Error(1)
}

type ratingTestCache struct{ mock.Mock }

func (c *ratingTestCache) UpdateRatingCache(ctx context.Context, accountID, videoID uint, status int8) error {
	return c.Called(ctx, accountID, videoID, status).Error(0)
}

func (c *ratingTestCache) GetRatingCache(ctx context.Context, accountID uint, videoIDs ...uint) ([]int8, error) {
	args := c.Called(ctx, accountID, videoIDs)
	statuses, _ := args.Get(0).([]int8)
	return statuses, args.Error(1)
}

var (
	_ RateRepo    = (*ratingTestRepo)(nil)
	_ RatingCache = (*ratingTestCache)(nil)
)

func newRatingQueryTest(t *testing.T, useCache bool) (*RatingService, *ratingTestRepo, *ratingTestCache) {
	t.Helper()
	repo := &ratingTestRepo{}
	repo.Test(t)
	t.Cleanup(func() { repo.AssertExpectations(t) })
	var cache RatingCache
	var cacheMock *ratingTestCache
	if useCache {
		cacheMock = &ratingTestCache{}
		cacheMock.Test(t)
		t.Cleanup(func() { cacheMock.AssertExpectations(t) })
		cache = cacheMock
	}
	return NewRatingService(repo, nil, nil, nil, cache), repo, cacheMock
}

func TestRatingServiceGetUserRatings(t *testing.T) {
	cacheErr := errors.New("redis unavailable")
	dbErr := errors.New("database unavailable")
	for _, tc := range []struct {
		name       string
		useCache   bool
		cached     []int8
		cacheErr   error
		readDB     bool
		dbStatuses map[uint]int8
		dbErr      error
		want       map[uint]int8
	}{
		{name: "cache_hit", useCache: true, cached: []int8{1, -1, 0}, want: map[uint]int8{30: 1, 10: -1, 20: 0}},
		{name: "cached_unrated_is_a_hit", useCache: true, cached: []int8{0, 0, 0}, want: map[uint]int8{30: 0, 10: 0, 20: 0}},
		{name: "cache_failure_falls_back", useCache: true, cacheErr: cacheErr, readDB: true, dbStatuses: map[uint]int8{10: 1, 30: -1}, want: map[uint]int8{10: 1, 30: -1}},
		{name: "no_cache", readDB: true, dbStatuses: map[uint]int8{30: 1}, want: map[uint]int8{30: 1}},
		{name: "database_empty", readDB: true, dbStatuses: map[uint]int8{}, want: map[uint]int8{}},
		{name: "cache_and_database_fail", useCache: true, cacheErr: cacheErr, readDB: true, dbErr: dbErr},
		{name: "database_failure_without_cache", readDB: true, dbErr: dbErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc, repo, cache := newRatingQueryTest(t, tc.useCache)
			ids := []uint{30, 10, 20} // 不按 ID 排序，检查数组到 ID 映射是否正确。
			if tc.useCache {
				cache.On("GetRatingCache", t.Context(), uint(7), ids).Return(tc.cached, tc.cacheErr).Once()
			}
			if tc.readDB {
				// 仓储参数顺序是 videoIDs、accountID，和 Service 不同。
				repo.On("GetRatings", t.Context(), ids, uint(7)).Return(tc.dbStatuses, tc.dbErr).Once()
			}

			result, err := svc.GetUserRatings(t.Context(), 7, ids)

			if tc.dbErr != nil {
				require.ErrorIs(t, err, tc.dbErr)
				require.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, result)
			}
			require.Equal(t, []uint{30, 10, 20}, ids)
			if !tc.readDB {
				repo.AssertNotCalled(t, "GetRatings", mock.Anything, mock.Anything, mock.Anything)
			}
		})
	}
}

func TestRatingServiceGetUserRating(t *testing.T) {
	cacheErr := errors.New("redis unavailable")
	dbErr := errors.New("database unavailable")
	for _, tc := range []struct {
		name     string
		useCache bool
		cached   []int8
		cacheErr error
		readDB   bool
		dbStatus int8
		dbErr    error
		want     int8
	}{
		{name: "cached_like", useCache: true, cached: []int8{1}, want: 1},
		{name: "cached_dislike", useCache: true, cached: []int8{-1}, want: -1},
		{name: "cached_unrated", useCache: true, cached: []int8{0}},
		{name: "cache_failure_falls_back", useCache: true, cacheErr: cacheErr, readDB: true, dbStatus: -1, want: -1},
		{name: "no_cache", readDB: true, dbStatus: 1, want: 1},
		{name: "database_unrated", readDB: true},
		{name: "cache_and_database_fail", useCache: true, cacheErr: cacheErr, readDB: true, dbErr: dbErr},
		{name: "database_failure_without_cache", readDB: true, dbErr: dbErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc, repo, cache := newRatingQueryTest(t, tc.useCache)
			if tc.useCache {
				cache.On("GetRatingCache", t.Context(), uint(7), []uint{30}).Return(tc.cached, tc.cacheErr).Once()
			}
			if tc.readDB {
				repo.On("GetRating", t.Context(), uint(30), uint(7)).Return(tc.dbStatus, tc.dbErr).Once()
			}

			result, err := svc.GetUserRating(t.Context(), 7, 30)

			if tc.dbErr != nil {
				require.ErrorIs(t, err, tc.dbErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, result)
			if !tc.readDB {
				repo.AssertNotCalled(t, "GetRating", mock.Anything, mock.Anything, mock.Anything)
			}
		})
	}
}

func TestRatingServiceQueryInvalidParameters(t *testing.T) {
	for _, tc := range []struct {
		name      string
		batch     bool
		accountID uint
		videoID   uint
		videoIDs  []uint
	}{
		{name: "single_missing_account", videoID: 30},
		{name: "single_missing_video", accountID: 7},
		{name: "batch_missing_account", batch: true, videoIDs: []uint{30}},
		{name: "batch_nil_ids", batch: true, accountID: 7},
		{name: "batch_empty_ids", batch: true, accountID: 7, videoIDs: []uint{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc, repo, cache := newRatingQueryTest(t, true)
			var err error
			if tc.batch {
				var statuses map[uint]int8
				statuses, err = svc.GetUserRatings(t.Context(), tc.accountID, tc.videoIDs)
				require.Nil(t, statuses)
			} else {
				var status int8
				status, err = svc.GetUserRating(t.Context(), tc.accountID, tc.videoID)
				require.Zero(t, status)
			}
			require.Error(t, err)
			var appErr *apperrors.AppError
			require.ErrorAs(t, err, &appErr)
			require.Equal(t, http.StatusBadRequest, appErr.Status)
			require.Empty(t, repo.Calls, "非法输入不应访问数据库")
			require.Empty(t, cache.Calls, "非法输入不应访问缓存")
		})
	}
}
