// Package feed 提供视频流服务
package feed

import (
	"context"
	"feedsystem/internal/model/feed"
	"feedsystem/internal/model/video"
	apperrors "feedsystem/internal/pkg/errors"
	videsvc "feedsystem/internal/service/video"
	"log"
	"net/http"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	timelineKey    = "feed:global_timeline"
	entityCacheTTL = time.Hour       // L2 实体缓存
	l1CacheTTL     = 5 * time.Second // L1 本地缓存
)

type FeedRepo interface {
	// 时间线操作,负责索引拿到显示vidoID
	// ZRevRangeByScore 返回 zset 中 score <= maxScore 的元素，按 score 从大到小排序，返回 limit 个
	ZRevRangeByScore(ctx context.Context, key string, maxScore float64, maxID uint, limit int) ([]feed.ZMember, error)
	ZRangeWithScores(ctx context.Context, key string, start, stop int64) ([]feed.ZMember, error)
	ZAdd(ctx context.Context, key string, member feed.ZMember) error
	ZAddBatch(ctx context.Context, key string, members []feed.ZMember) error
	ZUnionStore(ctx context.Context, dest string, keys []string, aggregate string) error
	Expire(ctx context.Context, key string, expire time.Duration) error
	ZRevRange(ctx context.Context, key string, start, stop int64) ([]feed.ZMember, error) //逆序

	// 冷数据查询
	ListLatest(ctx context.Context, cursor *video.Cursor, limit int) ([]video.Video, error)
	ListLatestByAuthors(ctx context.Context, authordIDs []uint, cursor *video.Cursor, limit int) ([]video.Video, error)

	// 实体缓存 (L2 redis)
	Key(format string, a ...any) string
	MGet(ctx context.Context, keys ...string) ([]any, error)
	GetBytes(ctx context.Context, key string) ([]byte, error)
	SetBytes(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Exists(ctx context.Context, key string) (bool, error)
	Del(ctx context.Context, key string) error
}

type VideoProvider interface {
	ListByTag(ctx context.Context, tagName string, cursor *video.Cursor, limit int) ([]video.Video, error)
}

type RatingProvider interface {
	GetUserRatings(ctx context.Context, accountID uint, videoID []uint) (stat map[uint]int8, err error)
}

// 返回关注作者列表
type FollowProvider interface {
	ListFollowingIDs(ctx context.Context, fanID uint) ([]uint, error)
}

type VideoReader interface {
	GetByIDs(ctx context.Context, ids []uint) ([]video.Video, error)
}

// fetcher 按照游标最多取 limit 条数据
// bool: 表示是否还有更多
type fetcher func(cursor *video.Cursor, limit int) ([]video.Video, bool, error)

type FeedService struct {
	repo           FeedRepo
	videoReader    VideoReader
	videoSvc       VideoProvider
	ratingSvc      RatingProvider
	followProvider FollowProvider
	viewBuilder    videsvc.ViewBuilder
	group          singleflight.Group // 防并发
}

func NewFeedService(repo FeedRepo, videoReader VideoReader, videoSvc VideoProvider, ratingSvc RatingProvider, followProvider FollowProvider, viewbuilder videsvc.ViewBuilder) *FeedService {
	return &FeedService{
		repo:           repo,
		videoReader:    videoReader,
		videoSvc:       videoSvc,
		ratingSvc:      ratingSvc,
		followProvider: followProvider,
		viewBuilder:    viewbuilder,
	}
}

// FeedListResult feed 响应（Items 复用 video.VideoView）
type FeedListResult struct {
	Items      []video.VideoView `json:"items"`
	NextCursor string            `json:"next_cursor"` // 空 = 没有更多
}

// runFeed 所有分页游标 Feed 的公共流程：
// 游标解析->取出视频实体数据->签发视频播放链接->查看用户点赞状态->返回视频播放列表和下一页游标信息
func (s *FeedService) runFeed(ctx context.Context, accountID uint, cursorStr string, limit int, fetch fetcher) (*FeedListResult, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}

	cursor, err := decodeCursor(cursorStr)
	if err != nil {
		return nil, err
	}

	vs, hasmore, err := fetch(cursor, limit)
	if err != nil {
		return nil, err
	}

	views, err := s.buildViews(ctx, vs)
	if err != nil {
		return nil, err
	}

	if len(views) > 0 {
		if _, err := s.stampStatus(ctx, accountID, views); err != nil {
			log.Printf("fail to get user's rating status,err: %v", err)
		}
	}

	return &FeedListResult{Items: views, NextCursor: encodeNextCursor(vs, hasmore)}, nil

}

func decodeCursor(cursorStr string) (*video.Cursor, error) {
	if cursorStr == "" {
		return nil, nil
	}
	cur, err := video.DecodeCursor(cursorStr)
	if err != nil {
		return nil, apperrors.NewAppError(http.StatusBadRequest, "bad_cursor")
	}
	return &cur, nil
}

func encodeNextCursor(videos []video.Video, hasMore bool) string {
	if !hasMore || len(videos) == 0 {
		return ""
	}

	last := videos[len(videos)-1]
	return video.EncodeCursor(video.Cursor{CreatedAt: last.CreatedAt, ID: last.ID})
}

func (s *FeedService) buildViews(ctx context.Context, videos []video.Video) ([]video.VideoView, error) {
	result := s.viewBuilder.BuildSome(ctx, videos)
	for _, failure := range result.Failures {
		log.Printf("视频展示失败： video_id = %d,err= %v", failure.VideoID, failure.Err)
	}

	if len(result.Items) == 0 && len(result.Failures) > 0 {
		return nil, apperrors.NewAppError(http.StatusInternalServerError, "视频展示无法播放")
	}

	return result.Items, nil
}



// stampStatus 查询用户用户对于一批视频的评价，匿名用户保持评价的默认值就行了
func (s *FeedService) stampStatus(ctx context.Context, accountID uint, views []video.VideoView) ([]video.VideoView, error) {
	if accountID == 0 || len(views) == 0 {
		return views, nil
	}

	ids := make([]uint, 0, len(views))
	for _, v := range views {
		ids = append(ids, v.ID)
	}

	// 查询rating
	statMap, err := s.ratingSvc.GetUserRatings(ctx, accountID, ids)
	if err != nil {
		return views, err
	}

	for i, v := range views {
		views[i].Status = statMap[v.ID]
	}
	return views, nil
}

