// Package feed 提供视频流服务
package feed

import (
	"context"
	"feedsystem/internal/model/feed"
	"feedsystem/internal/model/video"
	apperrors "feedsystem/internal/pkg/errors"
	videosvc "feedsystem/internal/service/video"
	"log"
	"net/http"
	"strconv"
	"time"
)

type FeedRepo interface {
	ZRevRangeByScore(ctx context.Context, key string, maxScore float64, maxID uint, limit int) ([]feed.ZMember, error)
}

type FeedService struct {
	repo     FeedRepo
	videoSvc *videosvc.VideoService
}

func NewFeedService(repo FeedRepo, videoSvc *videosvc.VideoService) *FeedService {
	return &FeedService{repo: repo, videoSvc: videoSvc}
}

// FeedListResult feed 响应（Items 复用 video.VideoView）
type FeedListResult struct {
	Items      []video.VideoView `json:"items"`
	NextCursor string            `json:"next_cursor"` // 空 = 没有更多
}

// ListFeed 骨架：先返回空
func (s *FeedService) ListFeed(ctx context.Context, cursorStr string, limit int) (*FeedListResult, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	var curScore float64
	var curID uint
	if cursorStr != "" {
		cur, err := video.DecodeCursor(cursorStr)
		if err != nil {
			return nil, apperrors.NewAppError(http.StatusBadRequest, "bad_cursor")
		}
		curScore = float64(cur.CreatedAt.UnixMilli())
		curID = cur.ID
	}

	// 查询缓存中的timeline,获取时间排序的视频
	members, err := s.repo.ZRevRangeByScore(ctx, "feed:global_timeline", curScore, curID, limit+1)
	if err != nil {
		return nil, apperrors.NewAppError(http.StatusInternalServerError, "feed failed")
	}

	hasmore := len(members) > limit
	if hasmore {
		members = members[:limit]
	}

	ids := make([]uint, 0, len(members))
	for _, v := range members {
		videID, err := strconv.ParseUint(v.Member, 10, 64)
		if err != nil {
			log.Printf("failed to parse video id %s from feed_global_timeline", v.Member)
			continue
		}
		ids = append(ids, uint(videID))
	}

	// nextcursor
	next := ""
	if hasmore {
		last := members[len(members)-1]
		id, _ := strconv.ParseUint(last.Member, 10, 64)
		next = video.EncodeCursor(video.Cursor{CreatedAt: time.UnixMilli(int64(last.Score)), ID: uint(id)})
	}

	// 从数据库中获取详细信息
	videoViews, err := s.videoSvc.GetVideosByIDs(ctx, ids)
	if err != nil {
		return nil, apperrors.NewAppError(http.StatusInternalServerError, "get videos failed")
	}

	if len(videoViews) == 0 {
		return &FeedListResult{}, nil
	}
	items := make([]video.VideoView, 0, len(videoViews))
	for _, v := range videoViews {
		items = append(items, *v)
	}

	return &FeedListResult{Items: items, NextCursor: next}, nil
}
