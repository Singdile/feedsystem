package video

import (
	"context"
	"feedsystem/internal/model/video"
	apperrors "feedsystem/internal/pkg/errors"
	"net/http"
)

// RateRepo 负责video_tags 表的操作(DB)
type RateRepo interface {
	SetRating(ctx context.Context, videoID, accountID uint, stauts int8) error
	GetRating(ctx context.Context, videoID, accountID uint) (int8, error)
	GetRatings(ctx context.Context, videoID []uint, accountID uint) (map[uint]int8, error)
	ListLikedVideos(ctx context.Context, accountID uint, cursorStr *video.Cursor, limit int) ([]video.Video, error)
}

// RatingMQ 发送 Rating 事件到MQ
type RatingMQ interface {
	PublishRating(ctx context.Context, action string, accountID, videoID uint) error
}

type RatingService struct {
	RateRepo    RateRepo
	VideoRepo   VideoDB
	ObjectStore ObjectStore
	MQ          RatingMQ
}

// NewRatingService 构造评价服务
func NewRatingService(repo RateRepo, dbRepo VideoDB, mq RatingMQ) *RatingService {
	return &RatingService{
		RateRepo:  repo,
		VideoRepo: dbRepo,
		MQ:        mq,
	}
}

// SetUserRating 接收用户id，视频id，rating status，对视频进行rating操作
func (s *RatingService) SetUserRating(ctx context.Context, accountID, videoID uint, status int8) (stat int8, err error) {
	// 参数校验
	if accountID == 0 || videoID == 0 {
		return 0, apperrors.NewAppError(http.StatusBadRequest, "参数错误")
	}

	if status != -1 && status != 0 && status != 1 {
		return 0, apperrors.NewAppError(http.StatusBadRequest, "参数错误")
	}

	// 正常路径：发送信息到mq
	if s.MQ != nil {
		if err := s.MQ.PublishRating(ctx, video.StatusToString(status), accountID, videoID); err == nil {
			return status, nil // 信息发送成功
		}
	}

	// 降级路径：直接DB写操作
	err = s.RateRepo.SetRating(ctx, videoID, accountID, status)
	if err != nil {
		return 0, err
	}

	stat = status
	return stat, err
}

// GetUserRating 接收用户id，视频id，查询用户对视频的rating
func (s *RatingService) GetUserRating(ctx context.Context, accountID uint, videoID uint) (stat int8, err error) {
	if accountID == 0 || videoID == 0 {
		return 0, apperrors.NewAppError(http.StatusBadRequest, "参数错误")
	}

	stat, err = s.RateRepo.GetRating(ctx, videoID, accountID)
	if err != nil {
		return 0, err
	}

	return stat, nil
}

// GetUserRatings 接收用户id，视频ids，查询用户对一批视频的评价
func (s *RatingService) GetUserRatings(ctx context.Context, accountID uint, videoID []uint) (stat map[uint]int8, err error) {
	if accountID == 0 || len(videoID) == 0 {
		return nil, apperrors.NewAppError(http.StatusBadRequest, "参数错误")
	}

	return s.RateRepo.GetRatings(ctx, videoID, accountID)
}

// ListLikedVideos 查询用户liked 的视频
func (s *RatingService) ListLikedVideos(ctx context.Context, accountID uint, cursorStr string, limit int) (videos []video.Video, nextCursor string, err error) {
	if accountID == 0 {
		return nil, "", apperrors.NewAppError(http.StatusBadRequest, "参数错误")
	}

	// 解析游标
	var cursor *video.Cursor
	if cursorStr == "" {
		cursor = nil
	} else {
		cur, _ := video.DecodeCursor(cursorStr)
		cursor = &cur
	}

	// 查询
	videos, err = s.RateRepo.ListLikedVideos(ctx, accountID, cursor, limit+1)
	if err != nil {
		return nil, "", err
	}

	hasmore := len(videos) > limit
	if hasmore {
		videos = videos[:limit]
	}

	// 更新游标
	next := ""
	if hasmore && len(videos) > 0 { // 有下一页游标更新，否则返回""
		last := videos[len(videos)-1]
		next = video.EncodeCursor(video.Cursor{
			CreatedAt: last.CreatedAt,
			ID:        last.ID,
		})
	}

	// 返回数据以及游标; 游标为"" 表示没有下一页了
	return videos, next, nil
}
