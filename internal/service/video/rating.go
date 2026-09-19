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
	GetStatus(ctx context.Context, videoID, accountID uint) (int8, error)
	ListLikedVideos(ctx context.Context, accountID uint) ([]video.Video, error)
}

// RatingMQ 发送 Rating 事件到MQ
type RatingMQ interface {
	PublishRating(ctx context.Context, actiong string, userID, videoID uint) error
}

type RatingService struct {
	RateRepo  RateRepo
	VideoRepo VideoDB
	MQ        RatingMQ
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

	return 0, nil
}

// ListLikedVideos 查询用户liked 的视频，并返回可播放视频数据(分页)
func (s *RatingService) ListLikedVideos(ctx context.Context, accountID uint, cursorStr string) (views []video.VideoView, nextCursor string, err error) {
	return nil, "", nil
}
