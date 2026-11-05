package video

import (
	"context"
	"feedsystem/internal/model/video"
)

// 提供视频读取服务

type VideoQueryService struct {
	reader  *VideoReader
	builder ViewBuilder
	repo    VideoRepo
}

func NewVideoQueryService(reader *VideoReader, builder ViewBuilder, repo VideoRepo) *VideoQueryService {
	return &VideoQueryService{
		reader:  reader,
		builder: builder,
		repo:    repo,
	}
}

// ListResult 分页结果
type ListResult struct {
	Items      []video.VideoView `json:"items"`
	NextCursor string            `json:"next_cursor"` // 空串 = 没有更多
}

func (s *VideoQueryService) GetByID(ctx context.Context, id uint) (*video.VideoView, error) {
	video, err := s.reader.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	view, err := s.builder.BuildOne(ctx, *video)
	return &view, err
}
