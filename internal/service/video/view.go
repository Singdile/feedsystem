package video

import (
	"context"
	videomodel "feedsystem/internal/model/video"
	apperrors "feedsystem/internal/pkg/errors"
	"log"
	"net/http"
	"time"
)

type VideoViewBuilder struct {
	singer       URLSigner
	tagsProvider TagsProvider
}

type ViewBuilFailure struct {
	Index   int // 在输入的切片中的位置
	VideoID uint
	Err     error
}

type ViewBuildResult struct {
	Items    []videomodel.VideoView
	Failures []ViewBuilFailure
}

// 生成播放、封面链接
type URLSigner interface {
	PresignedGetObject(ctx context.Context, objectKey string, expiry time.Duration) (string, error)
}

type TagsProvider interface {
	GetTags(ctx context.Context, videos []videomodel.Video) (map[uint][]string, error)
}

func NewVideoViewBuilder(signer URLSigner, tagsProvider TagsProvider) *VideoViewBuilder {
	return &VideoViewBuilder{
		singer:       signer,
		tagsProvider: tagsProvider,
	}
}

func (b *VideoViewBuilder) BuildSome(ctx context.Context, videos []videomodel.Video) ViewBuildResult {
	views := make([]videomodel.VideoView, 0, len(videos))
	failures := make([]ViewBuilFailure, 0)
	kept := make([]videomodel.Video, 0, len(videos))
	for i, v := range videos {
		view, err := b.videoView(ctx, &v)
		if err != nil {
			failures = append(failures, ViewBuilFailure{
				Index:   i,
				VideoID: v.ID,
				Err:     err,
			})
			continue
		}
		views = append(views, view)
		kept = append(kept, v)
	}

	// 获取视频tags
	tagMap, err := b.tagsProvider.GetTags(ctx, kept)
	if err != nil {
		log.Printf("获取tag失败，err: %v", err)
	}
	for i := range views {
		views[i].HashTags = tagMap[views[i].ID]
	}

	return ViewBuildResult{Items: views, Failures: failures}
}

func (b *VideoViewBuilder) BuildOne(ctx context.Context, video videomodel.Video) (videomodel.VideoView, error) {
	view, err := b.videoView(ctx, &video)
	if err != nil {
		return videomodel.VideoView{}, err
	}

	tagMap, err := b.tagsProvider.GetTags(ctx, []videomodel.Video{video})
	if err != nil {
		log.Printf("获取tag失败,err: %v", err)
	}

	view.HashTags = tagMap[view.ID]
	return view, nil
}

func (b *VideoViewBuilder) videoView(ctx context.Context, v *videomodel.Video) (videomodel.VideoView, error) {
	play, err := b.singer.PresignedGetObject(ctx, v.VideoKey, playExpiry)
	if err != nil {
		return videomodel.VideoView{}, apperrors.NewAppError(http.StatusInternalServerError, "生成播放地址失败")
	}
	if v.CoverKey == "" {
		return videomodel.VideoView{}, apperrors.NewAppError(http.StatusInternalServerError, "封面数据缺失")
	} // 强制封面
	cover, err := b.singer.PresignedGetObject(ctx, v.CoverKey, playExpiry)
	if err != nil {
		return videomodel.VideoView{}, apperrors.NewAppError(http.StatusInternalServerError, "生成封面地址失败")
	}

	return videomodel.VideoView{
		ID:          v.ID,
		Title:       v.Title,
		Description: v.Description,
		Author: videomodel.Author{
			ID:       v.AuthorID,
			Username: v.Username,
		},
		PlayURL:       play,
		CoverURL:      cover,
		LikedCount:    v.LikedCount,
		DislikedCount: v.DislikedCount,
		CommentCount:  v.CommentCount,
		CreatedAt:     v.CreatedAt,
	}, nil
}
