package video

import (
	"context"
	"errors"
	"feedsystem/internal/model/video"
	apperrors "feedsystem/internal/pkg/errors"
	"net/http"

	"gorm.io/gorm"
)

// CommentRepo 评论DB操作
type CommentRepo interface {
	Create(ctx context.Context, c *video.Comment) error
	Delete(ctx context.Context, commentID uint) error
	Update(ctx context.Context, c *video.Comment) error
	GetByID(ctx context.Context, commentID uint) (*video.Comment, error)
	ListByVideoID(ctx context.Context, videoID uint, cursor *video.Cursor, limit int8) ([]*video.Comment, error)
}

// CommentMQ 发送信息到MQ的操作
type CommentMQ interface {
	Publish(ctx context.Context, c *video.Comment) error
	Delete(ctx context.Context, commentID uint) error
}

// VideoChecker 检查视频是否存在
type VideoChecker interface {
	FindByID(ctx context.Context, videoID uint) (*video.Video, error)
}

type CommentService struct {
	Repo      CommentRepo
	MQ        CommentMQ
	VideoRepo VideoChecker
}

func NewCommentService(repo CommentRepo, mq CommentMQ, checker VideoChecker) *CommentService {
	return &CommentService{
		Repo:      repo,
		MQ:        mq,
		VideoRepo: checker,
	}
}

// Publish 提供提交用户对视频的评论服务
// 优先将评论事件提交到 MQ 上；失败则降级，直接写 DB
func (s *CommentService) Publish(ctx context.Context, videoID, accountID uint, username, content string) error {
	// 参数校验
	if videoID == 0 || accountID == 0 || content == "" {
		return apperrors.NewAppError(http.StatusBadRequest, "参数错误")
	}

	if s.VideoRepo == nil {
		return apperrors.NewAppError(http.StatusInternalServerError, "视频服务未连接")
	}

	v, err := s.VideoRepo.FindByID(ctx, videoID) //检验视频是否存在
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return apperrors.NewAppError(http.StatusBadRequest, "评价视频不存在")
		}
		return apperrors.NewAppError(http.StatusInternalServerError, "查询视频失败")
	}

	comment := &video.Comment{
		VideoID:   videoID,
		AccountID: accountID,
		Content:   content,
		UserName:  username,
		AuthorID:  v.AuthorID,
	}

	// 优先提交到 MQ
	if s.MQ != nil {
		if err := s.MQ.Publish(ctx, comment); err == nil {
			return nil
		}
	}

	// 失败降级走 DB
	if s.Repo == nil {
		return apperrors.NewAppError(http.StatusInternalServerError, "数据库未连接")
	}

	if err := s.Repo.Create(ctx, comment); err != nil {
		return apperrors.NewAppError(http.StatusInternalServerError, "创建失败")
	}

	return nil
}

// Delete 视频作者删除视频相关的评论
// 优先将删除事件发送到 MQ 上；失败则降级，直接写 DB
func (s *CommentService) Delete(ctx context.Context, accountID, commentID uint) error {
	// 参数校验
	if accountID == 0 || commentID == 0 {
		return apperrors.NewAppError(http.StatusBadRequest, "参数错误")
	}

	// 检查commentID 对应的 comment 实体是否存在，以及相关信息
	comment, err := s.Repo.GetByID(ctx, commentID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return apperrors.NewAppError(http.StatusBadRequest, "评论不存在")
		}
		return apperrors.NewAppError(http.StatusInternalServerError, "删除失败，内部错误")
	}

	if comment.AuthorID != accountID && comment.AccountID != accountID {
		return apperrors.NewAppError(http.StatusUnauthorized, "仅视频创作者或者评价者可以删除评价")
	}

	// 发送MQ事件
	if s.MQ != nil {
		if err := s.MQ.Delete(ctx, commentID); err == nil {
			return nil
		}
	}

	// MQ发送失败，降级到DB
	if s.Repo == nil {
		return apperrors.NewAppError(http.StatusInternalServerError, "数据库未连接")
	}

	if err := s.Repo.Delete(ctx, commentID); err != nil {
		return apperrors.NewAppError(http.StatusInternalServerError, "数据库删除失败，内部错误")
	}
	return nil
}

// ListComments 查询视频的评论列表,查询DB
func (s *CommentService) ListComments(ctx context.Context, videoID uint, cursorStr string, limit int8) ([]*video.Comment, string, error) {
	// 参数检验
	if videoID == 0 {
		return nil, "", apperrors.NewAppError(http.StatusBadRequest, "参数错误")
	}

	if limit < 0 || limit > 100 {
		limit = 20
	}

	// 转换游标
	var cursor *video.Cursor
	if cursorStr != "" {
		cur, err := video.DecodeCursor(cursorStr)
		if err == nil {
			cursor = &cur
		}
	}

	// 数据库查询
	comments, err := s.Repo.ListByVideoID(ctx, videoID, cursor, limit+1)
	if err != nil {
		return nil, "", err
	}

	hasmore := len(comments) > int(limit)
	if hasmore {
		comments = comments[:limit]
	}

	// 计算游标
	next := ""
	if hasmore && len(comments) > 0 {
		lastComment := comments[len(comments)-1]
		next = video.EncodeCursor(video.Cursor{
			CreatedAt: lastComment.CreatedAt,
			ID:        lastComment.ID,
		})
	}

	// 返回评论
	return comments, next, nil
}
