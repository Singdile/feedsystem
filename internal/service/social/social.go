// Package socail 处理用户关注博主的功能
package social

import (
	"context"
	"feedsystem/internal/model/account"
	apperrors "feedsystem/internal/pkg/errors"
	"log"
	"net/http"
)

// SocialRepo 处理DB
type SocialRepo interface {
	Create(ctx context.Context, fanID, vloggerID uint) error
	Delete(ctx context.Context, fanID, vloggerID uint) error
	ListVloggerIDByFollow(ctx context.Context, fanID uint) ([]account.Profile, error)
	ListFansByVloggerID(ctx context.Context, vloggerID uint) ([]account.Profile, error)
	ListFollowingIDs(ctx context.Context, fanID uint) ([]uint, error)
	IsFollowed(ctx context.Context, fanID, vloggerID uint) (bool, error)
	Counts(ctx context.Context, accountID uint) (uint, uint, error)
}

type UserProvider interface {
	FindByUsername(ctx context.Context, username string) (*account.User, error)
	FindByID(ctx context.Context, id uint) (*account.User, error)
}

type SocialMQ interface {
	PublishFollow(ctx context.Context, followerID, vloggerID uint) error
}

// 关注回填视频到用户的关注流时间线上
type FollowingBackfiller interface {
	BackfillFollowing(ctx context.Context, fanID, vloggerID uint) error
	ClearFollowingTimeline(ctx context.Context, fanID uint) error
}

type SocialService struct {
	repo         SocialRepo
	userProvider UserProvider
	socialMQ     SocialMQ
	backfiller   FollowingBackfiller
}

func NewSocialService(db SocialRepo, userprovider UserProvider, socialMQ SocialMQ, backfiller FollowingBackfiller) *SocialService {
	return &SocialService{
		repo:         db,
		userProvider: userprovider,
		socialMQ:     socialMQ,
		backfiller:   backfiller,
	}
}

// Follow 用户关注博主
func (s *SocialService) Follow(ctx context.Context, fanID, vloggerID uint) error {
	// 参数校验
	if fanID == 0 || vloggerID == 0 {
		return apperrors.NewAppError(http.StatusBadRequest, "参数错误")
	}

	if fanID == vloggerID {
		return apperrors.NewAppError(http.StatusBadRequest, "不能自己关注自己")
	}

	// 检验用户是否存在
	if _, err := s.userProvider.FindByID(ctx, fanID); err != nil {
		return apperrors.NewAppError(http.StatusBadRequest, "用户不存在")
	}

	if _, err := s.userProvider.FindByID(ctx, vloggerID); err != nil {
		return apperrors.NewAppError(http.StatusBadRequest, "用户不存在")
	}

	if err := s.repo.Create(ctx, fanID, vloggerID); err != nil {
		return apperrors.FromError(err)
	}

	// follow 落库成功之后，发布通知事件
	if s.socialMQ != nil {
		if err := s.socialMQ.PublishFollow(ctx, fanID, vloggerID); err != nil {
			log.Printf("socail follow 事件通知发布失败: %v", err)
		}
	}

	// 回填关注博主的视频到用户的关注时间线
	if s.backfiller != nil {
		if err := s.backfiller.BackfillFollowing(ctx, fanID, vloggerID); err != nil {
			log.Printf("关注流回填失败 fan=%d vlogger=%d: %v", fanID, vloggerID, err) // 只 log，不影响关注
		}
	}
	return nil
}

// UnFollow 用户取消关注博主
func (s *SocialService) UnFollow(ctx context.Context, fanID, vloggerID uint) error {
	// 参数校验
	if fanID == 0 || vloggerID == 0 {
		return apperrors.NewAppError(http.StatusBadRequest, "参数错误")
	}

	// 检验用户是否存在
	if _, err := s.userProvider.FindByID(ctx, fanID); err != nil {
		return apperrors.NewAppError(http.StatusBadRequest, "用户不存在")
	}

	if _, err := s.userProvider.FindByID(ctx, vloggerID); err != nil {
		return apperrors.NewAppError(http.StatusBadRequest, "用户不存在")
	}

	if err := s.repo.Delete(ctx, fanID, vloggerID); err != nil {
		return apperrors.FromError(err)
	}

	if s.backfiller != nil {
		if err := s.backfiller.ClearFollowingTimeline(ctx, fanID); err != nil {
			log.Printf("关注流清楚失败，fan = %d: %v", fanID, err)
		}
	}

	return nil
}

// GetAllFollowers 获取关注博主的所有用户id
func (s *SocialService) GetAllFollowers(ctx context.Context, accountID uint) ([]account.Profile, error) {
	if accountID == 0 {
		return nil, apperrors.NewAppError(http.StatusBadRequest, "参数错误")
	}

	return s.repo.ListFansByVloggerID(ctx, accountID)
}

// GetAllVloggers 获取用户关注的所有的博主
func (s *SocialService) GetAllVloggers(ctx context.Context, accountID uint) ([]account.Profile, error) {
	if accountID == 0 {
		return nil, apperrors.NewAppError(http.StatusBadRequest, "参数错误")
	}

	return s.repo.ListVloggerIDByFollow(ctx, accountID)
}

// ListFollowingIDs 获取用户关注的所有博主的id列表
func (s *SocialService) ListFollowingIDs(ctx context.Context, fanID uint) ([]uint, error) {
	if fanID == 0 {
		return []uint{}, apperrors.NewAppError(http.StatusInternalServerError, "未登录")
	}

	return s.repo.ListFollowingIDs(ctx, fanID)
}

// IsFollow 判断用户是否关注某博主
func (s *SocialService) IsFollow(ctx context.Context, accountID, vloggerID uint) (bool, error) {
	// 参数校验
	if accountID == 0 || vloggerID == 0 {
		return false, apperrors.NewAppError(http.StatusBadRequest, "参数错误")
	}

	return s.repo.IsFollowed(ctx, accountID, vloggerID)
}

// Counts 统计用户的粉丝数量以及关注数量
func (s *SocialService) Counts(ctx context.Context, accountID uint) (uint, uint, error) {
	// 参数校验
	if accountID == 0 {
		return 0, 0, apperrors.NewAppError(http.StatusBadRequest, "参数错误")
	}

	return s.repo.Counts(ctx, accountID)
}
