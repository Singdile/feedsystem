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

type SocialService struct {
	repo         SocialRepo
	userProvider UserProvider
	socialMQ     SocialMQ
}

func NewSocialService(db SocialRepo, userprovider UserProvider, socialMQ SocialMQ) *SocialService {
	return &SocialService{
		repo:         db,
		userProvider: userprovider,
		socialMQ:     socialMQ,
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
