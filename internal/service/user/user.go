// Package user 账号业务逻辑
package user

import (
	"context"

	"feedsystem/internal/model/account"
	"feedsystem/internal/pkg/password"
)

// UserRepo user操作接口定义在service层，调用者定义契约
type UserRepo interface {
	Create(ctx context.Context, u account.User) error
	FindByUsername(ctx context.Context, username string) (*account.User, error)
	FindByID(ctx context.Context, id uint) (*account.User, error)
	UpdatePassword(ctx context.Context, id uint, password string) error
}

// UserService 账号业务服务
type UserService struct {
	repo UserRepo
}

// NewUserService 构造账号服务
func NewUserService(repo UserRepo) *UserService {
	return &UserService{repo: repo}
}

// Register 注册：校验用户名、哈希密码后写入
func (s *UserService) Register(ctx context.Context, username, rawPassword string) error {
	hash, err := password.Hash(rawPassword)
	if err != nil {
		return err
	}
	return s.repo.Create(ctx, account.User{
		Username: username,
		Password: hash,
	})
}
