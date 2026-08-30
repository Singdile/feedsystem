// Package service 提供业务接口定义
package service

import (
	"context"
	"feedsystem/internal/model"
	"feedsystem/internal/pkg/password"
)

// UserRepo user操作接口定义在service层，调用者定义契约
type UserRepo interface {
    Create(ctx context.Context,u model.User) error
    FindByUsername(ctx context.Context,username string) (*model.User,error)
    FindByID(ctx context.Context,id uint) (*model.User,error)
    UpdatePassword(ctx context.Context,id uint,password string) error
}


type UserService struct {
    repo UserRepo
}
