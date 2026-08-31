// Package user 账号仓储实现
package user

import (
	"context"

	"feedsystem/internal/model/account"

	"gorm.io/gorm"
)

// Repo 实现 service.UserRepo 契约
type Repo struct {
	db *gorm.DB
}

// NewRepo 构造账号仓储
func NewRepo(db *gorm.DB) *Repo {
	return &Repo{db: db}
}

func (r *Repo) Create(ctx context.Context, u account.User) error {
	return r.db.WithContext(ctx).Create(&u).Error
}

func (r *Repo) FindByUsername(ctx context.Context, username string) (*account.User, error) {
	var u account.User
	if err := r.db.WithContext(ctx).Where("username = ?", username).First(&u).Error; err != nil {
		return nil, err
	}
	return &u, nil
}

func (r *Repo) FindByID(ctx context.Context, id uint) (*account.User, error) {
	var u account.User
	if err := r.db.WithContext(ctx).First(&u, id).Error; err != nil {
		return nil, err
	}
	return &u, nil
}

func (r *Repo) UpdatePassword(ctx context.Context, id uint, password string) error {
	return r.db.WithContext(ctx).Model(&account.User{}).
		Where("id = ?", id).
		Update("password", password).Error
}
