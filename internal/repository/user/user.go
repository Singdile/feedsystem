// Package user 账号仓储实现
package user

import (
	"context"
	"errors"
	"feedsystem/internal/model/account"
	apperrors "feedsystem/internal/pkg/errors"

	"github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
)

// Repo 实现 service.UserRepo 契约
type userRepo struct {
	db *gorm.DB
}

// NewuserRepo 构造账号仓储
func NewuserRepo(db *gorm.DB) *userRepo {
	return &userRepo{db: db}
}

func (r *userRepo) Create(ctx context.Context, u account.User) error {
	err := r.db.WithContext(ctx).Create(&u).Error
	if mysqlErr, ok := errors.AsType[*mysql.MySQLError](err); ok && mysqlErr.Number == 1062 {
		return apperrors.ErrUsernameExists
	}
	return err
}

func (r *userRepo) FindByUsername(ctx context.Context, username string) (*account.User, error) {
	var u account.User
	if err := r.db.WithContext(ctx).Where("username = ?", username).First(&u).Error; err != nil {
		return nil, err
	}
	return &u, nil
}

func (r *userRepo) FindByID(ctx context.Context, id uint) (*account.User, error) {
	var u account.User
	if err := r.db.WithContext(ctx).First(&u, id).Error; err != nil {
		return nil, err
	}
	return &u, nil
}

func (r *userRepo) UpdatePassword(ctx context.Context, id uint, password string) error {
	return r.db.WithContext(ctx).Model(&account.User{}).
		Where("id = ?", id).
		Update("password", password).Error
}

func (r *userRepo) UpdateRefreshToken(ctx context.Context, id uint, refreshtoken string) error {
	return r.db.WithContext(ctx).Model(&account.User{}).Where("id=?", id).Update("refresh_token", refreshtoken).Error
}

func (r *userRepo) ListByUserName(ctx context.Context, username string) ([]account.User, error) {
	var users = []account.User{}
	err := r.db.WithContext(ctx).Model(&account.User{}).Where("username LIKE ?", "%"+username+"%").Find(&users).Error
	if err != nil {
		return nil, err
	}
	return users, nil
}
