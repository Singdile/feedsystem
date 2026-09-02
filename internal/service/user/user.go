// Package user 账号业务逻辑
package user

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"feedsystem/internal/model/account"
	apperrors "feedsystem/internal/pkg/errors"
	"feedsystem/internal/pkg/jwt"
	"feedsystem/internal/pkg/password"
)

const (
	cacheKeyAccess  = "account:%d"
	cacheKeyRefresh = "account:%d:refresh"
)

// UserRepo user操作接口定义在service层，调用者定义契约
type UserRepo interface {
	Create(ctx context.Context, u account.User) error
	FindByUsername(ctx context.Context, username string) (*account.User, error)
	FindByID(ctx context.Context, id uint) (*account.User, error)
	UpdatePassword(ctx context.Context, id uint, password string) error
	UpdateRefreshToken(ctx context.Context, id uint, refreshtoken string) error
}

// CacheRepo the interface of cache used by service
type CacheRepo interface {
	Key(format string, a ...any) string
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key string, val any, ttl time.Duration) (string, error)
	Del(ctx context.Context, key string) error
	GetAccessByID(ctx context.Context, id uint) (string, error)
	GetIDByRefresh(ctx context.Context, token string) (string, error)
	GetRefreshByID(ctx context.Context, id uint) (string, error)
}

// UserService 账号业务服务
type UserService struct {
	repo  UserRepo
	cache CacheRepo
}

// NewUserService 构造账号服务
func NewUserService(repo UserRepo, cache CacheRepo) *UserService {
	return &UserService{repo: repo, cache: cache}
}

// Register 注册：校验用户名、哈希密码后写入
func (s *UserService) Register(ctx context.Context, username, rawPassword string) error {
	if strings.TrimSpace(username) == "" {
		return apperrors.NewAppError(http.StatusBadRequest, "用户名不能全为空格")
	}

	hash, err := password.Hash(rawPassword)
	if err != nil {
		return apperrors.NewAppError(http.StatusInternalServerError, "hash计算错误")
	}
	if err := s.repo.Create(ctx, account.User{
		Username: username,
		Password: hash,
	}); err != nil {
		if errors.Is(err, apperrors.ErrUsernameExists) {
			return apperrors.NewAppError(http.StatusConflict, "用户名已存在")
		}
		return err
	}
	return nil
}

// ChangePassword 修改用户密码
func (s *UserService) ChangePassword(ctx context.Context, id uint, newpassword string) error {
	// 查看用户是否存在
	_, err := s.repo.FindByID(ctx, id)
	if err != nil {
		return err
	}
	// 修改密码
	newpwd, _ := password.Hash(newpassword)
	err = s.repo.UpdatePassword(ctx, id, newpwd)
	if err != nil {
		return err
	}
	return nil
}

// CheckLogin 检查用户是否存在
func (s *UserService) checkuser(ctx context.Context, username, rawPassword string) (*account.User, error) {
	user, err := s.repo.FindByUsername(ctx, username)
	if err != nil {
		return nil, apperrors.NewAppError(http.StatusUnauthorized, "账号或密码错")
	}

	if !password.Verify(user.Password, rawPassword) {
		return nil, apperrors.NewAppError(http.StatusUnauthorized, "账号或密码错")
	}

	return user, nil
}

// Login 用户登录并返回token
func (s *UserService) Login(ctx context.Context, username, rawPassword string) (accesstoken, refreshtoken string, err error) {
	user, err := s.checkuser(ctx, username, rawPassword)
	if err != nil { //账号、密码错误或者用户不存在
		return "", "", err
	}

	// 用户存在，更新access-token,refresh-token
	accessToken, err := jwt.GenerateToken(jwt.JwtSecrete(), user.ID, user.Username)
	if err != nil { //内部出现错误，无法加密
		return "", "", err
	}
	refreshToken := jwt.GenerateRefreshToken()

	// 失效之前的token
	if old, err := s.cache.Get(ctx, s.cache.Key("account:%d:refresh", user.ID)); err == nil && old != "" {
		s.cache.Del(ctx, s.cache.Key("refresh:%s", old))
	}

	// 记录在redisn
	accessCacheKey := fmt.Sprintf("account:%d", user.ID)
	refreshCacheKey := fmt.Sprintf("account:%d:refresh", user.ID)

	s.cache.Set(ctx, accessCacheKey, accessToken, jwt.AccessTokenTTL)
	s.cache.Set(ctx, refreshCacheKey, refreshToken, jwt.RefreshTokenTTL)
	s.cache.Set(ctx, s.cache.Key("refresh:%s", refreshToken), fmt.Sprintf("%d", user.ID), jwt.RefreshTokenTTL)

	// 异步写回db
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := s.repo.UpdateRefreshToken(ctx, user.ID, refreshToken); err != nil {
			log.Printf("refresh token落库失败，userid:%v,err:%v", user.ID, err)
		}
	}()
	// 返回token
	return accessToken, refreshToken, nil
}

// Refresh 刷新access token 和 refresh token
func (s *UserService) Refresh(ctx context.Context, refreshToken string) (accessToken, newRefresh string, err error) {
	// 判断 refreshToken 是否有效,并反查用户
	idstr, err := s.cache.GetIDByRefresh(ctx, refreshToken)
	if err != nil { //查找不到说明，refreshtoken 不存在/已过期/已轮换
		return "", "", apperrors.NewAppError(http.StatusUnauthorized, "refresh token 无效或已过期")
	}

	id, _ := strconv.ParseUint(idstr, 10, 64)
	user, err := s.repo.FindByID(ctx, uint(id))
	if err != nil {
		return "", "", err
	}

	// 有效，先删除原本的refresh
	_ = s.cache.Del(ctx, s.cache.Key("refresh:%s", refreshToken))
	_ = s.cache.Del(ctx, s.cache.Key("account:%d:refresh", user.ID))

	// 有效，签发新的 access token 和 refresh token
	accessToken, err = jwt.GenerateToken(jwt.JwtSecrete(), user.ID, user.Username)
	if err != nil { //内部出现错误，无法加密
		return "", "", err
	}
	newRefresh = jwt.GenerateRefreshToken()

	// 记录redis
	_, _ = s.cache.Set(ctx, s.cache.Key("account:%d", user.ID), accessToken, jwt.AccessTokenTTL)
	_, _ = s.cache.Set(ctx, s.cache.Key("account:%d:refresh", user.ID), newRefresh, jwt.RefreshTokenTTL)
	_, _ = s.cache.Set(ctx, s.cache.Key("refresh:%s", newRefresh), idstr, jwt.RefreshTokenTTL)

	// refreshtoken异步写入数据库
	// 异步写回db
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := s.repo.UpdateRefreshToken(ctx, user.ID, newRefresh); err != nil {
			log.Printf("refresh token落库失败，userid:%v,err:%v", user.ID, err)
		}
	}()
	// 返回token
	return accessToken, newRefresh, nil

}

func (s *UserService) Logout(ctx context.Context, refreshToken string) error {
	// 由refreshtoken反查user
	idstr, err := s.cache.GetIDByRefresh(ctx, refreshToken)
	if err != nil { //查找不到说明，refreshtoken 不存在/已过期/已轮换
		return apperrors.NewAppError(http.StatusUnauthorized, "refresh token 无效或已过期")
	}

	id, _ := strconv.ParseUint(idstr, 10, 64)

	// 删除user相关的token
	_ = s.cache.Del(ctx, s.cache.Key("refresh:%s", refreshToken))
	_ = s.cache.Del(ctx, s.cache.Key("account:%d:refresh", id))
	_ = s.cache.Del(ctx, s.cache.Key("account:%d", id))

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := s.repo.UpdateRefreshToken(ctx, uint(id), ""); err != nil {
			log.Printf("logout 清库失败，userid:%v,err:%v", id, err)
		}
	}()
	return nil
}
