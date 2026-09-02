package user

import (
	"context"
	"errors"
	"feedsystem/internal/config"
	"feedsystem/internal/data"
	"feedsystem/internal/model/account"
	apperrors "feedsystem/internal/pkg/errors"
	pwd "feedsystem/internal/pkg/password"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeRepo struct {
	mu         sync.Mutex
	users      map[uint]*account.User
	byName     map[string]uint
	refreshLog map[uint][]string
}

// seedUser 往 fakeRepo 造一个用户，密码存 bcrypt hash
func seedUser(t *testing.T, repo *fakeRepo, id uint, username, rawPassword string) {
	t.Helper()
	hash, err := pwd.Hash(rawPassword)
	require.NoError(t, err)

	repo.mu.Lock()
	defer repo.mu.Unlock()
	repo.users[id] = &account.User{ID: id, Username: username, Password: hash}
	repo.byName[username] = id
}

func (f *fakeRepo) Create(ctx context.Context, u account.User) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.users[u.ID] = &u
	f.byName[u.Username] = u.ID
	return nil
}

func (f *fakeRepo) FindByUsername(ctx context.Context, username string) (*account.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	userID := f.byName[username]
	user, ok := f.users[userID]
	if !ok {
		return nil, errors.New("user not found")
	} else {
		return user, nil
	}
}

func (f *fakeRepo) FindByID(ctx context.Context, id uint) (*account.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	user, ok := f.users[id]
	if !ok {
		return nil, errors.New("user not found")
	}
	return user, nil
}
func (f *fakeRepo) UpdatePassword(ctx context.Context, id uint, password string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	user, ok := f.users[id]
	if !ok {
		return errors.New("user not found")
	}
	user.Password, _ = pwd.Hash(password)
	return nil
}
func (f *fakeRepo) UpdateRefreshToken(ctx context.Context, id uint, refreshtoken string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.users[id].RefreshToken = refreshtoken
	f.refreshLog[id] = append(f.refreshLog[id], refreshtoken)
	return nil
}

func newFakeRepo() *fakeRepo {
	users := make(map[uint]*account.User)
	password, _ := pwd.Hash("password1")
	users[1] = &account.User{
		ID:       1,
		Username: "user1",
		Password: password,
	}

	byName := make(map[string]uint)
	byName["user1"] = 1
	return &fakeRepo{users: users, byName: byName, refreshLog: make(map[uint][]string)}
}

func newTestCache(t *testing.T) *data.RedisClient {
	mr := miniredis.RunT(t)
	host, portStr, _ := net.SplitHostPort(mr.Addr())
	port, _ := strconv.Atoi(portStr)
	rdb, _ := data.NewRedis(config.RedisConfig{
		Host: host,
		Port: port,
	})
	return rdb
}

func newTestService(t *testing.T) (*UserService, *fakeRepo, *data.RedisClient) {
	repo := newFakeRepo()
	rdb := newTestCache(t)
	svc := NewUserService(repo, rdb)
	return svc, repo, rdb
}

func requireStatus(t *testing.T, err error, wantcode int) {
	t.Helper() // 报错时定位到调用它的用例行
	require.Error(t, err)
	assert.Equal(t, wantcode, apperrors.FromError(err).Status)
}

// TestLoginSuccess test accesstoken and refresh token are returned
// And db stored the refresh token
func TestLoginSuccess(t *testing.T) {
	svc, repo, _ := newTestService(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	accessToken, refreshToken, err := svc.Login(ctx, "user1", "password1")
	require.NoError(t, err)
	assert.NotEmpty(t, accessToken)
	assert.NotEmpty(t, refreshToken)

	// test token in the cache
	gotAccess, err := svc.cache.GetAccessByID(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, accessToken, gotAccess)

	gotRefresh, err := svc.cache.GetRefreshByID(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, refreshToken, gotRefresh)

	gotID, err := svc.cache.GetIDByRefresh(ctx, refreshToken)
	require.NoError(t, err)
	assert.Equal(t, "1", gotID)

	// test refresh token in the db
	require.Eventually(t, func() bool {
		repo.mu.Lock()
		defer repo.mu.Unlock()
		return len(repo.refreshLog[1]) > 0 && repo.refreshLog[1][0] == refreshToken
	}, time.Second, 10*time.Millisecond)
}

func TestLoginWrongPassword(t *testing.T) {
	svc, _, _ := newTestService(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, _, err := svc.Login(ctx, "user1", "password1xxx")
	requireStatus(t, err, 401)

	_, _, err = svc.Login(ctx, "xxx", "password1")
	requireStatus(t, err, 401)
}

func TestLoginKickOldDevice(t *testing.T) {
	svc, _, _ := newTestService(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// first login got accessToken,refreshtoken
	_, oldrefreshToken, _ := svc.Login(ctx, "user1", "password1")

	// now
	_, nowrefreshToken, _ := svc.Login(ctx, "user1", "password1")

	// assert
	redisRefreshToken, err := svc.cache.GetRefreshByID(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, nowrefreshToken, redisRefreshToken)
	assert.NotEqual(t, oldrefreshToken, redisRefreshToken)

	_, err = svc.cache.GetIDByRefresh(ctx, oldrefreshToken)
	require.Error(t, err)
	_, _, err = svc.Refresh(ctx, oldrefreshToken)
	require.Error(t, err)
}

func TestRefreshRotate(t *testing.T) {
	// 登录
	svc, repo, _ := newTestService(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, rt, err := svc.Login(ctx, "user1", "password1")
	require.NoError(t, err)

	newAccess, newRefresh, err := svc.Refresh(ctx, rt)
	require.NoError(t, err)
	assert.NotEmpty(t, newAccess)
	assert.NotEmpty(t, newRefresh)
	assert.NotEqual(t, rt, newRefresh) // change or not

	// check old refresh token still keeps alive or not
	_, err = svc.cache.GetIDByRefresh(ctx, rt)
	require.Error(t, err)

	// 新 token 已就位
	got, _ := svc.cache.GetAccessByID(ctx, 1)
	assert.Equal(t, newAccess, got)
	gotR, _ := svc.cache.GetRefreshByID(ctx, 1)
	assert.Equal(t, newRefresh, gotR)
	id, _ := svc.cache.GetIDByRefresh(ctx, newRefresh)
	assert.Equal(t, "1", id)

	// 异步落库：取 refreshLog[1] 的【最后一个】元素（Login 也写过一条）
	require.Eventually(t, func() bool {
		repo.mu.Lock()
		defer repo.mu.Unlock()
		log := repo.refreshLog[1]
		return len(log) > 0 && log[len(log)-1] == newRefresh
	}, time.Second, 10*time.Millisecond)
}

func TestLogout(t *testing.T) {
	// login
	svc, _, _ := newTestService(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, rt, err := svc.Login(ctx, "user1", "password1")
	require.NoError(t, err)

	// logout
	err = svc.Logout(ctx, rt)
	require.NoError(t, err)

	// assert
	_, err = svc.cache.GetIDByRefresh(ctx, rt)
	require.Error(t, err)
	_, err = svc.cache.GetAccessByID(ctx, 1)
	require.Error(t, err)
}

func TestRefreshReplay(t *testing.T) {
	svc, _, _ := newTestService(t)
	ctx := context.Background()
	_, rt, err := svc.Login(ctx, "user1", "password1")
	require.NoError(t, err)

	_, _, err = svc.Refresh(ctx, rt)
	require.NoError(t, err) // 第一次轮换成功

	_, _, err = svc.Refresh(ctx, rt) // 同一 rt 复用
	requireStatus(t, err, 401)       // 防重放核心，锁死是 401 而非 500
}
