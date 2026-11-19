package outbox

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"feedsystem/internal/config"
	"feedsystem/internal/data"
	outboxmodel "feedsystem/internal/model/outbox"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func newOutboxTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	base := config.DBConfig{
		User:     "root",
		Password: "123456",
		Host:     "localhost",
		Port:     3306,
		Dbname:   "mysql",
	}

	admin, err := data.NewDB(base)
	if err != nil {
		t.Skipf("mysql 不可达，跳过集成测试: %v", err)
	}

	name := fmt.Sprintf("feedtest_outbox_%d", time.Now().UnixNano())
	base.Dbname = name
	require.NoError(t, admin.Exec("CREATE DATABASE "+name).Error)
	data.CloseDB(admin)

	db, err := data.NewDB(base)
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&outboxmodel.Msg{}))

	t.Cleanup(func() {
		data.CloseDB(db)
		if a, e := data.NewDB(base); e == nil {
			a.Exec("DROP DATABASE " + name)
			data.CloseDB(a)
		}
	})

	return db
}

func seedPending(t *testing.T, db *gorm.DB, n int) []uint {
	t.Helper()

	ids := make([]uint, 0, n)
	for i := 0; i < n; i++ {
		m := outboxmodel.Msg{
			VideoID:   uint(1000 + i),
			AuthorID:  1,
			EventType: outboxmodel.EventTypePublish,
			Status:    outboxmodel.StatusPending,
		}
		require.NoError(t, db.Create(&m).Error)
		ids = append(ids, m.ID)
	}
	return ids
}

func getMsg(t *testing.T, db *gorm.DB, id uint) outboxmodel.Msg {
	t.Helper()

	var m outboxmodel.Msg
	require.NoError(t, db.First(&m, id).Error)
	return m
}

// 两个 worker 并发认领，不能拿到同一条
func TestClaimConcurrentDisjoint(t *testing.T) {
	db := newOutboxTestDB(t)
	store := NewOutboxRepo(db)
	seedPending(t, db, 2)

	var wg sync.WaitGroup
	results := make([][]outboxmodel.Msg, 2)

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			msgs, err := store.Claim(context.Background(), 1, 10)
			require.NoError(t, err)
			results[i] = msgs
		}(i)
	}
	wg.Wait()

	require.Equal(t, 2, len(results[0])+len(results[1]), "两条记录应各被认领一次")
	if len(results[0]) == 1 && len(results[1]) == 1 {
		require.NotEqual(t, results[0][0].ID, results[1][0].ID, "不能重复认领同一行")
	}
}

// 退避未到期不会被再次认领
func TestClaimRespectsNextRetry(t *testing.T) {
	db := newOutboxTestDB(t)
	store := NewOutboxRepo(db)
	seedPending(t, db, 1)

	msgs, err := store.Claim(context.Background(), 1, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, 1, msgs[0].Attempts)

	require.NoError(t, store.MarkFailed(context.Background(), msgs[0].ID, time.Minute, errors.New("boom")))

	msgs, err = store.Claim(context.Background(), 1, 10)
	require.NoError(t, err)
	require.Empty(t, msgs, "退避未到期不应被认领")
}

// 达到最大尝试次数后不再被认领
func TestClaimSkipsExhausted(t *testing.T) {
	db := newOutboxTestDB(t)
	store := NewOutboxRepo(db)

	m := outboxmodel.Msg{
		VideoID:   1,
		AuthorID:  1,
		EventType: outboxmodel.EventTypePublish,
		Status:    outboxmodel.StatusPending,
		Attempts:  3,
	}
	require.NoError(t, db.Create(&m).Error)

	msgs, err := store.Claim(context.Background(), 10, 3)
	require.NoError(t, err)
	require.Empty(t, msgs, "attempts >= maxAttempts 不应被认领")
}

// MarkDone 后状态为 done
func TestMarkDone(t *testing.T) {
	db := newOutboxTestDB(t)
	store := NewOutboxRepo(db)
	seedPending(t, db, 1)

	msgs, err := store.Claim(context.Background(), 1, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	require.NoError(t, store.MarkDone(context.Background(), msgs[0].ID))

	got := getMsg(t, db, msgs[0].ID)
	require.Equal(t, outboxmodel.StatusDone, got.Status)
	require.Nil(t, got.ClaimedAt)
	require.Nil(t, got.NextRetryAt)
}

// 失败但未耗尽：回到 pending，并设置退避与错误信息
func TestMarkFailedRetry(t *testing.T) {
	db := newOutboxTestDB(t)
	store := NewOutboxRepo(db)
	seedPending(t, db, 1)

	msgs, err := store.Claim(context.Background(), 1, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	before := time.Now()
	require.NoError(t, store.MarkFailed(context.Background(), msgs[0].ID, 30*time.Second, errors.New("mq down")))

	got := getMsg(t, db, msgs[0].ID)
	require.Equal(t, outboxmodel.StatusPending, got.Status)
	require.Nil(t, got.ClaimedAt)
	require.NotNil(t, got.NextRetryAt)
	require.True(t, got.NextRetryAt.After(before), "next_retry_at 应在未来")
	require.Contains(t, got.LastError, "mq down")
}

// 耗尽：标记为 failed
func TestMarkFailedExhausted(t *testing.T) {
	db := newOutboxTestDB(t)
	store := NewOutboxRepo(db)
	seedPending(t, db, 1)

	msgs, err := store.Claim(context.Background(), 1, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	require.NoError(t, store.MarkFailed(context.Background(), msgs[0].ID, 0, errors.New("dead")))

	got := getMsg(t, db, msgs[0].ID)
	require.Equal(t, outboxmodel.StatusFailed, got.Status)
	require.Nil(t, got.NextRetryAt)
	require.Contains(t, got.LastError, "dead")
}

// 回收超时的 publishing：还有次数 → pending
func TestReclaimToPending(t *testing.T) {
	db := newOutboxTestDB(t)
	store := NewOutboxRepo(db)

	stale := time.Now().Add(-10 * time.Minute)
	m := outboxmodel.Msg{
		VideoID:   1,
		AuthorID:  1,
		EventType: outboxmodel.EventTypePublish,
		Status:    outboxmodel.StatusPublishing,
		Attempts:  1,
		ClaimedAt: &stale,
	}
	require.NoError(t, db.Create(&m).Error)

	n, err := store.Reclaim(context.Background(), 5*time.Minute, 3)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	got := getMsg(t, db, m.ID)
	require.Equal(t, outboxmodel.StatusPending, got.Status)
	require.Nil(t, got.ClaimedAt)
	require.Equal(t, 1, got.Attempts, "回收不应重置 attempts")
}

// 回收超时的 publishing：次数耗尽 → failed
func TestReclaimToFailed(t *testing.T) {
	db := newOutboxTestDB(t)
	store := NewOutboxRepo(db)

	stale := time.Now().Add(-10 * time.Minute)
	m := outboxmodel.Msg{
		VideoID:   1,
		AuthorID:  1,
		EventType: outboxmodel.EventTypePublish,
		Status:    outboxmodel.StatusPublishing,
		Attempts:  3,
		ClaimedAt: &stale,
	}
	require.NoError(t, db.Create(&m).Error)

	n, err := store.Reclaim(context.Background(), 5*time.Minute, 3)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	got := getMsg(t, db, m.ID)
	require.Equal(t, outboxmodel.StatusFailed, got.Status)
	require.Nil(t, got.ClaimedAt)
}
