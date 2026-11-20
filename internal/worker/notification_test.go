package worker

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"feedsystem/internal/config"
	"feedsystem/internal/data"
	"feedsystem/internal/messaging"
	"feedsystem/internal/model/account"
	"feedsystem/internal/model/video"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// ---------- 测试基建 ----------

func newNotificationTestDB(t *testing.T) *gorm.DB {
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

	name := fmt.Sprintf("feedtest_notif_%d", time.Now().UnixNano())
	base.Dbname = name
	require.NoError(t, admin.Exec("CREATE DATABASE "+name).Error)
	data.CloseDB(admin)

	db, err := data.NewDB(base)
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&account.Notification{}, &account.User{}))

	t.Cleanup(func() {
		data.CloseDB(db)
		if a, e := data.NewDB(base); e == nil {
			a.Exec("DROP DATABASE " + name)
			data.CloseDB(a)
		}
	})

	return db
}

// recordingHub 记录推送，供断言使用。
type recordingHub struct {
	mu     sync.Mutex
	pushed []uint
}

func (h *recordingHub) Push(userID uint, _ *account.Notification) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pushed = append(h.pushed, userID)
}

func (h *recordingHub) count(userID uint) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, id := range h.pushed {
		if id == userID {
			n++
		}
	}
	return n
}

func (h *recordingHub) total() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.pushed)
}

const (
	notifTestAuthorID    = uint(301)
	notifTestCommenterID = uint(201)
	notifTestAliceID     = uint(101)
	notifTestBobID       = uint(102)
)

func seedNotificationUsers(t *testing.T, db *gorm.DB) {
	t.Helper()

	require.NoError(t, db.Create(&[]account.User{
		{ID: notifTestAliceID, Username: "alice"},
		{ID: notifTestBobID, Username: "bob"},
		{ID: notifTestCommenterID, Username: "commenter"},
		{ID: notifTestAuthorID, Username: "author"},
	}).Error)
}

func notificationPublishEvent() video.CommentEvent {
	return video.CommentEvent{
		EventID:   "evt-notif-publish-1",
		Action:    "publish",
		VideoID:   900001,
		AuthorID:  notifTestAuthorID,
		AccountID: notifTestCommenterID,
		UserName:  "commenter",
		Content:   "hello @alice @bob",
	}
}

func runNotificationHandler(t *testing.T, db *gorm.DB, hub NotificationHub, ev video.CommentEvent) *fakeMessage {
	t.Helper()

	env, err := messaging.NewEnvelope(
		"env-"+ev.EventID,
		messaging.EventTypeComment,
		messaging.CurrentEventVersion,
		ev,
	)
	require.NoError(t, err)

	body, err := env.Marshal()
	require.NoError(t, err)

	msg := &fakeMessage{body: body}
	require.NoError(t, NotificationHandler(db, hub, nil)(context.Background(), msg))
	require.True(t, msg.accepted)
	return msg
}

func notificationsForEvent(t *testing.T, db *gorm.DB, eventID string) []account.Notification {
	t.Helper()

	var got []account.Notification
	require.NoError(t, db.Where("source_event_id = ?", eventID).Find(&got).Error)
	return got
}

func recipientsOf(notifs []account.Notification) []uint {
	out := make([]uint, 0, len(notifs))
	for _, n := range notifs {
		out = append(out, n.RecipientID)
	}
	return out
}

// ---------- 用例 ----------

// TestNotificationPublishCreatesAuthorAndMentions 验证一次完整处理会创建作者通知和全部提及。
func TestNotificationPublishCreatesAuthorAndMentions(t *testing.T) {
	db := newNotificationTestDB(t)
	seedNotificationUsers(t, db)

	hub := &recordingHub{}
	ev := notificationPublishEvent()

	runNotificationHandler(t, db, hub, ev)

	got := notificationsForEvent(t, db, ev.EventID)
	require.Len(t, got, 3)
	require.ElementsMatch(t,
		[]uint{notifTestAuthorID, notifTestAliceID, notifTestBobID},
		recipientsOf(got),
	)

	require.Equal(t, 3, hub.total())
	require.Equal(t, 1, hub.count(notifTestAuthorID))
	require.Equal(t, 1, hub.count(notifTestAliceID))
	require.Equal(t, 1, hub.count(notifTestBobID))
}

// TestNotificationPublishResumesMentionsWhenAuthorExists 覆盖"作者通知已存在、提及缺失"的重投场景。
func TestNotificationPublishResumesMentionsWhenAuthorExists(t *testing.T) {
	db := newNotificationTestDB(t)
	seedNotificationUsers(t, db)

	ev := notificationPublishEvent()

	// 模拟上一次只完成了作者通知
	require.NoError(t, db.Create(&account.Notification{
		RecipientID:   ev.AuthorID,
		SenderID:      ev.AccountID,
		Type:          "comment",
		TargetID:      ev.VideoID,
		IsRead:        false,
		Content:       "评论了你的视频",
		SourceEventID: ev.EventID,
	}).Error)

	hub := &recordingHub{}
	runNotificationHandler(t, db, hub, ev)

	got := notificationsForEvent(t, db, ev.EventID)
	require.Len(t, got, 3)
	require.ElementsMatch(t,
		[]uint{notifTestAuthorID, notifTestAliceID, notifTestBobID},
		recipientsOf(got),
	)

	require.Equal(t, 0, hub.count(notifTestAuthorID), "作者通知已存在，不应重复推送")
	require.Equal(t, 1, hub.count(notifTestAliceID))
	require.Equal(t, 1, hub.count(notifTestBobID))
}

// TestNotificationPublishSkipsExistingMention 验证已存在的提及只跳过自身，不影响后续提及。
func TestNotificationPublishSkipsExistingMention(t *testing.T) {
	db := newNotificationTestDB(t)
	seedNotificationUsers(t, db)

	ev := notificationPublishEvent()

	// 作者通知与 alice 的提及都已经存在，只有 bob 缺失
	require.NoError(t, db.Create(&[]account.Notification{
		{
			RecipientID:   ev.AuthorID,
			SenderID:      ev.AccountID,
			Type:          "comment",
			TargetID:      ev.VideoID,
			IsRead:        false,
			Content:       "评论了你的视频",
			SourceEventID: ev.EventID,
		},
		{
			RecipientID:   notifTestAliceID,
			SenderID:      ev.AccountID,
			Type:          "mention",
			TargetID:      ev.VideoID,
			IsRead:        false,
			Content:       "提及了你",
			SourceEventID: ev.EventID,
		},
	}).Error)

	hub := &recordingHub{}
	runNotificationHandler(t, db, hub, ev)

	got := notificationsForEvent(t, db, ev.EventID)
	require.Len(t, got, 3)
	require.ElementsMatch(t,
		[]uint{notifTestAuthorID, notifTestAliceID, notifTestBobID},
		recipientsOf(got),
	)

	require.Equal(t, 1, hub.total(), "只有缺失的 bob 需要推送")
	require.Equal(t, 1, hub.count(notifTestBobID))
	require.Equal(t, 0, hub.count(notifTestAliceID))
	require.Equal(t, 0, hub.count(notifTestAuthorID))
}

// TestNotificationPublishIdempotentOnRedelivery 验证同一事件重复投递不会新增通知或重复推送。
func TestNotificationPublishIdempotentOnRedelivery(t *testing.T) {
	db := newNotificationTestDB(t)
	seedNotificationUsers(t, db)

	hub := &recordingHub{}
	ev := notificationPublishEvent()

	runNotificationHandler(t, db, hub, ev)
	firstPushTotal := hub.total()
	require.Equal(t, 3, firstPushTotal)

	runNotificationHandler(t, db, hub, ev)

	got := notificationsForEvent(t, db, ev.EventID)
	require.Len(t, got, 3)
	require.Equal(t, firstPushTotal, hub.total(), "重复投递不应重复推送")
}
