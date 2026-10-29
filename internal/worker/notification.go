package worker

import (
	"context"
	"encoding/json"
	"errors"
	"feedsystem/internal/data"
	"feedsystem/internal/model/account"
	"feedsystem/internal/model/video"
	"log"
	"regexp"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	"gorm.io/gorm"
)

const notifUnreadTTL = 7 * 24 * time.Hour

// NotificationHub 推送通知给在线用户（SSEHub 后续实现本接口）
type NotificationHub interface {
	Push(userID uint, n *account.Notification)
}

func NotificationHandler(db *gorm.DB, hub NotificationHub, cache *data.RedisClient) MQHandler {
	return func(ctx context.Context, dc rmq.IDeliveryContext) error {
		// 解析事件数据
		var header struct {
			Action string `json:"action"`
		}

		data := dc.Message().GetData()
		if err := json.Unmarshal(data, &header); err != nil {
			_ = dc.Accept(ctx)
			return nil
		}

		// 处理事件
		var processErr error

		process := func(action string, data []byte) error {
			switch action {
			case "like":
				return processLike(ctx, db, cache, hub, data) // todo: 处理like
			case "publish":
				return processPublish(ctx, db, cache, hub, data)
			case "follow":
				return processFollow(ctx, db, cache, hub, data)
			default:
				return nil
			}
		}

		processErr = process(header.Action, data)
		// 重试三次
		for i := 1; processErr != nil && i <= 3; i++ {
			time.Sleep(time.Duration(1<<uint(i-1)) * time.Second)
			processErr = process(header.Action, data)
		}
		if processErr != nil {
			log.Printf("notification 处理失败(重试3次后丢弃): %v", processErr)
		}

		return dc.Accept(ctx)
	}
}

// 通知落库成功后调用(key 不存在的时候懒加载)
func incrUnread(ctx context.Context, db *gorm.DB, cache *data.RedisClient, recipientID uint) {
	if cache == nil {
		return
	}

	key := cache.Key("notification:%d", recipientID)
	exists, err := cache.Exists(ctx, key)
	if err != nil {
		return
	}
	if !exists {
		// 懒加载
		var cnt int64
		db.WithContext(ctx).Model(&account.Notification{}).Where("recipient_id = ? and is_read = ?", recipientID, false).Count(&cnt)
		cache.Set(ctx, key, cnt, notifUnreadTTL)
		return
	}
	cache.Incr(ctx, key)
	cache.Expire(ctx, key, notifUnreadTTL)
}

func processLike(ctx context.Context, db *gorm.DB, cache *data.RedisClient, hub NotificationHub, data []byte) error {
	// 参数解析
	var likeEvent video.RatingEvent
	if err := json.Unmarshal(data, &likeEvent); err != nil { // 解析不了，说明data有问题，直接确认处理即可
		log.Printf("notificaton like error,err: %v", err)
		return nil
	}

	// 查询视频作者id
	var authorID uint
	err := db.WithContext(ctx).Model(&video.Video{}).Select("author_id").Where("id = ?", likeEvent.VideoID).Scan(&authorID).Error
	if err != nil {
		log.Printf("notificaton error,cannot find author_id,error :%v", err)
		return err
	}

	if likeEvent.AccountID == authorID { // 不能自己给自己点赞
		return nil
	}

	// 转换为notification事件
	notifiacitonEvent := account.Notification{
		RecipientID:   authorID,
		SenderID:      likeEvent.AccountID,
		Type:          "like",
		TargetID:      likeEvent.VideoID,
		IsRead:        false,
		Content:       "点赞了你的视频",
		SourceEventID: likeEvent.EventID,
	}

	// 存入DB中的notifications表
	created, err := createNotification(ctx, db, &notifiacitonEvent)
	if err != nil {
		return err
	}

	if !created { //幂等跳过，不用计数/推送
		return nil
	}

	// 推送信息给在线用户
	incrUnread(ctx, db, cache, notifiacitonEvent.RecipientID)
	hub.Push(notifiacitonEvent.RecipientID, &notifiacitonEvent)
	return nil
}

func processPublish(ctx context.Context, db *gorm.DB, cache *data.RedisClient, hub NotificationHub, data []byte) error {
	// 参数解析
	var commentEvent video.CommentEvent
	if err := json.Unmarshal(data, &commentEvent); err != nil { // 解析不了，说明data有问题，直接确认处理即可
		log.Printf("notificaton comment error,err: %v", err)
		return nil
	}

	if commentEvent.AccountID == commentEvent.AuthorID {
		return nil
	}

	// 转换为notification事件
	notifiacitonEvent := account.Notification{
		RecipientID:   commentEvent.AuthorID,
		SenderID:      commentEvent.AccountID,
		Type:          "comment",
		TargetID:      commentEvent.VideoID,
		IsRead:        false,
		Content:       "评论了你的视频",
		SourceEventID: commentEvent.EventID,
	}

	// 存入DB中的notifications表
	created, err := createNotification(ctx, db, &notifiacitonEvent)
	if err != nil {
		return err
	}

	if !created { //幂等跳过，不用计数/推送
		return nil
	}

	// 推送信息给视频作者
	incrUnread(ctx, db, cache, notifiacitonEvent.RecipientID)
	hub.Push(notifiacitonEvent.RecipientID, &notifiacitonEvent)

	// 推送信息给提及者
	names := extractMention(commentEvent.Content)
	// 查询提及者的id
	var users []account.User
	if err := db.WithContext(ctx).Model(&account.User{}).Where("username IN (?)", names).Find(&users).Error; err != nil {
		log.Printf("notification cannot find user,err %v", err)
		return err
	}

	for _, v := range users {
		if v.ID == commentEvent.AccountID {
			continue
		}
		mention := account.Notification{
			RecipientID:   v.ID,
			SenderID:      commentEvent.AccountID,
			Type:          "mention",
			TargetID:      commentEvent.VideoID,
			IsRead:        false,
			Content:       "提及了你",
			SourceEventID: commentEvent.EventID,
		}
		// 先入库再推送
		created, err := createNotification(ctx, db, &mention)
		if err != nil {
			return err
		}

		if !created { //幂等跳过，不用计数/推送
			return nil
		}

		incrUnread(ctx, db, cache, mention.RecipientID)
		hub.Push(v.ID, &mention)
	}
	return nil
}

// 匹配@
var mentionRegex = regexp.MustCompile(`@([\p{L}\p{N}_]+)`)

func extractMention(content string) []string {
	var names []string
	seen := map[string]bool{}
	for _, m := range mentionRegex.FindAllStringSubmatch(content, -1) {
		if !seen[m[1]] {
			seen[m[1]] = true
			names = append(names, m[1])
		}
	}
	return names
}

func processFollow(ctx context.Context, db *gorm.DB, cache *data.RedisClient, hub NotificationHub, data []byte) error {
	// 参数解析
	var socialEvent account.SocialEvent
	if err := json.Unmarshal(data, &socialEvent); err != nil { // 解析不了，说明data有问题，直接确认处理即可
		log.Printf("notificaton comment error,err: %v", err)
		return nil
	}

	// 转换为notification事件
	notifiacitonEvent := account.Notification{
		RecipientID:   socialEvent.VloggerID,
		SenderID:      socialEvent.FollowerID,
		Type:          "follow",
		TargetID:      socialEvent.FollowerID,
		IsRead:        false,
		Content:       "关注你",
		SourceEventID: socialEvent.EventID,
	}

	// 存入DB中的notifications表
	created, err := createNotification(ctx, db, &notifiacitonEvent)
	if err != nil {
		return err
	}

	if !created { //幂等跳过，不用计数/推送
		return nil
	}

	// 推送信息给在线用户
	incrUnread(ctx, db, cache, notifiacitonEvent.RecipientID)
	hub.Push(notifiacitonEvent.RecipientID, &notifiacitonEvent)
	return nil

}

// createNotification 幂等创建通知事件记录在DB
func createNotification(ctx context.Context, db *gorm.DB, notif *account.Notification) (bool, error) {
	var cnt int64

	err := db.WithContext(ctx).Model(&account.Notification{}).Where("source_event_id = ? AND recipient_id = ?", notif.SourceEventID, notif.RecipientID).Count(&cnt).Error
	if err != nil {
		return false, err
	}

	if cnt > 0 {
		return false, nil // 已经存在，幂等跳过
	}

	if err := db.WithContext(ctx).Create(notif).Error; err != nil {
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			return false, nil // 并发兜底，唯一索引挡住
		}
		return false,err
	}

	return true, nil
}
