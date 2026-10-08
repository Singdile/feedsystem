package worker

import (
	"context"
	"encoding/json"
	"feedsystem/internal/model/account"
	"feedsystem/internal/model/video"
	"log"
	"regexp"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	"gorm.io/gorm"
)

// NotificationHub 推送通知给在线用户（SSEHub 后续实现本接口）
type NotificationHub interface {
	Push(userID uint, n *account.Notification)
}

func NotificationHandler(db *gorm.DB, hub NotificationHub) MQHandler {
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
				return processLike(ctx, db, hub, data) // todo: 处理like
			case "publish":
				return processPublish(ctx, db, hub, data)
			case "follow":
				return processFollow(ctx, db, hub, data)
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

func processLike(ctx context.Context, db *gorm.DB, hub NotificationHub, data []byte) error {
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
		RecipientID: authorID,
		SenderID:    likeEvent.AccountID,
		Type:        "like",
		TargetID:    likeEvent.VideoID,
		IsRead:      false,
		Content:     "点赞了你的视频",
	}

	// 存入DB中的notifications表
	if err := db.WithContext(ctx).Create(&notifiacitonEvent).Error; err != nil {
		log.Printf("notificaton create fail,err: %v", err)
		return err
	}

	// 推送信息给在线用户
	hub.Push(notifiacitonEvent.RecipientID, &notifiacitonEvent)
	return nil
}

func processPublish(ctx context.Context, db *gorm.DB, hub NotificationHub, data []byte) error {
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
		RecipientID: commentEvent.AuthorID,
		SenderID:    commentEvent.AccountID,
		Type:        "comment",
		TargetID:    commentEvent.VideoID,
		IsRead:      false,
		Content:     "评论了你的视频",
	}

	// 存入DB中的notifications表
	if err := db.WithContext(ctx).Create(&notifiacitonEvent).Error; err != nil {
		log.Printf("notificaton create fail,err: %v", err)
		return err
	}

	// 推送信息给视频作者
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
			RecipientID: v.ID,
			SenderID:    commentEvent.AccountID,
			Type:        "mention",
			TargetID:    commentEvent.VideoID,
			IsRead:      false,
			Content:     "提及了你",
		}
		// 先入库再推送
		if err := db.WithContext(ctx).Create(&mention).Error; err != nil {
			log.Printf("notificaton create fail,err: %v", err)
			return err
		}
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

func processFollow(ctx context.Context, db *gorm.DB, hub NotificationHub, data []byte) error {
	// 参数解析
	var socialEvent account.SocialEvent
	if err := json.Unmarshal(data, &socialEvent); err != nil { // 解析不了，说明data有问题，直接确认处理即可
		log.Printf("notificaton comment error,err: %v", err)
		return nil
	}

	// 转换为notification事件
	notifiacitonEvent := account.Notification{
		RecipientID: socialEvent.VloggerID,
		SenderID:    socialEvent.FollowerID,
		Type:        "follow",
		TargetID:    socialEvent.FollowerID,
		IsRead:      false,
		Content:     "关注你",
	}

	// 存入DB中的notifications表
	if err := db.WithContext(ctx).Create(&notifiacitonEvent).Error; err != nil {
		log.Printf("notificaton create fail,err: %v", err)
		return err
	}

	// 推送信息给在线用户
	hub.Push(notifiacitonEvent.RecipientID, &notifiacitonEvent)
	return nil

}
