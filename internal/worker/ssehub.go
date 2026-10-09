package worker

import (
	"encoding/json"
	"feedsystem/internal/http/response"
	"feedsystem/internal/model/account"
	"feedsystem/internal/model/video"
	"feedsystem/internal/pkg/jwt"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type SSEHub struct {
	mu      sync.RWMutex                        // 保护clients 并发访问
	clients map[uint]chan *account.Notification // 用户id->channel
	db      *gorm.DB
	key     []byte //jwt 签名密钥
}

func NewSSEHub(db *gorm.DB, key []byte) *SSEHub {
	return &SSEHub{
		clients: make(map[uint]chan *account.Notification),
		db:      db,
		key:     key,
	}
}

// 用户连接管理
func (h *SSEHub) Subscribe(userID uint) chan *account.Notification {
	ch := make(chan *account.Notification, 20)
	h.mu.Lock()
	defer h.mu.Unlock()

	if old, ok := h.clients[userID]; ok { //新标签页接管
		close(old)
	}

	h.clients[userID] = ch
	return ch
}

func (h *SSEHub) UnSubscribe(userID uint, ch chan *account.Notification) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if cur, ok := h.clients[userID]; ok && cur == ch {
		delete(h.clients, userID) //map删除该用户的连接记录
		close(ch)                 //关闭通道
	}
}

// Push 实现 NotificationHub 接口
func (h *SSEHub) Push(userID uint, n *account.Notification) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	ch, ok := h.clients[userID]
	if !ok {
		return
	}

	select {
	case ch <- n:
	default: //缓冲满丢弃，不阻塞
	}
}

// currentAccountID 从 JWT 上下文取当前用户
func currentAccountID(c *gin.Context) (uint, bool) {
	v, exist := c.Get("user_id")
	if !exist {
		return 0, false
	}
	id, ok := v.(uint)
	return id, ok
}

// SSERequireAuth 校验 query token 或 Bearer（EventSource 无法带 header）
func (h *SSEHub) SSERequireAuth(key []byte) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 从query中获取token
		tokenStr := c.Query("token")
		claim, err := jwt.ParseToken(key, tokenStr)

		if err != nil {
			response.Fail(c, http.StatusUnauthorized, "invalid or expired token")
			c.Abort()
			return
		}

		// 如果有效，允许通过
		// 设置用户信息，方便后续的接口调用
		c.Set("user_id", claim.AccountID)
		c.Set("username", claim.Username)
		c.Next()
	}
}

// SSEHandler
// 建立并维持一条 SSE 长连接，把 channel 里实时收到的通知推给浏览器。
func (h *SSEHub) SSEHandler(c *gin.Context) {
	// 获取用户id
	userID, ok := currentAccountID(c)
	if !ok {
		response.Fail(c, http.StatusUnauthorized, "用户未登录")
		return
	}

	// 设置 SSE 响应头
	c.Writer.Header().Set("Content-type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")

	// 登记链接,给该用户开一条 channel 存进 SSEHub
	ch := h.Subscribe(userID)
	defer h.UnSubscribe(userID, ch)

	// 循环推送信息给浏览器
	for {
		select {
		case <-c.Request.Context().Done():
			return
		case n := <-ch:
			b, _ := json.Marshal(n)
			fmt.Fprintf(c.Writer, "data: %s\n\n", b)
			c.Writer.Flush()
		case <-time.After(30 * time.Second):
			fmt.Fprintf(c.Writer, ": keepalive\n\n")
			c.Writer.Flush()
		}
	}
}

// GET /notifications/list?cursor=xx&limit=20
// ListHandler 当前用户的历史通知,采用游标分页查询
func (h *SSEHub) ListHandler(c *gin.Context) {
	// 获取当前用户id
	userID, ok := currentAccountID(c)
	if !ok {
		response.Fail(c, http.StatusUnauthorized, "用户未登录")
		return
	}

	limit := 20
	if v, err := strconv.Atoi(c.Query("limit")); err == nil && v > 0 {
		limit = v
	}

	if limit > 100 {
		limit = 100
	}

	// 解析游标
	var cursor *video.Cursor
	if cursorStr := c.Query("cursor"); cursorStr != "" {
		cur, err := video.DecodeCursor(cursorStr)
		if err != nil {
			response.Fail(c, http.StatusBadRequest, "游标无效")
			return
		}
		cursor = &cur
	}

	// 查询
	q := h.db.WithContext(c.Request.Context()).Model(&account.Notification{}).Where("recipient_id = ?", userID)
	if cursor != nil {
		q = q.Where("created_at < ? OR (created_at = ? AND id < ?)", cursor.CreatedAt, cursor.CreatedAt, cursor.ID)
	}

	var notifications []account.Notification
	if err := q.Order("created_at desc,id desc").Limit(limit + 1).Find(&notifications).Error; err != nil {
		response.Fail(c, http.StatusInternalServerError, "查询失败")
		return
	}

	// 查看查询结果，是否还有下一页
	hasmore := len(notifications) > limit
	if hasmore {
		notifications = notifications[:limit]
	}

	// 游标更新
	next := ""
	if hasmore && len(notifications) > 0 {
		last := notifications[len(notifications)-1]
		cur := video.Cursor{
			ID:        last.ID,
			CreatedAt: last.CreatedAt,
		}
		next = video.EncodeCursor(cur)
	}

	// 返回响应
	response.OK(c, gin.H{
		"notifications": notifications,
		"next_cursor":   next,
	})
}

// MarkReadHandler 标记已读
// POST /notifications/mark-read
func (h *SSEHub) MarkReadHandler(c *gin.Context) {
	// 获取当前用户id
	userID, ok := currentAccountID(c)
	if !ok {
		response.Fail(c, http.StatusUnauthorized, "用户未登录")
		return
	}

	// 获取notification id
	var req struct {
		ID uint `json:"id"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}

	// 开始标记
	q := h.db.WithContext(c.Request.Context()).Model(&account.Notification{})
	if req.ID == 0 { //将所有的未读信息，标记为已读
		q = q.Where("recipient_id = ?", userID)
	} else {
		q = q.Where("id = ? AND recipient_id = ?", req.ID, userID)
	}
	if err := q.Update("is_read", true).Error; err != nil {
		response.Fail(c, http.StatusInternalServerError, "标记失败")
		return
	}

	response.OK(c)
}

// GET /notifications/unread-count
// UnreadCountHandler 当前用户未读通知数（红点）
func (h *SSEHub) UnreadCountHandler(c *gin.Context) {
	userID, ok := currentAccountID(c)
	if !ok {
		response.Fail(c, http.StatusUnauthorized, "用户未登录")
		return
	}

	var count int64
	if err := h.db.WithContext(c.Request.Context()).
		Model(&account.Notification{}).
		Where("recipient_id = ? AND is_read = ?", userID, false).
		Count(&count).Error; err != nil {
		response.Fail(c, http.StatusInternalServerError, "查询失败")
		return
	}

	response.OK(c, gin.H{"count": count})
}

// RegisterRoutes 注册通知路由
func (h *SSEHub) RegisterRoutes(r *gin.Engine) {
	notif := r.Group("/api/v1/notifications", h.SSERequireAuth(h.key))
	{
		notif.GET("/stream", h.SSEHandler)
		notif.GET("/list", h.ListHandler)
		notif.POST("/mark-read", h.MarkReadHandler)
		notif.GET("/unread-count", h.UnreadCountHandler)
	}
}
