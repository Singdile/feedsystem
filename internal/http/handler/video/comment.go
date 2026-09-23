package video

import (
	"feedsystem/internal/http/response"
	"feedsystem/internal/service/video"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

type CommentHandler struct {
	svc *video.CommentService
}

func NewCommentHandler(svc *video.CommentService) *CommentHandler {
	return &CommentHandler{svc: svc}
}

// Publish /videos/:id/comments
// 输入 videoID,  accountID, username, content
func (h *CommentHandler) Publish(c *gin.Context) {
	// 获取参数
	videoID, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}

	accountID, ok := currentAccountID(c)
	if !ok {
		response.Fail(c, http.StatusUnauthorized, "未登录")
		return
	}

	username, _ := c.Get("username")
	userName, ok := username.(string)
	if !ok {
		response.Fail(c, http.StatusUnauthorized, "未登录")
		return
	}

	type Req struct {
		Content string `json:"content"`
	}

	var req Req
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}

	err = h.svc.Publish(c.Request.Context(), uint(videoID), accountID, userName, req.Content)
	if err != nil {
		response.FromError(c, err)
		return
	}

	response.OK(c)
}

// Delete /comments/:comment_id
// 输入 comment_id accountID
func (h *CommentHandler) Delete(c *gin.Context) {
	// 获取参数
	accountID, ok := currentAccountID(c)
	if !ok {
		response.Fail(c, http.StatusUnauthorized, "未登录")
		return
	}

	commentID, err := strconv.ParseUint(c.Param("comment_id"), 10, 64)
	if err != nil {
		response.Fail(c, http.StatusBadRequest, err.Error())
		return
	}

	err = h.svc.Delete(c.Request.Context(), uint(accountID), uint(commentID))
	if err != nil {
		response.FromError(c, err)
		return
	}
	response.OK(c)
}

// List  /videos/:id/comments?cursor= &limit=
func (h *CommentHandler) List(c *gin.Context) {
	// 参数获取
	videoID, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}

	cursorStr := c.Query("cursor")
	limitStr := c.DefaultQuery("limit", "20")
	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		response.Fail(c, http.StatusBadRequest, "参数错误", gin.H{
			"limit": limit,
		})
		return
	}

	if limit <= 0 || limit > 100 {
		limit = 20
	}

	// 调用业务
	comments, next, err := h.svc.ListComments(c.Request.Context(), uint(videoID), cursorStr, int8(limit))
	if err != nil {
		response.FromError(c, err)
		return
	}
	response.OK(c, gin.H{
		"comments": comments,
		"cursor":   next,
	})
}
