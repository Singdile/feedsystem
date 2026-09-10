// Package feed 提供视频流接口
package feed

import (
	"feedsystem/internal/http/response"
	"feedsystem/internal/service/feed"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	svc *feed.FeedService
}

func NewHandler(svc *feed.FeedService) *Handler {
	return &Handler{svc: svc}
}

// ListFeed 按照时间顺序，根据游标信息，请求一组可播放的视频信息
func (h *Handler) ListFeed(c *gin.Context) {
	cursor := c.Query("cursor")
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
	if limit < 0 {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}
	res, err := h.svc.ListFeed(c.Request.Context(), cursor, limit)
	if err != nil {
		response.FromError(c, err)
		return
	}
	response.OK(c, res)
}
