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

	accountIDstr, _ := c.Get("user_id")
	accountID, ok := accountIDstr.(uint)
	if !ok {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}

	res, err := h.svc.ListFeed(c.Request.Context(), uint(accountID), cursor, limit)
	if err != nil {
		response.FromError(c, err)
		return
	}
	response.OK(c, res)
}

// ListByTag 按照单个tag name 返回视频列表
// 接收tag name 和 cursor，返回视频列表和cursor
// cursor 为空表示没有更多页了
func (h *Handler) ListByTag(c *gin.Context) {
	tagName := c.Query("tag_name")
	if tagName == "" {
		response.Fail(c, http.StatusBadRequest, "tag_name 不能为空")
		return
	}
	cursor := c.Query("cursor")
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
	if limit < 0 {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}

	accountIDstr, _ := c.Get("user_id")
	accountID, ok := accountIDstr.(uint)
	if !ok {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}

	res, err := h.svc.ListByTag(c.Request.Context(), uint(accountID), tagName, cursor, limit)
	if err != nil {
		response.FromError(c, err)
		return
	}
	response.OK(c, res)
}

// GET /feed/popular?asof & offset & limit
func (h *Handler) ListPopular(c *gin.Context) {
	// 参数获取与校验
	asof, err := strconv.ParseInt(c.DefaultQuery("asof","0"), 10, 64)
	if err != nil {
		response.Fail(c, http.StatusBadRequest, "时间戳错误")
		return
	}

	offset, err := strconv.ParseInt(c.DefaultQuery("offset","0"), 10, 64)
	if err != nil {
		response.Fail(c, http.StatusBadRequest, "offset错误")
		return
	}
	limit, err := strconv.ParseInt(c.DefaultQuery("limit","20"), 10, 64)
	if err != nil {
		response.Fail(c, http.StatusBadRequest, "limit错误")
		return
	}

	if limit <= 0 {
		limit = 20
	} else if limit > 100 {
		limit = 100
	}

	accountID, ok := currentAccountID(c)
	if !ok {
		accountID = 0
	}

	res, err := h.svc.ListByPopularity(c.Request.Context(), accountID, asof, offset, int(limit))
	if err != nil {
		response.FromError(c, err)
		return
	}
	response.OK(c, res)
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
