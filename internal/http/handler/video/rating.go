package video

import (
	"feedsystem/internal/http/response"
	videoModel "feedsystem/internal/model/video"
	"feedsystem/internal/service/video"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

type RatingHandler struct {
	svc *video.RatingService
}

func NewRatingHandler(svc *video.RatingService) *RatingHandler {
	return &RatingHandler{
		svc: svc,
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

// SetRating POST /videos/:id/rating
// body: {"action":"like|dislike|none"}
func (h *RatingHandler) SetRating(c *gin.Context) {
	// 获取视频id
	videoID, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}

	// 获取用户id
	accountID, ok := currentAccountID(c)
	if !ok {
		response.Fail(c, http.StatusUnauthorized, "未登录")
		return
	}

	var req struct {
		Action string `json:"action"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}

	actionNum, err := videoModel.StringToStatus(req.Action)
	if err != nil {
		response.Fail(c, http.StatusBadRequest, err.Error())
		return
	}

	stat, err := h.svc.SetUserRating(c.Request.Context(), accountID, uint(videoID), actionNum)
	if err != nil {
		response.Fail(c, http.StatusInternalServerError, err.Error())
		return
	}

	response.OK(c, gin.H{
		"stat": stat,
	})
}

// GetRating GET /videos/:id/rating
func (h *RatingHandler) GetRating(c *gin.Context) {
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
	stat, err := h.svc.GetUserRating(c.Request.Context(), accountID, uint(videoID))
	if err != nil {
		response.FromError(c, err)
		return
	}

	response.OK(c, gin.H{"stat": stat})
}

// ListLikedVideos GET /videos/me/liked-videos
func (h *RatingHandler) ListLikedVideos(c *gin.Context) {
	accountID, ok := currentAccountID(c)
	if !ok {
		response.Fail(c, http.StatusUnauthorized, "未登录")
		return
	}
	limit := 20
	if s := c.Query("limit"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			limit = n
		}
	}
	videos, nextCursor, err := h.svc.ListLikedVideos(c.Request.Context(), accountID, c.Query("cursor"), limit)
	if err != nil {
		response.FromError(c, err)
		return
	}
	response.OK(c, gin.H{"videos": videos, "next_cursor": nextCursor})
}
