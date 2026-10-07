package social

import (
	"feedsystem/internal/http/response"
	"feedsystem/internal/service/social"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

type SocialHandler struct {
	svc *social.SocialService
}

func NewSocialHandler(svc *social.SocialService) *SocialHandler {
	return &SocialHandler{
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

// POST /api/v1/social/follow  (JWT)
func (h *SocialHandler) Follow(c *gin.Context) {
	// 获取参数
	accountID, ok := currentAccountID(c)
	if !ok {
		response.Fail(c, http.StatusUnauthorized, "未登录")
		return
	}

	vloggerIDStr := c.Query("vlogger_id")
	if vloggerIDStr == "" {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}

	vloggerID, err := strconv.ParseUint(vloggerIDStr, 10, 64)
	if err != nil {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}

	err = h.svc.Follow(c.Request.Context(), accountID, uint(vloggerID))
	if err != nil {
		response.FromError(c, err)
		return
	}
	response.OK(c)
}

func (h *SocialHandler) UnFollow(c *gin.Context) {
	// 获取参数
	accountID, ok := currentAccountID(c)
	if !ok {
		response.Fail(c, http.StatusUnauthorized, "未登录")
		return
	}

	vloggerIDStr := c.Query("vlogger_id")
	if vloggerIDStr == "" {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}

	vloggerID, err := strconv.ParseUint(vloggerIDStr, 10, 64)
	if err != nil {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}

	err = h.svc.UnFollow(c.Request.Context(), accountID, uint(vloggerID))
	if err != nil {
		response.FromError(c, err)
		return
	}
	response.OK(c)
}

// GET  /api/v1/social/followers   ?vlogger_id=可空默认当前 (JWT)
func (h *SocialHandler) GetFollowers(c *gin.Context) {
	// 获取参数
	accountID, ok := currentAccountID(c)
	if !ok {
		response.Fail(c, http.StatusUnauthorized, "未登录")
		return
	}

	followers, err := h.svc.GetAllFollowers(c.Request.Context(), accountID)
	if err != nil {
		response.FromError(c, err)
		return
	}

	response.OK(c, gin.H{
		"followers": followers,
	})
}

// GET  /api/v1/social/following   (JWT)
func (h *SocialHandler) GetFollowing(c *gin.Context) {
	// 获取参数
	accountID, ok := currentAccountID(c)
	if !ok {
		response.Fail(c, http.StatusUnauthorized, "未登录")
		return
	}

	vloggers, err := h.svc.GetAllVloggers(c.Request.Context(), accountID)
	if err != nil {
		response.FromError(c, err)
		return
	}

	response.OK(c, gin.H{
		"vloggers": vloggers,
	})
}

func (h *SocialHandler) IsFollowed(c *gin.Context) {
	// 获取参数
	accountID, ok := currentAccountID(c)
	if !ok {
		response.Fail(c, http.StatusUnauthorized, "未登录")
		return
	}

	vloggerIDStr := c.Query("vlogger_id")
	if vloggerIDStr == "" {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}

	vloggerID, err := strconv.ParseUint(vloggerIDStr, 10, 64)
	if err != nil {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}

	ok, err = h.svc.IsFollow(c.Request.Context(), accountID, uint(vloggerID))
	if err != nil {
		response.FromError(c, err)
		return
	}

	response.OK(c, gin.H{
		"followed": ok,
	})
}

// GET /api/v1/social/counts  (JWT)
func (h *SocialHandler) Counts(c *gin.Context) {
	// 获取参数
	accountID, ok := currentAccountID(c)
	if !ok {
		response.Fail(c, http.StatusUnauthorized, "未登录")
		return
	}

	followersCnt, followingCnt, err := h.svc.Counts(c.Request.Context(), accountID)
	if err != nil {
		response.FromError(c, err)
		return
	}

	response.OK(c, gin.H{
		"follower_count": followersCnt,
		"vlogger_count":  followingCnt,
	})
}
