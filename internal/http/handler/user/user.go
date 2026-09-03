// Package user 账号 HTTP 处理器
package user

import (
	"feedsystem/internal/http/response"
	"feedsystem/internal/model/account"
	"feedsystem/internal/service/user"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

// Handler 账号 HTTP 处理器
type Handler struct {
	svc *user.UserService
}

// NewHandler 构造账号处理器
func NewHandler(svc *user.UserService) *Handler {
	return &Handler{
		svc: svc,
	}
}

// Register 创建用户
func (h *Handler) Register(c *gin.Context) {
	var req account.RegisterReq
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, http.StatusBadRequest, "请求参数有误")
		return
	}

	if err := h.svc.Register(c.Request.Context(), req.Username, req.Password); err != nil {
		response.FromError(c, err)
		return
	}
	response.OK(c)
}

// ChangePassword 修改密码
func (h *Handler) ChangePassword(c *gin.Context) {
	var req account.ChangePasswordReq
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, http.StatusBadRequest, "请求参数有误")
		return
	}

	userid, _ := c.Get("user_id")
	userID, ok := userid.(uint)
	if !ok {
		response.Fail(c, http.StatusInternalServerError, "内部错误")
		return
	}

	req.UserID = userID
	if err := h.svc.ChangePassword(c.Request.Context(), req.UserID, req.NewPassword); err != nil {
		response.FromError(c, err)
		return
	}
	response.OK(c)
}

// Login 用户登录
func (h *Handler) Login(c *gin.Context) {
	var req account.LoginReq
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, http.StatusBadRequest, "请求参数有误")
		return
	}
	accessToken, refreshToken, err := h.svc.Login(c.Request.Context(), req.Username, req.Password)
	if err != nil {
		response.FromError(c, err)
		return
	}
	response.OK(c, gin.H{"access_token": accessToken, "refresh_token": refreshToken})
}

// Refresh 刷新access token 和 refresh token
func (h *Handler) Refresh(c *gin.Context) {
	// 提取header中的refresh token
	var req account.RefreshTokenReq
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, http.StatusBadRequest, "请求参数有误")
		return
	}

	// refresh
	accessToken, refreshToken, err := h.svc.Refresh(c.Request.Context(), req.RefreshToken)
	if err != nil {
		response.FromError(c, err)
		return
	}

	response.OK(c, gin.H{
		"access_token":  accessToken,
		"refresh_token": refreshToken,
	})
}

// Logout 注销+服务端踢掉token
func (h *Handler) Logout(c *gin.Context) {
	var req account.LogoutReq
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, http.StatusBadRequest, "请求参数有误")
		return
	}

	if err := h.svc.Logout(c.Request.Context(), req.RefreshToken); err != nil {
		response.FromError(c, err)
		return
	}
	response.OK(c)
}

// ListByUserName 用户名称模糊匹配，需登录后使用
func (h *Handler) ListByUserName(c *gin.Context) {
	username := c.Query("username")
	if username == "" {
		response.Fail(c, http.StatusBadRequest, "请求参数有误")
		return
	}

	userInfos, err := h.svc.ListByUserName(c.Request.Context(), username)
	if err != nil {
		response.FromError(c, err)
		return
	}
	response.OK(c, userInfos)
}

// GetUserByID 精确查找用户信息n
func (h *Handler) GetUserByID(c *gin.Context) {
	ID := c.Param("id")
	if ID == "" {
		response.Fail(c, http.StatusBadRequest, "请求参数有误")
		return

	}

	id, err := strconv.ParseUint(ID, 10, 64)
	if err != nil {
		response.Fail(c, http.StatusBadRequest, "请求参数有误")
		return
	}

	userinfo, err := h.svc.GetUserByID(c.Request.Context(), uint(id))
	if err != nil {
		response.FromError(c, err)
		return
	}

	response.OK(c, userinfo)
}
