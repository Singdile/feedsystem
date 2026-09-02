// Package user 账号 HTTP 处理器
package user

import (
	"feedsystem/internal/http/response"
	"feedsystem/internal/model/account"
	"feedsystem/internal/service/user"
	"net/http"

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

// GetByID 按照ID查询
func (h *Handler) GetByID(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"msg": "get_by_id"})
}

// ListByUsername 按照username,使用query查询
func (h *Handler) ListByUsername(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"msg": "list_by_username"})
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
