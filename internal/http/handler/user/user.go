// Package user 账号 HTTP 处理器
package user

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// Handler 账号 HTTP 处理器
type Handler struct{}

// NewHandler 构造账号处理器
func NewHandler() *Handler {
	return &Handler{}
}

// Register 创建用户
func (h *Handler) Register(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"msg": "register"})
}

// ChangePassword 修改密码
func (h *Handler) ChangePassword(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"msg": "change_password"})
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
	c.JSON(http.StatusOK, gin.H{"msg": "login"})
}

// Refresh 刷新token
func (h *Handler) Refresh(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"msg": "refresh"})
}

// Logout 注销+服务端踢掉token
func (h *Handler) Logout(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"msg": "logout"})
}
