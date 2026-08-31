// Package http 设置api路由
package http

import (
	"feedsystem/internal/http/handler/user"

	"github.com/gin-gonic/gin"
)

// SetRouter 装配全部路由与中间件
func SetRouter() *gin.Engine {
	r := gin.Default()

	// 健康检查
	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	userHandler := user.NewHandler()

	// 用户api
	userG := r.Group("/api/v1/users")
	userG.POST("", userHandler.Register)                   //创建用户
	userG.PUT("/:id/password", userHandler.ChangePassword) //修改密码
	userG.GET("/:id", userHandler.GetByID)                 //按照ID查询
	userG.GET("", userHandler.ListByUsername)              //按照username,使用query查询

	// 认证
	authG := r.Group("/api/v1/auth")
	authG.POST("/login", userHandler.Login)     //用户登录
	authG.POST("/refresh", userHandler.Refresh) //刷新token
	authG.POST("/logout", userHandler.Logout)   //注销+服务端踢掉token

	return r
}
