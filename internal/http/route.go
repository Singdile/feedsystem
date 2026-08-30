// Package http 设置api路由
package http

import "github.com/gin-gonic/gin"

func SetRouter() *gin.Engine {
	r := gin.Default()

	// 用户api
	userG := r.Group("/api/v1/users")
	userG.POST("", func(*gin.Context) {})             //创建用户
	userG.PUT("/:id/password", func(*gin.Context) {}) //修改密码
	userG.GET("/:id", func(*gin.Context) {})          //按照ID查询
	userG.GET("", func(*gin.Context) {})              //按照username,使用query查询

	// 认证
	authG := r.Group("/api/v1/auth")
	authG.POST("/login", func(*gin.Context) {})   //用户登录
	authG.POST("/refresh", func(*gin.Context) {}) //刷新token
	authG.POST("/logout", func(*gin.Context) {})  //注销+服务端踢掉token

	return r
}
