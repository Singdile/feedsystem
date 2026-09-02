// Package http 设置api路由
package http

import (
	"feedsystem/internal/data"
	"feedsystem/internal/http/handler/user"
	"feedsystem/internal/middleware/auth"
	"feedsystem/internal/pkg/jwt"
	userrepo "feedsystem/internal/repository/user"
	usersvc "feedsystem/internal/service/user"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// SetRouter 装配全部路由与中间件
func SetRouter(db *gorm.DB, cache *data.RedisClient) *gin.Engine {
	r := gin.Default()

	// 健康检查
	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	// 中间件装配
	authmiddle := auth.NewAuthSecret(jwt.JwtSecrete())

	// 依赖注入，装配
	userRepo := userrepo.NewuserRepo(db)
	userSvc := usersvc.NewUserService(userRepo, cache)
	userHandler := user.NewHandler(userSvc)

	// 用户api
	r.POST("/api/v1/users", userHandler.Register) //创建用户

	userG := r.Group("/api/v1/users", authmiddle.JWTAuthMiddleWare(cache))
	{
		userG.PUT("/password", userHandler.ChangePassword) //修改密码
		userG.GET("/:id", userHandler.GetByID)             //按照ID查询
		userG.GET("", userHandler.ListByUsername)          //按照username,使用query查询
	}

	// 认证
	authG := r.Group("/api/v1/auth")
	authG.POST("/login", userHandler.Login)     //用户登录
	authG.POST("/refresh", userHandler.Refresh) //刷新token
	authG.POST("/logout", userHandler.Logout)   //注销+服务端踢掉token

	return r
}
