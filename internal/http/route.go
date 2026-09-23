// Package http 设置api路由
package http

import (
	"feedsystem/internal/http/handler/feed"
	"feedsystem/internal/http/handler/user"
	"feedsystem/internal/http/handler/video"
	"feedsystem/internal/middleware/auth"
	"feedsystem/internal/pkg/jwt"
	feedrepo "feedsystem/internal/repository/feed"
	userrepo "feedsystem/internal/repository/user"
	videorepo "feedsystem/internal/repository/video"
	feedsvc "feedsystem/internal/service/feed"
	usersvc "feedsystem/internal/service/user"
	videosvc "feedsystem/internal/service/video"

	"github.com/gin-gonic/gin"
)

// SetRouter 装配全部路由与中间件
func SetRouter(app *App) *gin.Engine {
	r := gin.Default()

	// 健康检查
	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	// 中间件装配
	authmiddle := auth.NewAuthSecret(jwt.SetSecret(app.Secret.Secret))

	// 依赖注入，装配
	userRepo := userrepo.NewuserRepo(app.DB)
	userSvc := usersvc.NewUserService(userRepo, app.Cache)
	userHandler := user.NewHandler(userSvc)

	// 用户api
	r.POST("/api/v1/users", userHandler.Register) //创建用户

	userG := r.Group("/api/v1/users", authmiddle.JWTAuthMiddleWare(app.Cache))
	{
		userG.PUT("/password", userHandler.ChangePassword) //修改密码
		userG.GET("/:id", userHandler.GetUserByID)         //按照ID查询
		userG.GET("", userHandler.ListByUserName)          //按照username,使用query查询
	}

	// 认证
	authG := r.Group("/api/v1/auth")
	authG.POST("/login", userHandler.Login)     //用户登录
	authG.POST("/refresh", userHandler.Refresh) //刷新token
	authG.POST("/logout", userHandler.Logout)   //注销+服务端踢掉token

	// video
	videoRepo := videorepo.NewVideoRepo(app.MC, app.DB)
	videoCacheClean := videorepo.NewVideoCacheCleaner(app.Cache)
	videoSvc := videosvc.NewVideoService(videoRepo, app.Cache, videoCacheClean, app.Cache)
	videoHandler := video.NewHandler(videoSvc)
	uploadG := r.Group("/api/v1/uploads/videos", authmiddle.JWTAuthMiddleWare(app.Cache))
	{
		uploadG.POST("/init", videoHandler.Init)
		uploadG.GET("/:uploadID/status", videoHandler.GetLoadStatus)
		uploadG.POST("/:uploadID/complete", videoHandler.CompleteUpload)
		uploadG.POST("/:uploadID/abort", videoHandler.AbortUpload)

		uploadG.POST("/covers", videoHandler.UpLoadConver)
		uploadG.POST("/publish", videoHandler.Publish)

	}

	r.GET("/api/v1/videos/:id", videoHandler.GetVideo)
	r.GET("/api/v1/videos", videoHandler.ListVideos)
	r.DELETE("/api/v1/videos/:id", authmiddle.JWTAuthMiddleWare(app.Cache), videoHandler.DeleteVideo)

	// feed
	feedRepo := feedrepo.NewFeedRepo(app.Cache, app.DB)
	feedSvc := feedsvc.NewFeedService(feedRepo, videoRepo)
	feedHandler := feed.NewHandler(feedSvc)
	r.GET("/api/v1/feed", feedHandler.ListFeed)
	r.GET("/api/v1/feed/tag", feedHandler.ListByTag)

	// rating video
	ratingRepo := videorepo.NewRatingRepo(app.DB)
	mqRepo := videorepo.NewRatingMQ(app.RatingMQPub)
	ratingSvc := videosvc.NewRatingService(ratingRepo, videoRepo, mqRepo)
	ratingHandler := video.NewRatingHandler(ratingSvc)
	ratingG := r.Group("/api/v1/videos", authmiddle.JWTAuthMiddleWare(app.Cache))
	{
		ratingG.GET("/:id/video", videoHandler.GetVideoRating)
		ratingG.POST("/:id/rating", ratingHandler.SetRating)
		ratingG.GET("/:id/rating", ratingHandler.GetRating)
		ratingG.GET("/me/liked-videos", ratingHandler.ListLikedVideos)
	}

	// comment
	commentRepo := videorepo.NewCommentRepo(app.DB)
	commentMQ := videorepo.NewCommentMQ(app.CommentMQPub)
	commentSvc := videosvc.NewCommentService(commentRepo, commentMQ, videoRepo) // videoRepo 有 FindByID → 满足 VideoChecker
	commentHandler := video.NewCommentHandler(commentSvc)

	commentAuthG := r.Group("/api/v1/videos/:id/comments", authmiddle.JWTAuthMiddleWare(app.Cache))
	{
		commentAuthG.POST("", commentHandler.Publish) // 发布（登录）
	}

	r.GET("/api/v1/videos/:id/comments", commentHandler.List)                                                // 列表（公开）
	r.DELETE("/api/v1/comments/:comment_id", authmiddle.JWTAuthMiddleWare(app.Cache), commentHandler.Delete) // 删除（登录）
	return r
}
