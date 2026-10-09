// Package http 设置api路由
package http

import (
	"feedsystem/internal/http/handler/feed"
	"feedsystem/internal/http/handler/social"
	"feedsystem/internal/http/handler/user"
	"feedsystem/internal/http/handler/video"
	"feedsystem/internal/middleware/auth"
	"feedsystem/internal/pkg/jwt"
	feedrepo "feedsystem/internal/repository/feed"
	socialrepo "feedsystem/internal/repository/social"
	userrepo "feedsystem/internal/repository/user"
	videorepo "feedsystem/internal/repository/video"
	feedsvc "feedsystem/internal/service/feed"
	socialsvc "feedsystem/internal/service/social"
	usersvc "feedsystem/internal/service/user"
	videosvc "feedsystem/internal/service/video"

	"github.com/gin-gonic/gin"
)

// SetRouter 装配全部路由与中间件
func SetRouter(app *App) *gin.Engine {
	r := gin.Default()
	// 中间件装配
	authmiddle := auth.NewAuthSecret(jwt.SetSecret(app.Secret.Secret))

	// 构造所有依赖
	userRepo := userrepo.NewuserRepo(app.DB)
	videoRepo := videorepo.NewVideoRepo(app.MC, app.DB)
	socialRepo := socialrepo.NewSocialRepo(app.DB)
	socialMQ := socialrepo.NewSocialMQ(app.SocialMQPub)
	ratingRepo := videorepo.NewRatingRepo(app.DB, app.Cache)
	commentRepo := videorepo.NewCommentRepo(app.DB, app.Cache)
	feedRepo := feedrepo.NewFeedRepo(app.Cache, app.DB)
	videoCacheClean := videorepo.NewVideoCacheCleaner(app.Cache)
	ratingProvider := videorepo.NewRatingRepo(app.DB, app.Cache)
	mqRepo := videorepo.NewRatingMQ(app.RatingMQPub)
	commentMQ := videorepo.NewCommentMQ(app.CommentMQPub)

	// services（依赖 repos）
	videoSvc := videosvc.NewVideoService(videoRepo, app.Cache, videoCacheClean, app.Cache)
	userSvc := usersvc.NewUserService(userRepo, app.Cache, socialRepo, videoRepo)
	feedSvc := feedsvc.NewFeedService(feedRepo, videoRepo, ratingProvider)
	ratingSvc := videosvc.NewRatingService(ratingRepo, videoRepo, mqRepo)
	commentSvc := videosvc.NewCommentService(commentRepo, commentMQ, videoRepo)
	socialSvc := socialsvc.NewSocialService(socialRepo, userRepo, socialMQ)

	// handlers（依赖 services）
	userHandler := user.NewHandler(userSvc)
	videoHandler := video.NewHandler(videoSvc)
	feedHandler := feed.NewHandler(feedSvc)
	ratingHandler := video.NewRatingHandler(ratingSvc)
	commentHandler := video.NewCommentHandler(commentSvc)
	socialHandler := social.NewSocialHandler(socialSvc)

	// 健康检查
	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	// 用户api
	r.POST("/api/v1/users", userHandler.Register) //创建用户

	userG := r.Group("/api/v1/users", authmiddle.JWTAuthMiddleWare(app.Cache))
	{
		userG.PUT("/password", userHandler.ChangePassword) //修改密码
		userG.GET("/:id", userHandler.GetUserByID)         //按照ID查询
		userG.GET("", userHandler.ListByUserName)          //按照username,使用query查询
	}

	r.GET("/api/v1/users/:id/profile", userHandler.GetProfile)

	// 认证
	authG := r.Group("/api/v1/auth")
	authG.POST("/login", userHandler.Login)     //用户登录
	authG.POST("/refresh", userHandler.Refresh) //刷新token
	authG.POST("/logout", userHandler.Logout)   //注销+服务端踢掉token

	// video
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
	r.GET("/api/v1/feed", authmiddle.OptionalAuthMiddleWare(app.Cache), feedHandler.ListFeed)
	r.GET("/api/v1/feed/tag", authmiddle.OptionalAuthMiddleWare(app.Cache), feedHandler.ListByTag)

	// rating video
	ratingG := r.Group("/api/v1/videos", authmiddle.JWTAuthMiddleWare(app.Cache))
	{
		ratingG.GET("/:id/video", videoHandler.GetVideoRating)
		ratingG.POST("/:id/rating", ratingHandler.SetRating)
		ratingG.GET("/:id/rating", ratingHandler.GetRating)
		ratingG.GET("/me/liked-videos", ratingHandler.ListLikedVideos)
	}

	// comment
	commentAuthG := r.Group("/api/v1/videos/:id/comments", authmiddle.JWTAuthMiddleWare(app.Cache))
	{
		commentAuthG.POST("", commentHandler.Publish) // 发布（登录）
	}

	r.GET("/api/v1/videos/:id/comments", commentHandler.List)                                                // 列表（公开）
	r.DELETE("/api/v1/comments/:comment_id", authmiddle.JWTAuthMiddleWare(app.Cache), commentHandler.Delete) // 删除（登录）

	// social
	socialG := r.Group("/api/v1/social", authmiddle.JWTAuthMiddleWare(app.Cache))
	{
		socialG.POST("follow", socialHandler.Follow)
		socialG.POST("unfollow", socialHandler.UnFollow)
		socialG.GET("followers", socialHandler.GetFollowers)
		socialG.GET("following", socialHandler.GetFollowing)
		socialG.POST("is-followed", socialHandler.IsFollowed)
		socialG.GET("counts", socialHandler.Counts)
	}
	return r
}
