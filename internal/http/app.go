package http

import (
	"feedsystem/internal/config"
	"feedsystem/internal/data"
	"feedsystem/internal/messaging"

	"gorm.io/gorm"
)

// App 装配所有的共享依赖
type App struct {
	DB            *gorm.DB
	Cache         *data.RedisClient
	MC            *data.MinioClient
	RatingMQPub   *messaging.Publisher
	CommentMQPub  *messaging.Publisher
	SocialMQPub   *messaging.Publisher
	PopularityPub *messaging.Publisher
	Secret        config.JwtConfig
}
