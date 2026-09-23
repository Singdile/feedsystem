package http

import (
	"feedsystem/internal/config"
	"feedsystem/internal/data"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	"gorm.io/gorm"
)

// App 装配所有的共享依赖
type App struct {
	DB           *gorm.DB
	Cache        *data.RedisClient
	MC           *data.MinioClient
	MQ           *data.RabbitMQClient
	TimelinePub  *rmq.Publisher
	RatingMQPub  *rmq.Publisher
	CommentMQPub *rmq.Publisher
	Secret       config.JwtConfig
}
