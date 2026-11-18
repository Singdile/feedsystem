// main 提供api服务
package main

import (
	"context"
	"feedsystem/internal/messaging"
	"feedsystem/internal/pkg/jwt"
	"feedsystem/internal/worker"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"feedsystem/internal/config"
	"feedsystem/internal/data"
	feedHttp "feedsystem/internal/http"
)

func main() {
	// 加载配置
	if err := config.Init(); err != nil {
		log.Panic(err)
	}

	conf := config.Conf

	// 连接数据库
	DB, err := data.NewDB(conf.DBConfig)
	if err != nil {
		log.Fatalf("failed to connect database,err: %v", err)
	}
	defer data.CloseDB(DB)

	if err := data.AutoMigrate(DB); err != nil {
		log.Fatalf("failed to auto migrate,err: %v", err)
	}

	// 连接redis
	rdb, err := data.NewRedis(conf.RedisConfig)
	if err != nil {
		log.Fatalf("falied to connect redis,err:%v", err)
	}
	defer rdb.Close()

	// 连接rabbitmq（MQ 不可用时降级禁用，不阻塞启动）
	mq, err := messaging.NewClient(conf.RabbitMQConfig)
	if err != nil {
		log.Printf("rabbitmq connect failed (mq disabled): %v", err)
		mq = nil
	} else {
		log.Printf("RabbitMQ connected")
	}

	var (
		ratingPublisher     messaging.Publisher
		commentPublisher    messaging.Publisher
		socialPublisher     messaging.Publisher
		popularityPublisher messaging.Publisher
	)

	if mq != nil {
		topCtx, cancelTopo := context.WithTimeout(context.Background(), 30*time.Second)
		err := messaging.DeclareAll(topCtx, mq)
		cancelTopo()
		if err != nil {
			log.Printf("rabbitmq topology declaration failed: %v", err)
			mq = nil
		}
	}

	if mq != nil {
		pubCtx, cancelPub := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelPub()

		ratingPublisher = initPublisher("rating", pubCtx, mq.NewRatingPublisher)
		commentPublisher = initPublisher("comment", pubCtx, mq.NewCommentPublisher)
		socialPublisher = initPublisher("social", pubCtx, mq.NewSocialPublisher)
		popularityPublisher = initPublisher("popularity", pubCtx, mq.NewPopularityPublisher)
	}

	// 连接minio
	mc, err := data.NewMinioClient(conf.MinIOConfig)
	if err != nil {
		log.Fatalf("failed to connect minio client,err: %v", err)
	} else {
		log.Printf("minio connect success")
	}

	// 创建 SSEHub
	hub := worker.NewSSEHub(DB, rdb, jwt.SetSecret(conf.JwtConfig.Secret))
	// 启动 NotificationWorker 消费 notification.events 队列
	if mq != nil {
		messaging.RunConsumer(context.Background(), mq, messaging.NotificationQueue, worker.NotificationHandler(DB, hub, rdb))
	}

	// 装配路由并启动 HTTP 服务
	app := &feedHttp.App{
		DB:            DB,
		Cache:         rdb,
		MC:            mc,
		RatingMQPub:   ratingPublisher,
		CommentMQPub:  commentPublisher,
		SocialMQPub:   socialPublisher,
		PopularityPub: popularityPublisher,
		Secret:        conf.JwtConfig,
	}
	router := feedHttp.SetRouter(app)
	hub.RegisterRoutes(router)

	// 在非生产环境启动 pprof (独立端口)
	if os.Getenv("FEED_ENV") != "prod" {
		go func() {
			log.Println("pprof listening on 127.0.0.1:6060")
			if err := http.ListenAndServe("127.0.0.1:6060", nil); err != nil {
				log.Printf("pprof server stopped: %v", err)
			}
		}()
	}
	addr := fmt.Sprintf(":%d", conf.AppConfig.Port)
	log.Printf("Server is running on %s", addr)
	if err := router.Run(addr); err != nil {
		log.Fatalf("failed to run server: %v", err)
	}
}
func initPublisher(name string, ctx context.Context, f func(context.Context) (messaging.Publisher, error)) messaging.Publisher {
	pub, err := f(ctx)
	if err != nil {
		log.Printf("failed to create %s publisher: %v", name, err)
		return nil
	}
	return pub
}
