// main 提供api服务
package main

import (
	"context"
	"feedsystem/internal/worker"
	"fmt"
	"log"
	"time"

	"feedsystem/internal/config"
	"feedsystem/internal/data"
	"feedsystem/internal/http"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
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
	mq, err := data.NewRabbitMQ(conf.RabbitMQConfig)
	if err != nil {
		log.Printf("rabbitmq connect failed (mq disabled): %v", err)
		mq = nil
	} else {
		log.Printf("RabbitMQ connected")
	}

	// 时间线链路：声明交换机+发布者+启动worker
	ctx := context.Background()
	timelinePublisher := initTimelineMQ(ctx, mq)

	// outboxPoller 无论mq 是否可用都可以使用
	worker.OutboxPoller(ctx, DB, rdb, timelinePublisher)
	if mq != nil && timelinePublisher != nil {
		worker.RunConsumer(ctx, mq, data.TimelineQueue, worker.TimelineHandler(rdb))
	}

	// 连接minio
	mc, err := data.NewMinioClient(conf.MinIOConfig)
	if err != nil {
		log.Fatalf("failed to connect minio client,err: %v", err)
	} else {
		log.Printf("minio connect success")
	}

	// 声明topo结构
	ratingPublisher := initRatingMQ(ctx, mq)

	// 装配路由并启动 HTTP 服务
	app := &http.App{
		DB:          DB,
		Cache:       rdb,
		MC:          mc,
		MQ:          mq,
		TimelinePub: timelinePublisher,
		RatingMQPub: ratingPublisher,
		Secret:      conf.JwtConfig,
	}
	router := http.SetRouter(app)
	addr := fmt.Sprintf(":%d", conf.AppConfig.Port)
	log.Printf("Server is running on %s", addr)
	if err := router.Run(addr); err != nil {
		log.Fatalf("failed to run server: %v", err)
	}
}

// initTimelineMQ 声明拓扑并创建发布者；失败返回nil
func initTimelineMQ(ctx context.Context, mq *data.RabbitMQClient) *rmq.Publisher {
	if mq == nil {
		return nil
	}

	topoCtx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()
	// 顺序：先声明拓扑，再建发布者
	if err := mq.DeclareTimelineTopology(topoCtx); err != nil {
		log.Printf("failed to declare timeline topology,err: %v", err)
		return nil
	}

	pub, err := mq.NewTimelinePublisher(topoCtx)
	if err != nil {
		log.Printf("failed to create timeline publisher,err: %v", err)
		return nil
	}
	return pub
}

// initRatingMQ 声明rating拓扑并创建发表者； 失败返回nil
func initRatingMQ(ctx context.Context, mq *data.RabbitMQClient) *rmq.Publisher {
	if mq == nil {
		return nil
	}

	topoCtx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()
	// 顺序：先声明拓扑，再建发布者
	if err := mq.DeclareRatingTopology(topoCtx); err != nil {
		log.Printf("failed to declare rating topology,err: %v", err)
		return nil
	}

	pub, err := mq.NewRatingPublisher(topoCtx)
	if err != nil {
		log.Printf("failed to create rating publisher,err: %v", err)
		return nil
	}
	return pub
}
