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
	var timelinePub *rmq.Publisher
	if mq != nil {
		topoCtx, cancel := context.WithTimeout(ctx, time.Second*5)
		if err := mq.DeclareTimelineTopology(topoCtx); err != nil {
			log.Printf("failed to declare timeline topology,err: %v", err)
		} else if publisher, err := mq.NewTimelinePublisher(topoCtx); err != nil {
			log.Printf("failed to create timeline publisher,err: %v", err)
			timelinePub = nil
		} else {
			timelinePub = publisher
		}
		cancel()
	}

	// outboxPoller 无论mq 是否可用都可以使用
	worker.OutboxPoller(ctx, DB, rdb, timelinePub)
	if mq != nil && timelinePub != nil {
		worker.RunConsumer(ctx, mq, data.TimelineQueue, worker.TimelineHandler(rdb))
	}

	// 连接minio
	mc, err := data.NewMinioClient(conf.MinIOConfig)
	if err != nil {
		log.Fatalf("failed to connect minio client,err: %v", err)
	} else {
		log.Printf("minio connect success")
	}

	// 装配路由并启动 HTTP 服务
	router := http.SetRouter(DB, rdb, mc)
	addr := fmt.Sprintf(":%d", conf.AppConfig.Port)
	log.Printf("Server is running on %s", addr)
	if err := router.Run(addr); err != nil {
		log.Fatalf("failed to run server: %v", err)
	}
}
