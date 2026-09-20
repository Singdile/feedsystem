// main 提供多种worker异步处理消息队列的信息，实现数据落盘。
package main

import (
	"context"
	"feedsystem/internal/config"
	"feedsystem/internal/data"
	"feedsystem/internal/repository/video"
	"feedsystem/internal/worker"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// 加载配置
	config.Init()

	// 连接DB
	DB, err := data.NewDB(config.Conf.DBConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer data.CloseDB(DB)
	// 连接MQ
	mq, err := data.NewRabbitMQ(config.Conf.RabbitMQConfig)
	if err != nil {
		log.Fatal(err)
	}

	// 优雅退出（SIGINT/SIGTERM）
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 声明rating 拓扑
	topoCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := mq.DeclareRatingTopology(topoCtx); err != nil {
		log.Fatalf("failed to declare rating topology: %v", err)
	}

	// 启动消费
	ratingSeter := video.NewRatingRepo(DB)
	worker.RunConsumer(ctx, mq, data.RatingQueue, worker.RatingHandler(ratingSeter))

	<-ctx.Done() // 阻塞等待 SIGINT/SIGTERM
}
