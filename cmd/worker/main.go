// main 提供多种worker异步处理消息队列的信息，实现数据落盘。
package main

import (
	"context"
	"feedsystem/internal/config"
	"feedsystem/internal/data"
	"feedsystem/internal/repository/social"
	"feedsystem/internal/repository/video"
	"feedsystem/internal/worker"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
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

	// 连接redis
	rdb, err := data.NewRedis(config.Conf.RedisConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer rdb.Close()

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
	ratingSeter := video.NewRatingRepo(DB, rdb)
	worker.RunConsumer(ctx, mq, data.RatingQueue, worker.RatingHandler(ratingSeter))

	// 启动timeline
	timelinePublisher := initTimelineMQ(ctx, mq)

	// outboxPoller 无论mq 是否可用都可以使用
	worker.OutboxPoller(ctx, DB, rdb, timelinePublisher)
	socialRepo := social.NewSocialRepo(DB)
	if mq != nil && timelinePublisher != nil {
		worker.RunConsumer(ctx, mq, data.TimelineQueue, worker.TimelineHandler(rdb, socialRepo))
	}
	// 声明comment 拓扑
	if err := mq.DeclareCommentTopology(topoCtx); err != nil {
		log.Fatalf("failed to declare comment topology: %v", err)
	}

	// 启动comment
	commentWriter := video.NewCommentRepo(DB, rdb)
	worker.RunConsumer(ctx, mq, data.CommentQueue, worker.CommentHandler(commentWriter))

	// 启动popularity worker
	if err := mq.DeclarePopularityTopology(topoCtx); err != nil {
		log.Fatalf("failed to declare popularity topology: %v", err)
	}
	popularityCache := video.NewPopularityRepo(rdb)
	worker.RunConsumer(ctx, mq, data.PopularityQueue, worker.PopularityHandler(popularityCache))

	<-ctx.Done() // 阻塞等待 SIGINT/SIGTERM
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
