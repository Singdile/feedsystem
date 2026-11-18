// main 提供多种worker异步处理消息队列的信息，实现数据落盘。
package main

import (
	"context"
	"feedsystem/internal/config"
	"feedsystem/internal/data"
	"feedsystem/internal/messaging"
	"feedsystem/internal/repository/social"
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

	// 连接redis
	rdb, err := data.NewRedis(config.Conf.RedisConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer rdb.Close()

	// 连接MQ
	mq, err := messaging.NewClient(config.Conf.RabbitMQConfig)
	if err != nil {
		log.Fatal(err)
	}

	// 优雅退出（SIGINT/SIGTERM）
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 声明拓扑
	topoCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	if err := messaging.DeclareAll(topoCtx, mq); err != nil {
		cancel()
		log.Fatalf("failed to declare topology: %v", err)
	}
	cancel()

	// 启动消费
	ratingSeter := video.NewRatingRepo(DB, rdb)
	messaging.RunConsumer(ctx, mq, messaging.RatingQueue, worker.RatingHandler(ratingSeter))

	// 启动timeline
	timelinePublisher := initTimelineMQ(ctx, mq)

	// outboxPoller 无论mq 是否可用都可以使用
	worker.OutboxPoller(ctx, DB, rdb, timelinePublisher)
	socialRepo := social.NewSocialRepo(DB)
	messaging.RunConsumer(ctx, mq, messaging.TimelineQueue, worker.TimelineHandler(rdb, socialRepo))

	// 启动comment
	commentWriter := video.NewCommentRepo(DB, rdb)
	messaging.RunConsumer(ctx, mq, messaging.CommentQueue, worker.CommentHandler(commentWriter))

	// 启动popularity worker
	popularityCache := video.NewPopularityRepo(rdb)
	messaging.RunConsumer(ctx, mq, messaging.PopularityQueue, worker.PopularityHandler(popularityCache))

	<-ctx.Done() // 阻塞等待 SIGINT/SIGTERM
}

// initTimelineMQ 声明拓扑并创建发布者；失败返回nil
func initTimelineMQ(ctx context.Context, mq *messaging.Client) messaging.Publisher {
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
