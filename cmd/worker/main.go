// main 提供多种worker异步处理消息队列的信息，实现数据落盘。
package main

import (
	"context"
	"feedsystem/internal/config"
	"feedsystem/internal/data"
	"feedsystem/internal/messaging"
	"feedsystem/internal/messaging/rabbitmq"
	outboxrepo "feedsystem/internal/repository/outbox"
	"feedsystem/internal/repository/social"
	"feedsystem/internal/repository/video"
	"feedsystem/internal/worker"
	"log"
	"os"
	"os/signal"
	"sync"
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
	mq, err := rabbitmq.NewClient(config.Conf.RabbitMQConfig)
	if err != nil {
		log.Fatal(err)
	}

	// 优雅退出（SIGINT/SIGTERM）
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 声明拓扑
	topoCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	if err := rabbitmq.DeclareAll(topoCtx, mq); err != nil {
		cancel()
		log.Fatalf("failed to declare topology: %v", err)
	}
	cancel()

	// 启动timeline
	timelinePublisher := initTimelineMQ(ctx, mq)

	// outboxPoller：publisher 可用时持续投递视频发布时间线事件
	outboxStore := outboxrepo.NewOutboxRepo(DB)
	outboxPoller := worker.NewOutboxPoller(outboxStore, timelinePublisher, worker.OutboxConfig{BatchSize: 100, PublishTimeout: 5 * time.Second, PollInterval: time.Second, ReclaimAfter: 5 * time.Minute, MaxAttempts: 10})

	var wg sync.WaitGroup

	wg.Go(func() { outboxPoller.Run(ctx) })
	// 多线处理队列
	run := func(cfg messaging.ConsumerConfig, h messaging.Handler) {
		log.Printf("consumer %s start: concurrency=%d prefetch=%d timeout=%s", cfg.Queue, cfg.Concurrency, cfg.Prefetch, cfg.Timeout)

		if err := messaging.NewConsumer(mq).Consume(ctx, cfg, h); err != nil {
			log.Printf("consumer %s stopped: %v", cfg.Queue, err)
		}
	}

	// 启动rating 消费
	ratingSeter := video.NewRatingRepo(DB, rdb)
	wg.Go(
		func() {
			run(
				messaging.ConsumerConfig{
					Queue:       messaging.RatingQueue,
					Concurrency: 4,
					Prefetch:    16,
					Timeout:     3 * time.Second,
				},
				worker.RatingHandler(ratingSeter),
			)
		},
	)

	// 启动comment 消费
	commentWriter := video.NewCommentRepo(DB, rdb)
	wg.Go(
		func() {
			run(messaging.ConsumerConfig{
				Queue:       messaging.CommentQueue,
				Concurrency: 1,
				Prefetch:    4,
				Timeout:     5 * time.Second,
			}, worker.CommentHandler(commentWriter))
		},
	)

	//启动popularity 消费
	popularityCache := video.NewPopularityRepo(rdb)
	wg.Go(
		func() {
			run(messaging.ConsumerConfig{
				Queue:       messaging.PopularityQueue,
				Concurrency: 8,
				Prefetch:    32,
				Timeout:     2 * time.Second,
			}, worker.PopularityHandler(popularityCache))
		},
	)

	//启动 timeline 消费
	socialRepo := social.NewSocialRepo(DB)
	wg.Go(
		func() {
			run(messaging.ConsumerConfig{
				Queue:       messaging.TimelineQueue,
				Concurrency: 4,
				Prefetch:    16,
				Timeout:     5 * time.Second,
			}, worker.TimelineHandler(rdb, socialRepo))
		},
	)
	<-ctx.Done() // 阻塞等待 SIGINT/SIGTERM
	wg.Wait()    // 等所有消费者 drain 完成后，再由 defer 关闭 DB/Redis
}

// initTimelineMQ 声明拓扑并创建发布者；失败返回nil
func initTimelineMQ(ctx context.Context, mq *rabbitmq.Client) messaging.Publisher {
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
