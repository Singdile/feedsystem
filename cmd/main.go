// main 提供api服务
package main

import (
	"context"
	"errors"
	"feedsystem/internal/messaging"
	"feedsystem/internal/messaging/rabbitmq"
	"feedsystem/internal/pkg/jwt"
	"feedsystem/internal/worker"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"feedsystem/internal/config"
	"feedsystem/internal/data"
	feedHttp "feedsystem/internal/http"
)

const shutdownTimeout = 5 * time.Second

func main() {
	// 加载配置
	if err := config.Init(); err != nil {
		log.Panic(err)
	}

	conf := config.Conf

	// 优雅退出：SIGINT / SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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
	mq, err := rabbitmq.NewClient(conf.RabbitMQConfig)
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
		err := rabbitmq.DeclareAll(topCtx, mq)
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

	consumerCtx, cancelConsumer := context.WithCancel(ctx)
	defer cancelConsumer()

	var consumerWG sync.WaitGroup
	// 启动 NotificationWorker 消费 notification.events 队列
	if mq != nil {
		consumerWG.Go(func() {
			cfg := messaging.ConsumerConfig{
				Queue:       messaging.NotificationQueue,
				Concurrency: 2,
				Prefetch:    8,
				Timeout:     5 * time.Second,
			}
			if err := messaging.NewConsumer(mq).Consume(consumerCtx, cfg,
				worker.NotificationHandler(DB, hub, rdb)); err != nil {
				log.Printf("notification consumer stopped: %v", err)
			}
		})
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

	srv := http.Server{
		Addr:    addr,
		Handler: router,
	}
	log.Printf("Server is running on %s", addr)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("failed to run server: %v", err)
		}
	}()

	<-ctx.Done() //等待结束信号
	stop()       // 恢复默认信号处理：再次收到信号可立即退出
	log.Printf("shutdown: signal received")
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancelShutdown()

	var wg sync.WaitGroup
	wg.Add(2)

	// 1. 停止接收新请求，等待在途请求
	go func() {
		defer wg.Done()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			// 当前 SSE 长连接不会自己返回，通常会在这里超时
			log.Printf("http shutdown: %v, forcing close", err)
			_ = srv.Close()
		}
	}()

	// 2. 取消消费者并等待 drain
	go func() {
		defer wg.Done()
		cancelConsumer()
		consumerWG.Wait()
		log.Printf("notification consumer stopped")
	}()

	wg.Wait()

	// 3. 所有使用 MQ 的地方都结束后，再关闭连接
	if mq != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := mq.Close(closeCtx); err != nil {
			log.Printf("rabbitmq close: %v", err)
		}
		cancel()
	}

	log.Printf("shutdown complete")
}
func initPublisher(name string, ctx context.Context, f func(context.Context) (messaging.Publisher, error)) messaging.Publisher {
	pub, err := f(ctx)
	if err != nil {
		log.Printf("failed to create %s publisher: %v", name, err)
		return nil
	}
	return pub
}
