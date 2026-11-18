package messaging

import (
	"context"
	"log"
	"time"
)

type Consumer interface {
	Consume(ctx context.Context, cfg ConsumerConfig, h Handler) error
}

type ConsumerConfig struct {
	Queue            string        //消费队列
	Concurrency      int           //同时处理队列的几条消息
	Prefetch         int32         //最多同一时间接收多少条信息
	Timeout          time.Duration // 单条信息最多处理多久
	DrainTimeout     time.Duration // 推出时最多等待多久
	ConnectBackoff   time.Duration //首次连接失败后等待多久
	ReconnectBackoff time.Duration //链路断开后等多久重建
}

func (cfg ConsumerConfig) withDefaults() ConsumerConfig {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	if cfg.Prefetch <= 0 {
		cfg.Prefetch = int32(cfg.Concurrency * 4)
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 10 * time.Second
	}
	if cfg.ConnectBackoff <= 0 {
		cfg.ConnectBackoff = time.Second
	}
	if cfg.ReconnectBackoff <= 0 {
		cfg.ReconnectBackoff = 5 * time.Second
	}
	return cfg
}

func NewConsumer(factory SourceFactory) Consumer {
	return &runtimeConsumer{factory: factory}
}

type runtimeConsumer struct {
	factory SourceFactory
}

func (c *runtimeConsumer) Consume(ctx context.Context, cfg ConsumerConfig, h Handler) error {
	cfg = cfg.withDefaults()
	for {
		if ctx.Err() != nil { // 检查是否关闭
			return nil
		}

		// 打开信息来源
		src, err := c.factory.OpenSource(ctx, cfg.Queue, cfg.Prefetch)
		if err != nil {
			log.Printf("messaging: connect %s failed: %v", cfg.Queue, err)
			if !sleepCtx(ctx, cfg.ConnectBackoff) {
				return nil
			}
			continue
		}

		// receive loop
		receiveLoop(ctx, cfg, src, h)

		// 关闭信息链路
		closeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = src.Close(closeCtx)
		cancel()

		// 总ctx 关闭，返回
		if ctx.Err() != nil {
			return nil
		}

		// 尝试重连
		log.Printf("messaging: %s link broken, reconnecting", cfg.Queue)

		if !sleepCtx(ctx, cfg.ReconnectBackoff) {
			return nil
		}
	}
}

// RunConsumer 通用的消费循环，新建一个消费者链接上队列，循环进行消费，拿到信息,并对信息进行处理
func RunConsumer(ctx context.Context, c *Client, queueName string, h Handler) {
	if c == nil {
		log.Printf("messaging: consumer %s not started: nil client", queueName)
		return
	}
	consumer := NewConsumer(c)

	go func() {
		if err := consumer.Consume(ctx, ConsumerConfig{Queue: queueName}, h); err != nil {
			log.Printf("messaging: consumer %s stopped: %v", queueName, err)
		}
	}()
}

// 停止等待一段时间
// 如果ctx没有被关闭，返回true；如果被关闭了，返回false
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d) //到了时间，t会向channel发送一个当前的时间
	defer t.Stop()        //函数退出时停止该计时器

	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func receiveLoop(ctx context.Context, cfg ConsumerConfig, src MessageSource, h Handler) {
	for {
		msg, err := src.Receive(ctx)
		if err != nil {
			return
		}
		if err := h(ctx, msg); err != nil {
			log.Printf("messaging: handle %s failed: %v", cfg.Queue, err)
		}
	}
}
