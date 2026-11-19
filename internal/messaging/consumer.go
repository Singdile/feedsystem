package messaging

import (
	"context"
	"errors"
	"log"
	"runtime/debug"
	"sync"
	"time"
)

var ErrDrainTimeout = errors.New("messaging: drain timeout")

type Consumer interface {
	Consume(ctx context.Context, cfg ConsumerConfig, h Handler) error
}

type ConsumerConfig struct {
	Queue            string        //消费队列
	Concurrency      int           //同时处理队列的几条消息
	Prefetch         int32         //最多同一时间接收多少条信息
	Timeout          time.Duration // 单条信息最多处理多久
	DrainTimeout     time.Duration // 退出时最多等待多久
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

	jobs := make(chan Message, cfg.Prefetch)
	var wg sync.WaitGroup

	// 启动多个 goroutine 处理channel中的信息
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Go(
			func() {
				for msg := range jobs {
					processOne(ctx, cfg, h, msg)
				}
			},
		)
	}

	// 循环从消息队列中取出数据到对channel
	c.reconnectLoop(ctx, cfg, jobs)

	// 关闭channel，释放对应的处理 goroutine
	close(jobs)

	// 等待处理剩余的数据
	done := make(chan struct{})

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(cfg.DrainTimeout):
		log.Printf("messaging: %s drain timeout after %s, %d messages still queued",
			cfg.Queue, cfg.DrainTimeout, len(jobs))
		return ErrDrainTimeout
	}
}

// reconnectLoop 从消息队列中取出数据到对应channel中
func (c *runtimeConsumer) reconnectLoop(ctx context.Context, cfg ConsumerConfig, jobs chan<- Message) {
	for {
		if ctx.Err() != nil {
			return
		}

		src, err := c.factory.OpenSource(ctx, cfg.Queue, cfg.Prefetch)
		if err != nil {
			log.Printf("messaging: connect %s failed: %v", cfg.Queue, err)
			if !sleepCtx(ctx, cfg.ConnectBackoff) {
				return
			}
			continue
		}

		receiveLoop(ctx, src, jobs)

		closeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = src.Close(closeCtx)
		cancel()

		if ctx.Err() != nil {
			return
		}
		if !sleepCtx(ctx, cfg.ReconnectBackoff) {
			return
		}
	}
}

// RunConsumer 通用的消费循环，新建一个消费者链接上队列，循环进行消费，拿到信息,并对信息进行处理
//
// Deprecated: 使用 NewConsumer + Consumer.Consume。
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

func receiveLoop(ctx context.Context, src MessageSource, jobs chan<- Message) {
	for {
		msg, err := src.Receive(ctx)
		if err != nil {
			return
		}
		select {
		case jobs <- msg:
		case <-ctx.Done():
			return
		}
	}
}

func processOne(ctx context.Context, cfg ConsumerConfig, h Handler, msg Message) {
	base := context.WithoutCancel(ctx)
	msgCtx, cancel := context.WithTimeout(base, cfg.Timeout)
	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			log.Printf("messaging: panic queue = %s err= %v \n %s", cfg.Queue, r, debug.Stack())
			_ = msg.Accept(ctx)
		}
	}()

	if err := h(msgCtx, msg); err != nil {
		log.Printf("messaging: handle %s failed: %v", cfg.Queue, err)
	}
}
