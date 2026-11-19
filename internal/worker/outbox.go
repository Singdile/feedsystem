package worker

import (
	"context"
	"feedsystem/internal/messaging"
	"feedsystem/internal/model/feed"
	outboxmodel "feedsystem/internal/model/outbox"
	"fmt"
	"log"
	"math/rand"
	"time"
)

const (
	outboxBackoffBase = time.Second
	outboxBackoffMax  = 5 * time.Minute
)

type OutboxStore interface {
	// 认领一批任务
	Claim(ctx context.Context, limit, maxAttempts int) ([]outboxmodel.Msg, error)

	// 标记完成
	MarkDone(ctx context.Context, id uint) error

	// 尝试标记失败
	MarkFailed(ctx context.Context, id uint, retryAfter time.Duration, cause error) error

	// 清除一些长时间未处理完成的任务
	Reclaim(ctx context.Context, olderThan time.Duration, maxAttempts int) (int64, error)
}

type OutboxConfig struct {
	BatchSize      int           //每次处理规模
	PublishTimeout time.Duration //发布最长容忍时间
	PollInterval   time.Duration // 批处理间隔
	ReclaimAfter   time.Duration // 认领超时阈值：publishing 超过该时长视为持有者已死亡
	MaxAttempts    int           // 最大重试发布次数
}

type OutboxPoller struct {
	store OutboxStore
	pub   messaging.Publisher
	cfg   OutboxConfig
}

func NewOutboxPoller(store OutboxStore, pub messaging.Publisher, cfg OutboxConfig) *OutboxPoller {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.PublishTimeout <= 0 {
		cfg.PublishTimeout = 5 * time.Second
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.ReclaimAfter <= 0 {
		cfg.ReclaimAfter = 5 * time.Minute
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 10
	}

	return &OutboxPoller{store: store, pub: pub, cfg: cfg}
}

// Run 阻塞运行，直到ctx取消
func (p *OutboxPoller) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		// 检查 publisher 是否可用
		if p.pub == nil {
			log.Printf("outbox: publisher not ready,skip claiming")
			if !sleepCtx(ctx, p.cfg.PollInterval) {
				return
			}
			continue
		}

		// 清除长时间未完成的任务：
		// claimed_at 超过 ReclaimAfter 仍处于 publishing 的记录，视为持有者已死亡
		if n, err := p.store.Reclaim(ctx, p.cfg.ReclaimAfter, p.cfg.MaxAttempts); err != nil {
			log.Printf("outbox: reclaim failed: %v", err)
		} else if n > 0 {
			log.Printf("outbox: reclaimed %d stale records", n)
		}

		// 认领一些msgs
		msgs, err := p.store.Claim(ctx, p.cfg.BatchSize, p.cfg.MaxAttempts)
		if err != nil {
			log.Printf("outbox: claim failed: %v", err)
			if !sleepCtx(ctx, p.cfg.PollInterval) {
				return
			}
			continue
		}

		if len(msgs) == 0 {
			if !sleepCtx(ctx, p.cfg.PollInterval) {
				return
			}
			continue
		}

		// 处理
		p.publishBatch(ctx, msgs)
	}
}

// sleepCtx 休眠重试
// 如果ctx取消，返回false
func sleepCtx(ctx context.Context, sleep time.Duration) bool {
	t := time.NewTimer(sleep)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (p *OutboxPoller) publishBatch(ctx context.Context, msgs []outboxmodel.Msg) {
	okCount, failedCount := 0, 0

	for _, msg := range msgs {
		// 发布
		if err := p.publishOne(ctx, msg); err != nil {
			failedCount++

			var retryAfter time.Duration

			// 计算可重试时间间隔
			if msg.Attempts < p.cfg.MaxAttempts {
				retryAfter = outboxBackoff(msg.Attempts)
			}

			if err := p.store.MarkFailed(ctx, msg.ID, retryAfter, err); err != nil {
				log.Printf("outbox: mark failed id = %d: %v", msg.ID, err)
			}

			continue
		}

		// 发布成功
		if err := p.store.MarkDone(ctx, msg.ID); err != nil {
			log.Printf("outbox: mark done id = %d err: %v", msg.ID, err)
			continue
		}
		okCount++
	}

	log.Printf("outbox: batch=%d ok=%d failed=%d", len(msgs), okCount, failedCount)
}

// 发送事件到MQ
func (p *OutboxPoller) publishOne(ctx context.Context, msg outboxmodel.Msg) error {
	if msg.EventType != outboxmodel.EventTypePublish {
		return fmt.Errorf("outbox: unsupported event type %q", msg.EventType)
	}

	event := feed.TimeLineEvent{
		EventID:    fmt.Sprintf("outbox:%d", msg.ID), // 稳定 ID，重试不变
		VideoID:    msg.VideoID,
		AuthorID:   msg.AuthorID,
		CreateTime: msg.CreateTime.UnixMilli(),
		OccurredAt: msg.CreateTime, // 用行时间，重试时 payload 不漂移
	}

	envelope, err := messaging.NewEnvelopeAt(
		event.EventID,
		messaging.EventTypeTimeline,
		messaging.CurrentEventVersion,
		event.OccurredAt,
		event,
	)
	if err != nil {
		return err
	}

	pubCtx, cancel := context.WithTimeout(ctx, p.cfg.PublishTimeout)
	defer cancel()

	return p.pub.Publish(pubCtx, envelope)
}

// outboxBackoff 指数退避：1s, 2s, 4s ... 封顶 5min，附加 0~20% 抖动
func outboxBackoff(attempts int) time.Duration {
	d := outboxBackoffBase
	for i := 1; i < attempts && d < outboxBackoffMax; i++ {
		d *= 2
	}
	if d > outboxBackoffMax {
		d = outboxBackoffMax
	}
	return d + time.Duration(rand.Int63n(int64(d/5)+1))
}
