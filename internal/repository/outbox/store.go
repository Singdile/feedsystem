package outbox

import (
	"context"
	"time"

	outboxmodel "feedsystem/internal/model/outbox"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type OutboxRepo struct {
	db *gorm.DB
}

func NewOutboxRepo(db *gorm.DB) *OutboxRepo {
	return &OutboxRepo{
		db: db,
	}
}

// Claim 认领一批待发布的记录
// 并发安全，使用skip locked
func (s *OutboxRepo) Claim(ctx context.Context, limit, maxAttempts int) ([]outboxmodel.Msg, error) {
	var msgs []outboxmodel.Msg

	now := time.Now()

	// 开启认领事务
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Where("status = ? AND (next_retry_at IS NULL OR next_retry_at <= ?) AND attempts < ?", outboxmodel.StatusPending, now, maxAttempts).Order("create_time ASC,id ASC").Limit(limit).Find(&msgs).Error
		if err != nil {
			return err
		}

		if len(msgs) == 0 {
			return nil
		}

		ids := make([]uint, 0, len(msgs))
		for _, m := range msgs {
			ids = append(ids, m.ID)
		}

		return tx.Model(&outboxmodel.Msg{}).Where("id IN (?)", ids).Updates(map[string]any{
			"status":     outboxmodel.StatusPublishing,
			"claimed_at": now,
			"attempts":   gorm.Expr("attempts + 1"),
			"last_error": nil,
		}).Error
	})

	if err != nil {
		return nil, err
	}

	for i := range msgs {
		msgs[i].Attempts++
	}
	return msgs, nil
}

// MarkDone 标记事件发送成功
func (s *OutboxRepo) MarkDone(ctx context.Context, id uint) error {
	return s.db.WithContext(ctx).Model(&outboxmodel.Msg{}).
		Where("id = ? AND status = ?", id, outboxmodel.StatusPublishing).
		Updates(map[string]any{
			"status":        outboxmodel.StatusDone,
			"claimed_at":    nil,
			"next_retry_at": nil,
		}).Error
}

// MarkFailed 发布失败后的状态标记
// 有重试机会 retryAfter > 0 => 状态变为 pending,等待下一次处理
// 没有重试机会，标记为Failed
func (s *OutboxRepo) MarkFailed(ctx context.Context, id uint, retryAfter time.Duration, cause error) error {
	lastErr := ""
	if cause != nil {
		lastErr = truncate(cause.Error(), 512)
	}

	updates := map[string]any{
		"claimed_at": nil,
		"last_error": lastErr,
	}

	if retryAfter > 0 {
		updates["status"] = outboxmodel.StatusPending
		updates["next_retry_at"] = time.Now().Add(retryAfter)
	} else {
		updates["status"] = outboxmodel.StatusFailed
		updates["next_retry_at"] = nil
	}

	return s.db.WithContext(ctx).Model(&outboxmodel.Msg{}).
		Where("id = ? AND status = ?", id, outboxmodel.StatusPublishing).
		Updates(updates).Error
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

// Reclaim 清理长期处以 Publishing 的任务
func (s *OutboxRepo) Reclaim(ctx context.Context, olderThan time.Duration, maxAttempts int) (int64, error) {
	cutoff := time.Now().Add(-olderThan)

	res := s.db.WithContext(ctx).Model(&outboxmodel.Msg{}).
		Where("status = ? AND claimed_at < ?", outboxmodel.StatusPublishing, cutoff).
		Updates(map[string]any{
			"status": gorm.Expr("IF(attempts >= ?, ?, ?)",
				maxAttempts, outboxmodel.StatusFailed, outboxmodel.StatusPending),
			"claimed_at":    nil,
			"next_retry_at": nil,
		})

	return res.RowsAffected, res.Error
}
