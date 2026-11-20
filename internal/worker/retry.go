package worker

import (
	"context"
	"time"
)

// retryWithBackOff 执行fn，失败的时候指数退避重试，最多重试 retries 次
// 返回最后一次错误；期间成功返回 nil
func retryWithBackOff(retries int, fn func() error) error {
	var err error

	for i := range retries {
		if i > 0 {
			time.Sleep(time.Duration(1 << uint(i-1) * time.Second)) // 1s,2s,4s
		}
		if err = fn(); err == nil {
			return nil
		}
	}

	return err
}

// retryWithBackOffCtx 在 ctx 生命周期内最多尝试 attempts 次，
// 第 i 次失败后等待 baseDelay << (i-1)。
func retryWithBackOffCtx(ctx context.Context, attempts int, baseDelay time.Duration, fn func() error) error {
	var err error
	for i := 0; i < attempts; i++ {
		if i > 0 {
			delay := baseDelay << uint(i-1) // 100ms, 200ms, 400ms...
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err = fn(); err == nil {
			return nil
		}
	}
	return err
}
