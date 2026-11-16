package worker

import (
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
