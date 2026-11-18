package messaging

import (
	"context"
	"log"
	"time"
)

// RunConsumer 通用的消费循环，新建一个消费者链接上队列，循环进行消费，拿到信息,并对信息进行处理
func RunConsumer(ctx context.Context, c *Client, queueName string, h Handler) {
	if c == nil {
		log.Printf("messaging: consumer %s not started: nil client", queueName)
		return
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			src, err := c.openSource(ctx, queueName, 32) //连接
			if err != nil {
				log.Printf("messaging: consumer %s connect failed: %v", queueName, err)
				time.Sleep(time.Second)
				continue
			}

			// 循环接收信息，并交给handle函数处理信息
			for {
				msg, err := src.Receive(ctx)
				if err != nil {
					break
				}

				err = h(ctx, msg) // handle 处理mq信息
				if err != nil {
					log.Printf("messaging: handle %s failed: %v ", queueName, err)
				}
			}

			closeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_ = src.Close(closeCtx)
			cancel()
			time.Sleep(5 * time.Second)
		}
	}()
}
