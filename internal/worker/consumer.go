package worker

import (
	"context"
	"feedsystem/internal/data"
	"log"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

type MQHandler func(ctx context.Context, deliveryContext rmq.IDeliveryContext) error

// RunConsumer 通用的消费循环，新建一个消费者链接上队列，循环进行消费，拿到信息,并对信息进行处理
func RunConsumer(ctx context.Context, rmq *data.RabbitMQClient, queueName string, handle MQHandler) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			consumer, err := rmq.NewConsumer(ctx, queueName) //连接
			if err != nil {
				log.Println("consumer error: ", err)
				time.Sleep(time.Second)
				continue
			}

			// 循环接收信息，并交给handle函数处理信息
			for {
				dc, err := consumer.Receive(ctx)
				if err != nil {
					break
				}

				err = handle(ctx, dc) // handle 处理mq信息
				if err != nil {
					log.Println("handle MQ's msg error: ", err)
				}
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
			_ = consumer.Close(ctx)
			cancel()
			time.Sleep(5 * time.Second)
		}
	}()
}

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
