// main 提供api服务
package main

import (
	"fmt"
	"log"

	"feedsystem/internal/config"
	"feedsystem/internal/data"
	"feedsystem/internal/http"
)

func main() {
	// 加载配置
	if err := config.Init(); err != nil {
		log.Panic(err)
	}

	conf := config.Conf

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
	if _, err = data.NewRabbitMQ(conf.RabbitMQConfig); err != nil {
		log.Printf("rabbitmq connect failed (mq disabled): %v", err)
	} else {
		log.Printf("RabbitMQ connected")
	}

	// 装配路由并启动 HTTP 服务
	router := http.SetRouter(DB, rdb)
	addr := fmt.Sprintf(":%d", conf.AppConfig.Port)
	log.Printf("Server is running on %s", addr)
	if err := router.Run(addr); err != nil {
		log.Fatalf("failed to run server: %v", err)
	}
}
