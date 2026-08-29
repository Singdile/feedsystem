// main 提供api服务
package main

import (
	"feedsystem/internal/config"
	"feedsystem/internal/db"
	"feedsystem/internal/middleware/rabbitmq"
	"feedsystem/internal/middleware/redis"
	"log"
)


func main() {
    // 加载配置
    if err := config.Init(); err != nil {
	log.Panic(err)
    }

    conf := config.Conf

    // 连接数据库
    DB,err := db.NewDB(conf.DBConfig)
    if err != nil {
	log.Fatalf("failed to connect database,err: %v",err)
    }
    defer db.CloseDB(DB)

    // 连接redis
    rdb,err := redis.NewRedis(conf.RedisConfig)
    if err != nil {
	log.Fatalf("falied to connect redis,err:%v",err)
    }
    defer rdb.Close()

    // 连接rabbitmq
    _,err = rabbitmq.NewRabbitMQ(conf.RabbitMQConfig)
    if err != nil {
	log.Fatalf("falied to connect rabbitmq,err:%v",err)
    }

    // 设置路由
    

}
