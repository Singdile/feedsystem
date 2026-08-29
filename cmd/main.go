// main 提供api服务
package main

import (
	"feedsystem/internal/config"
	"feedsystem/internal/db"
	"log"
)


func main() {
    // 加载配置
    if err := config.Init(); err != nil {
	log.Panic(err)
    }

    // 连接数据库
    if _,err := db.NewDB(config.Conf.DBConfig); err != nil {
	log.Fatalf("failed to connect database,err: %v",err)
    }
    
    // 连接redis

    // 连接rabbitmq

    // 设置路由

}
