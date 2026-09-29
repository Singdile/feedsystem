// Package main 用于处理redis中zset里面的脏数据
package main

import (
	"context"
	"feedsystem/internal/config"
	"feedsystem/internal/data"
	"feedsystem/internal/model/video"
	"log"
	"strconv"
	"time"
)

func main() {
	err := config.Init()
	if err != nil {
		log.Fatalf("failed to load config, err: %v", err)
	}
	// 连接redis 和 db
	cache, err := data.NewRedis(config.Conf.RedisConfig)
	if err != nil {
		log.Fatalf("failed to connect redis,err: %v", err)
	}

	defer cache.Close()

	db, err := data.NewDB(config.Conf.DBConfig)
	if err != nil {
		log.Fatalf("failed to connect db,err: %v", err)
	}
	defer data.CloseDB(db)

	// 读取zset 全部的member
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	members, err := cache.ZRangeWithScores(ctx, "feed:global_timeline", 0, -1) // 读取全部的zset数据
	if err != nil {
		log.Fatalf("failed to get zset data,err : %v", err)
	}
	ids := []uint{}
	for _, v := range members {
		id, err := strconv.ParseUint(v.Member, 10, 64)
		if err != nil {
			continue
		}
		ids = append(ids, uint(id))
	}

	// 查询 DB 中那些视频还存在
	var exists []uint
	err = db.Model(&video.Video{}).Where("id in (?)", ids).Pluck("id", &exists).Error
	if err != nil {
		log.Fatalf("failed to check videos,err: %v", err)
	}

	// 筛选出不存在的视频ID，并移除
	check := map[uint]bool{}
	for _, v := range exists {
		check[v] = true
	}

	count := 0
	delCtx, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel2()
	for _, v := range ids {
		if !check[v] {
			cache.ZRem(delCtx, "feed:global_timeline", strconv.FormatUint(uint64(v), 10))
			count++
		}
	}

	log.Printf("清理完成，总 member = %d, 存活 = %d , 残留已清除 = %d", len(members), len(exists), count)
}
