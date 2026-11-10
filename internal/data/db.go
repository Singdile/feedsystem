// Package data 负责连接数据库
package data

import (
	"context"
	"feedsystem/internal/config"
	"feedsystem/internal/model/account"
	"feedsystem/internal/model/feed"
	"feedsystem/internal/model/video"
	"fmt"
	"log"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// NewDB 初始化数据库连接
func NewDB(config config.DBConfig) (*gorm.DB, error) {
	// 初始化
	dsn := fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?charset=utf8mb4&parseTime=True&loc=Local&timeout=2s&readTimeout=3s&writeTimeout=3s", config.User, config.Password, config.Host, config.Port, config.Dbname)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		DisableAutomaticPing: true,
	})
	if err != nil {
		return nil, err
	}

	// ping建立真实连接，查看是否正常
	sqlDB, err := db.DB() //获取底层的连接池
	if err != nil {
		return nil, err
	}

	maxOpen := config.MaxOpenConns
	if maxOpen <= 0 {
		maxOpen = 40
	}
	maxIdle := config.MaxIdleConns
	if maxIdle <= 0 {
		maxIdle = 10
	}

	if maxIdle > maxOpen { //允许空闲的连接应该要比最大连接数要小
		maxIdle = maxOpen
	}

	lifetime := config.ConnMaxLifetime
	if lifetime <= 0 {
		lifetime = 30 * time.Minute
	}

	idleTime := config.ConnMaxIdleTime
	if idleTime <= 0 {
		idleTime = 5 * time.Minute
	}

	sqlDB.SetMaxOpenConns(maxOpen)
	sqlDB.SetMaxIdleConns(maxIdle)
	sqlDB.SetConnMaxLifetime(lifetime)
	sqlDB.SetConnMaxIdleTime(idleTime)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := sqlDB.PingContext(ctx); err != nil {
		return nil, err
	}

	log.Printf("db pool: maxOpen=%d maxIdle=%d lifetime=%s idleTime=%s",
		maxOpen, maxIdle, lifetime, idleTime)
	return db, nil
}

// AutoMigrate 根据定义，迁移创建表
func AutoMigrate(db *gorm.DB) error {
	return db.AutoMigrate(&account.User{}, &video.Video{}, &feed.OutboxMsg{}, &video.Tag{}, &video.VideoTag{}, &video.VideoRating{}, &video.Comment{}, &account.Social{}, &account.Notification{})
}

// CloseDB 关闭数据库连接
func CloseDB(db *gorm.DB) error {
	connPool, err := db.DB()
	if err != nil {
		return nil
	}

	if connPool != nil {
		return connPool.Close()
	}
	return nil
}
