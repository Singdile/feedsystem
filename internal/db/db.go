// Package db 负责连接数据库
package db

import (
	"feedsystem/internal/config"
	"fmt"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// NewDB 初始化数据库连接
func NewDB(config config.DBConfig) (*gorm.DB,error) {
    // 初始化
    dsn := fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?charset=utf8mb4&parseTime=True&loc=Local",config.User,config.Password,config.Host,config.Port,config.Dbname)
    db,err := gorm.Open(mysql.Open(dsn),&gorm.Config{})
    if err != nil {
	return nil,err
    }

    // ping建立真实连接，查看是否正常
    if 
    return db,nil
}

// AutoMigrate 根据定义，迁移创建表
func AutoMigrate(db *gorm.DB) error {
    return nil
}

func CloseDB(db *gorm.DB) error {

}
