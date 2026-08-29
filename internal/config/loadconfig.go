// Package config 用于从配置文件中加载配置
package config

import (
	"fmt"
	"log"

	"github.com/spf13/viper"
)

// Config 总配置
type Config struct {
	AppConfig      AppConfig      `mapstructure:"app"`
	DBConfig       DBConfig       `mapstructure:"db"`
	RedisConfig    RedisConfig    `mapstructure:"redis"`
	RabbitMQConfig RabbitMQConfig `mapstructure:"rabbitmq"`
}

// AppConfig 应用自身的配置
type AppConfig struct {
	Appname string `mapstructure:"appname"`
	Version string `mapstructure:"version"`
	Port    int    `mapstructure:"port"`
}

// DBConfig 数据库配置
type DBConfig struct {
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	Dbname   string `mapstructure:"dbname"`
}

// RedisConfig 缓存配置
type RedisConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

// RabbitMQConfig 消息队列配置
type RabbitMQConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

// Conf 全局配置变量
var Conf *Config

// Init 默认读取默认配置文件，可以通过启动传递参数指定配置文件
func Init() error {
	// viper 基础配置
	viper.SetConfigName("config")    // 配置文件名，不带后缀. 不是模糊查找
	viper.SetConfigType("yaml")      // 文件类型 yaml, 即 config.yaml
	viper.AddConfigPath("./configs") // 查找 configs 文件夹

	// 读取配置文件
	if err := viper.ReadInConfig(); err != nil {
		return fmt.Errorf("load config failed,err:%v", err)
	}

	// 反序列化到Conf
	Conf = new(Config) //必须分配空间，否则是空指针
	if err := viper.Unmarshal(Conf); err != nil {
		return fmt.Errorf("unmarshal config failed,err: %v", err)
	}

	log.Printf("[config] load successfully,%#v",Conf)
	return nil
}


