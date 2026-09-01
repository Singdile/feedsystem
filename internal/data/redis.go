package data

import (
	"context"
	"feedsystem/internal/config"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	rdb *redis.Client
}

func NewRedis(config config.RedisConfig) (*RedisClient, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", config.Host, config.Port),
		Password: config.Password,
		DB:       config.DB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		return nil, err
	}

	return &RedisClient{
		rdb: rdb,
	}, nil
}

func (c *RedisClient) Close() error {
	if c == nil || c.rdb == nil {
		return nil
	}

	return c.rdb.Close()
}

func (c *RedisClient) Key(format string, a ...any) string {
	return fmt.Sprintf(format, a...)
}

func (c *RedisClient) Get(ctx context.Context, key string) (string, error) {
	return c.rdb.Get(ctx, key).Result()
}

func (c *RedisClient) Set(ctx context.Context, key string, val any, ttl time.Duration) (string, error) {
	return c.rdb.Set(ctx, key, val, ttl).Result()
}

func (c *RedisClient) Del(ctx context.Context, key string) error {
	_, err := c.rdb.Del(ctx, key).Result()
	return err
}
