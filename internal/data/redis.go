package data

import (
	"context"
	"feedsystem/internal/config"
	"feedsystem/internal/model/feed"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	getAccessByID  = "account:%d"
	getRefreshByID = "account:%d:refresh"
	getIDByRefresh = "refresh:%s"
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

func (c *RedisClient) GetAccessByID(ctx context.Context, id uint) (string, error) {
	return c.rdb.Get(ctx, fmt.Sprintf(getAccessByID, id)).Result()
}

func (c *RedisClient) GetRefreshByID(ctx context.Context, id uint) (string, error) {
	return c.rdb.Get(ctx, fmt.Sprintf(getRefreshByID, id)).Result()
}

func (c *RedisClient) GetIDByRefresh(ctx context.Context, refreshtoken string) (string, error) {
	return c.rdb.Get(ctx, fmt.Sprintf(getIDByRefresh, refreshtoken)).Result()
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

// ZRevRangeByScore 返回 zset 中 score <= maxScore 的元素，按 score 从大到小排序，返回 limit 个
func (c *RedisClient) ZRevRangeByScore(ctx context.Context, key string, maxScore float64, maxID uint, limit int) ([]feed.ZMember, error) {
	max := "+inf"
	if maxScore > 0 {
		max = strconv.FormatFloat(maxScore, 'f', -1, 64)
	}

	res, err := c.rdb.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
		Key:     key,
		Start:   max,
		Stop:    "-inf",
		ByScore: true, // 按score排序
		ByLex:   false,
		Rev:     true, //从大到小遍历
		Offset:  0,
		Count:   int64(limit * 2), //冗余拿取
	}).Result()

	if err != nil {
		return nil, err
	}

	// 二次过滤
	out := make([]feed.ZMember, 0, len(res))

	for _, m := range res {
		score := m.Score
		member, ok := m.Member.(string)
		if !ok {
			member = fmt.Sprintf("%v", m.Member)
		}

		// 再次筛选，去除比游标位置更新的(score更大的，videoID更大的)
		id, _ := strconv.ParseUint(member, 10, 64)
		if maxScore > 0 && (score > maxScore || (maxID > 0 && uint(id) >= maxID)) {
			continue
		}
		out = append(out, feed.ZMember{
			Score:  score,
			Member: member,
		})

		if len(out) >= limit {
			break
		}
	}

	return out, nil
}

// ZAdd 向 zset 写入一个 (score, member)
func (c *RedisClient) ZAdd(ctx context.Context, key string, member feed.ZMember) error {
	_, err := c.rdb.ZAdd(ctx, key, redis.Z{
		Score:  member.Score,
		Member: member.Member,
	}).Result()
	return err
}

// ZRangeWithScores 返回排名区间 [start, stop] 的元素及分数
func (c *RedisClient) ZRangeWithScores(ctx context.Context, key string, start, stop int64) ([]feed.ZMember, error) {
	// 使用 ZRangeArgsWithScores 获取指定范围的元素及分数
	res, err := c.rdb.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
		Key:     key,
		Start:   strconv.FormatInt(start, 10),
		Stop:    strconv.FormatInt(stop, 10),
		ByScore: false,
		ByLex:   false,
		Rev:     false,
	}).Result()
	if err != nil {
		return nil, err
	}

	// 将结果转换为 []feed.ZMember
	out := make([]feed.ZMember, 0, len(res))
	for _, m := range res {
		score := m.Score
		member, ok := m.Member.(string)
		if !ok {
			member = fmt.Sprintf("%v", m.Member)
		}
		out = append(out, feed.ZMember{
			Score:  score,
			Member: member,
		})
	}

	return out, nil
}

// ZRemRangeByRank 按排名区间删除元素，用于时间线裁剪（保留最近 N 条）
func (c *RedisClient) ZRemRangeByRank(ctx context.Context, key string, start, stop int64) error {
	return c.rdb.ZRemRangeByRank(ctx, key, start, stop).Err()
}

// ZRem 从ZSet中删除指定member
func (c *RedisClient) ZRem(ctx context.Context, key string, member string) error {
	return c.rdb.ZRem(ctx, key, member).Err()
}

// MGet 批量读取多个key的元素
func (c *RedisClient) MGet(ctx context.Context, keys ...string) ([]any, error) {
	return c.rdb.MGet(ctx, keys...).Result()
}

// GetBytes 读取 key 的字节内容
func (c *RedisClient) GetBytes(ctx context.Context, key string) ([]byte, error) {
	return c.rdb.Get(ctx, key).Bytes()
}

// SetBytes 写入字节内容（带TTL）
func (c *RedisClient) SetBytes(ctx context.Context, key string, bytes []byte, ttl time.Duration) error {
	return c.rdb.Set(ctx, key, bytes, ttl).Err()
}
