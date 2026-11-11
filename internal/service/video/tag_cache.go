// 把 tags 表（id → name）缓存在进程内，让 TagIDs 转标签名变成 O(1) 内存查找，避免每请求查 DB。
package video

import (
	"context"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

type TagNameReader interface {
	ListTagNames(ctx context.Context) (map[uint]string, error)
}

type TagNameCache struct {
	reader TagNameReader //DB 回源
	ttl    time.Duration //整表刷新时间

	mu       sync.RWMutex
	names    map[uint]string // tagID -> name
	expireAt time.Time
	sf       singleflight.Group
}

func NewTagNameCache(reader TagNameReader, ttl time.Duration) *TagNameCache {
	return &TagNameCache{
		reader: reader,
		ttl:    ttl,
		names:  make(map[uint]string),
	}
}

// id -> name
func (c *TagNameCache) GetNames(ctx context.Context, ids []uint) (map[uint]string, error) {
	if len(ids) == 0 {
		return map[uint]string{}, nil
	}

	// 确保快表有效
	if err := c.ensureLoaded(ctx); err != nil {
		return nil, err
	}

	// 只查询本地的，缺失的忽略
	out, _ := c.lookup(ids)
	return out, nil
}

func (c *TagNameCache) lookup(ids []uint) (map[uint]string, []uint) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	out := make(map[uint]string, len(ids))
	missing := make([]uint, 0, len(ids))

	for _, id := range ids {
		if name, ok := c.names[id]; ok {
			out[id] = name
		} else {
			missing = append(missing, id)
		}
	}
	return out, missing
}

func (c *TagNameCache) ensureLoaded(ctx context.Context) error {
	c.mu.RLock()
	fresh := !c.expireAt.IsZero() && time.Now().Before(c.expireAt)
	c.mu.RUnlock()

	if fresh { //检查是否到期
		return nil
	}

	// 到期更新
	_, err, _ := c.sf.Do("refreshTagNameMap", func() (any, error) {
		// 再次检查是否已经刷新
		c.mu.RLock()
		fresh := !c.expireAt.IsZero() && time.Now().Before(c.expireAt)
		c.mu.RUnlock()

		if fresh {
			return nil, nil
		}

		opCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		all, err := c.reader.ListTagNames(opCtx)
		if err != nil {
			return nil, err
		}

		c.mu.Lock()
		c.names = all
		c.expireAt = time.Now().Add(c.ttl)
		c.mu.Unlock()

		return nil, nil
	})

	return err
}
