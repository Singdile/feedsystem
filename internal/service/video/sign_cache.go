package video

import (
	"container/list"
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	defaultURLCacheSize  = 10000
	defaultRefreshMargin = 15 * time.Minute
	minCacheableExpiry   = time.Second
	signTimeout          = 3 * time.Second
)

type signKey struct {
	ExpirySec int64
	ObjectKey string
}

type urlEntry struct {
	key      signKey
	url      string
	expireAt time.Time
}

// 使用LRU 缓存视频播放连接以及视频封面链接
type urlLRU struct {
	mu    sync.Mutex
	cap   int
	ll    *list.List //Front 最新，Back 最旧
	items map[signKey]*list.Element
}

func newURLLRU(capacity int) *urlLRU {
	if capacity <= 0 {
		capacity = defaultURLCacheSize
	}
	return &urlLRU{
		cap:   capacity,
		ll:    list.New(),
		items: make(map[signKey]*list.Element, capacity),
	}
}

func (c *urlLRU) Get(key signKey) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.items[key]
	if !ok {
		return "", false
	}

	entry := elem.Value.(*urlEntry)
	if !time.Now().Before(entry.expireAt) { //过期数据
		c.removeElement(elem)
		return "", false
	}

	// 使用之后移动到开头
	c.ll.MoveToFront(elem)
	return entry.url, true
}

func (c *urlLRU) Add(key signKey, url string, expireAt time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok { //已经存在，更新
		entry := elem.Value.(*urlEntry)
		entry.url = url
		entry.expireAt = expireAt
		c.ll.MoveToFront(elem)
		return
	}

	// 不存在，添加
	elem := c.ll.PushFront(&urlEntry{
		key:      key,
		url:      url,
		expireAt: expireAt,
	})

	c.items[key] = elem

	// 超量淘汰最旧的
	for c.ll.Len() > c.cap { //这里的len返回的是节点的个数
		if oldest := c.ll.Back(); oldest != nil {
			c.removeElement(oldest)
		}
	}

}

func (c *urlLRU) removeElement(elem *list.Element) {
	elemvalue := c.ll.Remove(elem)
	entry := elemvalue.(*urlEntry)
	delete(c.items, entry.key)
}

// 返回存的url的个数
func (c *urlLRU) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len()
}

// url 缓存装饰器
type CachingURLSigner struct {
	next   URLSigner //本地签名
	cache  *urlLRU
	margin time.Duration //预留的有效期余量，低于这个的不用缓存
	sf     singleflight.Group
	hits   atomic.Int64 //原子类型,并发安全
	misses atomic.Int64
}

func NewCachingURLSigner(next URLSigner, opts ...URLCacheOption) *CachingURLSigner {
	s := &CachingURLSigner{
		next:   next,
		cache:  newURLLRU(defaultURLCacheSize),
		margin: defaultRefreshMargin,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

type URLCacheOption func(*CachingURLSigner)

func WithURLCacheSize(n int) URLCacheOption {
	return func(s *CachingURLSigner) {
		s.cache = newURLLRU(n)
	}
}

func WithURLRefreshMargin(d time.Duration) URLCacheOption {
	return func(s *CachingURLSigner) {
		if d > 0 {
			s.margin = d
		}
	}
}

func (s *CachingURLSigner) PresignedGetObject(ctx context.Context, objectKey string, expiry time.Duration) (string, error) {
	// 检查空key 或 过短expiry，直接透传
	if objectKey == "" || expiry < minCacheableExpiry {
		return s.next.PresignedGetObject(ctx, objectKey, expiry)
	}

	// 构造cache 中的key，检查是否命中
	key := signKey{
		ExpirySec: int64(expiry / time.Second),
		ObjectKey: objectKey,
	}

	// cache hit
	if url, ok := s.cache.Get(key); ok {
		s.hits.Add(1)
		return url, nil
	}

	// miss
	// 使用singleflight 让同一个key，只有一个gorutine 去签名
	sfKey := strconv.FormatInt(key.ExpirySec, 10) + ":" + key.ObjectKey
	v, err, _ := s.sf.Do(sfKey, func() (any, error) {
		// 再次检查缓存
		if url, ok := s.cache.Get(key); ok {
			s.hits.Add(1)
			return url, nil
		}

		// 加一个ctx，避免第一个取消之后，所有的等待者失败
		signCtx, cancel := context.WithTimeout(context.Background(), signTimeout)
		defer cancel()

		s.misses.Add(1)

		url, err := s.next.PresignedGetObject(signCtx, objectKey, expiry)
		if err != nil {
			return "", err
		}

		if ttl := expiry - s.margin; ttl > 0 {
			s.cache.Add(key, url, time.Now().Add(ttl))
		}
		return url, nil
	})

	if err != nil {
		return "", err
	}
	return v.(string), nil
}

func (s *CachingURLSigner) Stats() (hits, misses int64) {
	return s.hits.Load(), s.misses.Load()
}
