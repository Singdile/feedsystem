// Package feed 提供视频流服务
package feed

import (
	"context"
	"encoding/json"
	"feedsystem/internal/model/feed"
	"feedsystem/internal/model/video"
	apperrors "feedsystem/internal/pkg/errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/patrickmn/go-cache"
	"golang.org/x/sync/singleflight"
)

const (
	timelineKey    = "feed:global_timeline"
	entityCacheTTL = time.Hour       // L2 实体缓存
	l1CacheTTL     = 5 * time.Second // L1 本地缓存
)

type FeedRepo interface {
	// 时间线操作,负责索引拿到显示vidoID
	ZRevRangeByScore(ctx context.Context, key string, maxScore float64, maxID uint, limit int) ([]feed.ZMember, error)
	ZRangeWithScores(ctx context.Context, key string, start, stop int64) ([]feed.ZMember, error)
	ZAdd(ctx context.Context, key string, member feed.ZMember) error

	// 冷数据查询
	ListLatest(ctx context.Context, cursor *video.Cursor, limit int) ([]video.Video, error)

	// 实体缓存 (L2 redis)
	Key(format string, a ...any) string
	MGet(ctx context.Context, keys ...string) ([]any, error)
	GetBytes(ctx context.Context, key string) ([]byte, error)
	SetBytes(ctx context.Context, key string, value []byte, ttl time.Duration) error
}

type VideoProvider interface {
	GetVideoEntitiesByIDs(ctx context.Context, ids []uint) ([]video.Video, error)
	BuildViews(ctx context.Context, vs []video.Video) ([]video.VideoView, error)
}

type FeedService struct {
	repo       FeedRepo
	videoSvc   VideoProvider
	localcache *cache.Cache       // go-cache:L1,默认5s过期
	group      singleflight.Group // 防并发
}

func NewFeedService(repo FeedRepo, videoSvc VideoProvider) *FeedService {
	return &FeedService{
		repo:       repo,
		videoSvc:   videoSvc,
		localcache: cache.New(l1CacheTTL, 10*time.Second),
	}
}

// FeedListResult feed 响应（Items 复用 video.VideoView）
type FeedListResult struct {
	Items      []video.VideoView `json:"items"`
	NextCursor string            `json:"next_cursor"` // 空 = 没有更多
}

// ListFeed 查询视频数据，返回可播放的视频信息
// 1.查找redis里面的时间线，获取前1000条热门视频ids
//
//	1.1 如果redis失效，直接查询数据库
//
// 2.查找ids对应的视频元数据
//
//	2.1  先查本地cache
//	2.2  未命中再查redis
//	2.3  未命中再查mysql
//
// 3.获取视频元数据，对其进行签发，获得播放地址并返回
func (s *FeedService) ListFeed(ctx context.Context, cursorStr string, limit int) (*FeedListResult, error) {
	// 解析游标
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	var curScore float64
	var curID uint
	var cursor *video.Cursor
	if cursorStr != "" {
		cur, err := video.DecodeCursor(cursorStr)
		if err != nil {
			return nil, apperrors.NewAppError(http.StatusBadRequest, "bad_cursor")
		}
		cursor = &cur
		curScore = float64(cur.CreatedAt.UnixMilli())
		curID = cur.ID
	}

	// 查看当前redis里面的最老的视频数据
	tail, err := s.repo.ZRangeWithScores(ctx, timelineKey, 0, 0)
	if err != nil {
		// redis fail -> DB
		return s.listLatestFromDB(ctx, cursor, limit)
	}

	// ZSET 中feed:global_time 为空，重建
	if len(tail) == 0 {
		return s.rebuildAndRetry(ctx, cursorStr, cursor, limit)
	}

	// 查询缓存中的timeline,获取时间排序的视频
	members, err := s.repo.ZRevRangeByScore(ctx, "feed:global_timeline", curScore, curID, limit+1)
	if err != nil {
		return s.listLatestFromDB(ctx, cursor, limit)
	}

	hasmore := len(members) > limit
	if hasmore {
		members = members[:limit]
	}

	ids := make([]uint, 0, len(members))
	for _, v := range members {
		videID, err := strconv.ParseUint(v.Member, 10, 64)
		if err != nil {
			log.Printf("failed to parse video id %s from feed_global_timeline", v.Member)
			continue
		}
		ids = append(ids, uint(videID))
	}

	// nextcursor
	next := ""
	if hasmore {
		last := members[len(members)-1]
		id, _ := strconv.ParseUint(last.Member, 10, 64)
		next = video.EncodeCursor(video.Cursor{CreatedAt: time.UnixMilli(int64(last.Score)), ID: uint(id)})
	}

	//查找三级缓存，获取视频实体信息
	entities, err := s.GetVideoByIDs(ctx, ids)
	if err != nil {
		return nil, apperrors.NewAppError(http.StatusInternalServerError, "get video failed")
	}

	// sign url
	videoViews, err := s.videoSvc.BuildViews(ctx, entities)
	if err != nil {
		return nil, apperrors.NewAppError(http.StatusInternalServerError, "get videos failed")
	}

	if len(videoViews) == 0 {
		return &FeedListResult{Items: []video.VideoView{}}, nil
	}

	return &FeedListResult{Items: videoViews, NextCursor: next}, nil
}

// GetVideoByIDs 查找视频实体数据
// 查找三级缓存：
// L1 : 本地go-cache （ttl 5s）
// L2 : redis缓存 （ttl 1h）
// L3 : mysql (采用singleflight,压缩操作次数)
func (s *FeedService) GetVideoByIDs(ctx context.Context, ids []uint) ([]video.Video, error) {
	if len(ids) == 0 {
		return []video.Video{}, nil
	}

	videoMap := make(map[uint]video.Video)

	// L1: cache
	missedL1 := make([]uint, 0, len(ids))
	for _, id := range ids {
		key := s.repo.Key("video:entity:%d", id)
		if v, ok := s.localcache.Get(key); !ok {
			missedL1 = append(missedL1, id)
		} else {
			videoMap[id] = v.(video.Video)
		}
	}

	// L2: redis
	keys := make([]string, 0, len(missedL1))
	missedL2 := make([]uint, 0, len(missedL1))
	for _, v := range missedL1 {
		keys = append(keys, s.repo.Key("video:entity:%d", v))
	}
	res, err := s.repo.MGet(ctx, keys...)
	if err != nil {
		missedL2 = missedL1
	} else {
		for i, v := range res {
			if v == nil { // 未命中，交给L3
				missedL2 = append(missedL2, missedL1[i])
				continue
			}

			str, ok := v.(string)
			if !ok { // 类型断言失败，交给L3
				missedL2 = append(missedL2, missedL1[i])
				continue
			}
			var entity video.Video
			if err := json.Unmarshal([]byte(str), &entity); err != nil {
				missedL2 = append(missedL2, missedL1[i]) // 反序列化失败，交给L3
				continue
			}

			videoMap[entity.ID] = entity // L2 命中，放进结果 map，并回填至 L1
			s.localcache.Set(keys[i], entity, l1CacheTTL)
		}
	}

	// L3: 数据库查询
	for _, id := range missedL2 {
		sfKey := fmt.Sprintf("video:entity:%d", id)
		val, err, _ := s.group.Do(sfKey, func() (any, error) {
			vs, err := s.videoSvc.GetVideoEntitiesByIDs(ctx, []uint{id})
			if err != nil {
				return nil, err
			}

			if len(vs) == 0 { // id not exists
				return nil, nil
			}
			return vs[0], nil
		})

		if err != nil {
			log.Println("cannot get video entity,err:", err)
			continue
		}

		if val == nil {
			continue
		}
		entity := val.(video.Video)
		videoMap[entity.ID] = entity

		key := s.repo.Key("video:entity:%d", entity.ID)
		if b, err := json.Marshal(entity); err == nil {
			_ = s.repo.SetBytes(ctx, key, b, entityCacheTTL) // 回填L2
		}
		s.localcache.Set(key, entity, l1CacheTTL) // 回填L1
	}

	result := make([]video.Video, 0, len(ids))
	for _, v := range ids {
		if item, ok := videoMap[v]; ok {
			result = append(result, item)
		}
	}
	return result, nil
}

// listLatestFromDB DB兜底，直接从数据库中获取最新的视频信息并签发
func (s *FeedService) listLatestFromDB(ctx context.Context, cursor *video.Cursor, limit int) (*FeedListResult, error) {
	// DB中获取数据
	vs, err := s.repo.ListLatest(ctx, cursor, limit+1)
	if err != nil {
		return nil, err
	}
	if len(vs) == 0 {
		return &FeedListResult{Items: []video.VideoView{}}, nil
	}

	// 签发获取播放地址
	hasmore := len(vs) > limit
	if len(vs) > limit {
		vs = vs[:limit]
	}
	views, err := s.videoSvc.BuildViews(ctx, vs)
	if err != nil {
		return nil, err
	}

	// 回填 L2;L1
	for _, v := range vs {
		key := s.repo.Key("video:entity:%d", v.ID)
		if b, err := json.Marshal(v); err == nil {
			_ = s.repo.SetBytes(ctx, key, b, entityCacheTTL)
		}
		s.localcache.Set(key, v, l1CacheTTL)
	}

	// 清理feed:global_time

	// nextcursor
	nextCursor := ""
	if hasmore {
		last := vs[len(vs)-1]
		nextCursor = video.EncodeCursor(video.Cursor{CreatedAt: last.CreatedAt, ID: last.ID})
	}

	// 返回可播放信息
	return &FeedListResult{
		Items:      views,
		NextCursor: nextCursor,
	}, nil
}

// rebuilidAndRetry ZSET为空的时候重建，从DB中拉取最近的1000条回填 Redis，然后重新走 ListFeed
func (s *FeedService) rebuildAndRetry(ctx context.Context, cursorStr string, cursor *video.Cursor, limit int) (*FeedListResult, error) {
	res, err, _ := s.group.Do("sf:rebuild:global_time", func() (any, error) {
		videos, err := s.repo.ListLatest(ctx, nil, 1000)
		if err != nil {
			return nil, err
		}
		if len(videos) == 0 {
			return "EMPTY_DB", nil
		}

		// 逐条回填ZSET
		for _, v := range videos {
			member := feed.ZMember{
				Score:  float64(v.CreatedAt.UnixMilli()),
				Member: strconv.FormatUint(uint64(v.ID), 10),
			}

			if err := s.repo.ZAdd(ctx, timelineKey, member); err != nil {
				return nil, err
			}
		}
		return "REBUILT", nil
	})

	if err != nil {
		return s.listLatestFromDB(ctx, cursor, limit)
	}

	if res.(string) == "EMPTY_DB" {
		return &FeedListResult{Items: []video.VideoView{}}, nil
	}

	return s.ListFeed(ctx, cursorStr, limit)
}
