// reader.go 提供视频实体的统一的读取能力
// 只返回视频实体，不负责组装
package video

import (
	"context"
	"encoding/json"
	"errors"
	"feedsystem/internal/model/video"
	apperrors "feedsystem/internal/pkg/errors"
	"log"
	"net/http"
	"time"

	localcache "github.com/patrickmn/go-cache"
	"golang.org/x/sync/singleflight"
	"gorm.io/gorm"
)

const (
	l1CacheTTL     = 5 * time.Second // L1 本地缓存
	entityCacheTTL = time.Hour       //L2 redis 缓存
)

// VideoReader 实现视频实体读取、缓存查询、数据库回源
type VideoReader struct {
	repo       VideoReadRepo      //DB操作与对象存储操作
	cache      VideoEntityCache   //缓存操作
	localcache *localcache.Cache  // go-cache:L1,默认5s过期
	group      singleflight.Group // 防并发
}

type VideoReadRepo interface {
	FindByID(ctx context.Context, id uint) (*video.Video, error)
	GetVideosByIDs(ctx context.Context, ids []uint) ([]video.Video, error)
	GetTagIDsByVideoIDs(ctx context.Context, ids []uint) (map[uint][]uint, error)
}

type VideoEntityCache interface {
	Key(format string, args ...any) string
	GetBytes(ctx context.Context, key string) ([]byte, error)
	MGet(ctx context.Context, keys ...string) ([]any, error)
	SetBytes(
		ctx context.Context,
		key string,
		value []byte,
		ttl time.Duration,
	) error
}

func NewVideoReader(repo VideoReadRepo, cache VideoEntityCache) *VideoReader {
	return &VideoReader{
		repo:       repo,
		cache:      cache,
		localcache: localcache.New(l1CacheTTL, 10*time.Second),
	}
}
func (r *VideoReader) GetByID(ctx context.Context, id uint) (*video.Video, error) {
	if id <= 0 {
		return nil, apperrors.NewAppError(http.StatusBadRequest, "视频不存在")
	}

	key := r.cache.Key("video:entity:%d", id)
	// L1:localcache
	v, ok := r.localcache.Get(key)
	if ok {
		entity := v.(video.VideoEntityCache)
		video := entity.ToVideo()
		return &video, nil
	}

	// L2: redis
	if b, err := r.cache.GetBytes(ctx, key); err == nil {
		var entity video.VideoEntityCache
		if err := json.Unmarshal(b, &entity); err == nil {
			r.localcache.Set(key, entity, l1CacheTTL) // 回填L1
			video := entity.ToVideo()
			return &video, nil
		}
	}

	// L3: DB
	//使用singleflight，将同一时间对于同一视频的并发请求压缩为1条
	findV, err, _ := r.group.Do(key, func() (any, error) {
		// 再次查看cache 中有没有被回填
		if b, err := r.cache.GetBytes(ctx, key); err == nil {
			var entity video.VideoEntityCache
			if err := json.Unmarshal(b, &entity); err == nil {
				r.localcache.Set(key, entity, l1CacheTTL) // 回填L1
				video := entity.ToVideo()
				return &video, nil
			}
		}
		// 查询DB
		v, err := r.repo.FindByID(ctx, id)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil, apperrors.NewAppError(http.StatusNotFound, "视频不存在")
			}
			log.Printf("查询视频失败 video_id=%d err=%v", id, err)
			return nil, apperrors.NewAppError(http.StatusInternalServerError, "视频查询失败")
		}

		// tag
		tmp := []video.Video{*v}
		if err := r.FillTagIDs(ctx, tmp); err != nil {
			log.Printf("fill tag ids failed : %v", err)
		}
		v.TagIDs = tmp[0].TagIDs

		// 回填L2;L1
		entity := v.ToVideoEntityCache()
		if b, err := json.Marshal(entity); err == nil {
			r.cache.SetBytes(ctx, key, b, entityCacheTTL)
		}
		r.localcache.Set(key, entity, l1CacheTTL)
		return v, nil
	})

	if err != nil {
		return nil, err
	}

	ans, ok := findV.(*video.Video)
	if !ok || ans == nil {
		return nil, apperrors.NewAppError(http.StatusInternalServerError, "视频查询失败")
	}
	result := *ans //复制结构体，给每一个调用者一份独立结果
	return &result, nil
}

func (r *VideoReader) GetByIDs(ctx context.Context, ids []uint) ([]video.Video, error) {
	if len(ids) == 0 {
		return []video.Video{}, nil
	}

	videoMap := make(map[uint]video.Video)

	// L1: cache
	missedL1 := make([]uint, 0, len(ids))
	for _, id := range ids {
		key := r.cache.Key("video:entity:%d", id)
		if v, ok := r.localcache.Get(key); !ok {
			missedL1 = append(missedL1, id)
		} else {
			entity := v.(video.VideoEntityCache)
			videoMap[id] = entity.ToVideo()
		}
	}

	// L2: redis
	missedL2 := make([]uint, 0, len(missedL1))
	if len(missedL1) > 0 {
		keys := make([]string, 0, len(missedL1))
		for _, v := range missedL1 {
			keys = append(keys, r.cache.Key("video:entity:%d", v))
		}
		res, err := r.cache.MGet(ctx, keys...)
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
				var cached video.VideoEntityCache
				if err := json.Unmarshal([]byte(str), &cached); err != nil {
					missedL2 = append(missedL2, missedL1[i]) // 反序列化失败，交给L3
					continue
				}
				cacheVideo := cached.ToVideo()
				videoMap[cacheVideo.ID] = cacheVideo // L2 命中，放进结果 map，并回填至 L1
				r.localcache.Set(keys[i], cached, l1CacheTTL)
			}
		}

	}
	// L3: 数据库查询
	if len(missedL2) > 0 {
		videoBatch, err := r.repo.GetVideosByIDs(ctx, missedL2)
		if err != nil {
			return nil, err
		}

		if err := r.FillTagIDs(ctx, videoBatch); err != nil {
			log.Printf("fill tag ids failed : %v", err)
		}

		for _, v := range videoBatch {
			videoMap[v.ID] = v
			// 回填redis,localcache
			entity := v.ToVideoEntityCache()
			if b, err := json.Marshal(entity); err != nil {
				log.Printf("video entity cache set failed,err: %v", err)
			} else {
				key := r.cache.Key("video:entity:%d", v.ID)
				r.cache.SetBytes(ctx, key, b, entityCacheTTL)
				r.localcache.Set(key, entity, l1CacheTTL)
			}
		}

	}
	// 返回查询结果
	result := make([]video.Video, 0, len(ids))
	for _, v := range ids {
		if item, ok := videoMap[v]; ok {
			result = append(result, item)
		}
	}
	return result, nil
}

// 读取视频实体对应的tag_id
func (r *VideoReader) FillTagIDs(ctx context.Context, videos []video.Video) error {
	ids := make([]uint, 0, len(videos))
	for i := range videos {
		if videos[i].TagIDs == nil {
			ids = append(ids, videos[i].ID)
		}
	}

	if len(ids) == 0 {
		return nil
	}

	tagIDs, err := r.repo.GetTagIDsByVideoIDs(ctx, ids)
	if err != nil {
		return err
	}

	for i := range videos {
		if videos[i].TagIDs == nil {
			videos[i].TagIDs = tagIDs[videos[i].ID]
			if videos[i].TagIDs == nil {
				videos[i].TagIDs = []uint{}
			}
		}
	}

	return nil
}
