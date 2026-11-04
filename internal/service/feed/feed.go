// Package feed 提供视频流服务
package feed

import (
	"context"
	"encoding/json"
	"feedsystem/internal/model/feed"
	"feedsystem/internal/model/video"
	apperrors "feedsystem/internal/pkg/errors"
	videsvc "feedsystem/internal/service/video"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
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
	// ZRevRangeByScore 返回 zset 中 score <= maxScore 的元素，按 score 从大到小排序，返回 limit 个
	ZRevRangeByScore(ctx context.Context, key string, maxScore float64, maxID uint, limit int) ([]feed.ZMember, error)
	ZRangeWithScores(ctx context.Context, key string, start, stop int64) ([]feed.ZMember, error)
	ZAdd(ctx context.Context, key string, member feed.ZMember) error
	ZAddBatch(ctx context.Context, key string, members []feed.ZMember) error
	ZUnionStore(ctx context.Context, dest string, keys []string, aggregate string) error
	Expire(ctx context.Context, key string, expire time.Duration) error
	ZRevRange(ctx context.Context, key string, start, stop int64) ([]feed.ZMember, error) //逆序

	// 冷数据查询
	ListLatest(ctx context.Context, cursor *video.Cursor, limit int) ([]video.Video, error)
	ListLatestByAuthors(ctx context.Context, authordIDs []uint, cursor *video.Cursor, limit int) ([]video.Video, error)

	// 实体缓存 (L2 redis)
	Key(format string, a ...any) string
	MGet(ctx context.Context, keys ...string) ([]any, error)
	GetBytes(ctx context.Context, key string) ([]byte, error)
	SetBytes(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Exists(ctx context.Context, key string) (bool, error)
	Del(ctx context.Context, key string) error
}

type VideoProvider interface {
	ListByTag(ctx context.Context, tagName string, cursor *video.Cursor, limit int) ([]video.Video, error)
	GetVideoEntitiesByIDs(ctx context.Context, ids []uint) ([]video.Video, error)
}

type RatingProvider interface {
    GetUserRatings(ctx context.Context, accountID uint, videoID []uint) (stat map[uint]int8, err error)
}

// 返回关注作者列表
type FollowProvider interface {
	ListFollowingIDs(ctx context.Context, fanID uint) ([]uint, error)
}

type FeedService struct {
	repo           FeedRepo
	videoSvc       VideoProvider
	ratingSvc      RatingProvider
	followProvider FollowProvider
	viewBuilder    videsvc.ViewBuilder
	localcache     *cache.Cache       // go-cache:L1,默认5s过期
	group          singleflight.Group // 防并发
}

func NewFeedService(repo FeedRepo, videoSvc VideoProvider, ratingSvc RatingProvider, followProvider FollowProvider, viewbuilder videsvc.ViewBuilder) *FeedService {
	return &FeedService{
		repo:           repo,
		videoSvc:       videoSvc,
		localcache:     cache.New(l1CacheTTL, 10*time.Second),
		ratingSvc:      ratingSvc,
		followProvider: followProvider,
		viewBuilder:    viewbuilder,
	}
}

// FeedListResult feed 响应（Items 复用 video.VideoView）
type FeedListResult struct {
	Items      []video.VideoView `json:"items"`
	NextCursor string            `json:"next_cursor"` // 空 = 没有更多
}

// ListFeed 查询视频数据，返回可播放的视频信息
// 按照发布时间线进行查询
// 1.查找redis里面的时间线，获取前1000条视频ids
//
//	1.1 如果redis失效，直接查询数据库，返回
//	1.2 如果redis有效，先查询redis，获取视频ids
//	    1.2.1 如果redis 为空，查询数据库并重建redis，重新调用 ListFeed
//	    1.2.2 如果redis 不为空，获取cursor之后的视频ids
//
// 2.查找ids对应的视频元数据 （三级缓存查找视频元数据： local-cache->redis->DB)
//
//	2.1 查询并获取热区数据(ids)对应的视频元数据
//	2.2 当热区数据满足一页，使用视频元数据，签发对象存储可播放url并返回
//	2.2 当热区数据没有了且当前获取的视频元数据不足一页(limit)的时候，从数据库中获取冷数据补充完一页，签发对象存储可播放url并返回
func (s *FeedService) ListFeed(ctx context.Context, accountID uint, cursorStr string, limit int) (*FeedListResult, error) {
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
		return s.listLatestFromDB(ctx, accountID, cursor, limit)
	}

	// ZSET 中feed:global_time 为空，重建
	if len(tail) == 0 {
		return s.rebuildAndRetry(ctx, accountID, cursorStr, cursor, limit)
	}

	watermark := tail[0].Score                 // 冷热数据分界线
	reqTime := float64(time.Now().UnixMilli()) // req表示请求视频的时间线，若没有游标则表示请求当前时间之后的视频
	if cursor != nil {
		reqTime = float64(cursor.CreatedAt.UnixMilli())
	}

	if reqTime <= watermark { // 请求的时间比热门最老的视频还要老，那么直接查询DB,不写回ZSET
		return s.listLatestFromDB(ctx, accountID, cursor, limit)
	}

	// 热数据路径，查询缓存中的timeline,获取时间排序的视频
	members, err := s.repo.ZRevRangeByScore(ctx, timelineKey, curScore, curID, limit+1)
	if err != nil {
		return s.listLatestFromDB(ctx, accountID, cursor, limit)
	}

	hotMore := len(members) > limit // 热区(ZSET)是否还有超过一页的更多数据（limit+1 探测）
	if hotMore {
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

	//查找三级缓存，获取视频实体信息
	entities, err := s.GetVideoByIDs(ctx, ids)
	if err != nil {
		return nil, apperrors.NewAppError(http.StatusInternalServerError, "get video failed")
	}

	if len(ids) > 0 && len(entities) == 0 { //如果热区ids存在，但是查找数据库没有，那么直接查询数据库，防止返回空游标
		return s.listLatestFromDB(ctx, accountID, cursor, limit)
	}

	// 冷热缝合：hasMore 初始为"热区是否还有更多"
	hasMore := hotMore
	if !hotMore && len(entities) < limit { // 只有热点数据用完并且不满页的时候，会从DB冷区取出数据
		coldCursor := cursor
		if len(entities) > 0 {
			last := entities[len(entities)-1]
			coldCursor = &video.Cursor{
				CreatedAt: last.CreatedAt,
				ID:        last.ID,
			}
		}

		remain := limit - len(entities)
		cold, err := s.repo.ListLatest(ctx, coldCursor, remain+1)
		if err == nil {
			coldMore := len(cold) > remain
			if coldMore {
				cold = cold[:remain]
			}
			entities = append(entities, cold...)
			hasMore = coldMore // 冷区(DB)是否还有更多
		}
	}

	// 边界盲区修复：热区已耗尽但本页恰好取满 limit 条时，limit+1 探测只覆盖热区 ZSET，
	// 无法确定冷区(DB)是否还有更多。此时假设"可能还有"，交出游标，
	// 下一次请求会因 reqTime <= watermark 落入冷路径，从而接上 DB 冷数据。
	hasMore = hasMore || len(entities) == limit

	// next_cursor：仅当"可能还有更多"时才返回游标，否则前端判定没有更多
	next := ""
	if hasMore && len(entities) > 0 {
		last := entities[len(entities)-1]
		next = video.EncodeCursor(video.Cursor{CreatedAt: last.CreatedAt, ID: last.ID})
	}

	// sign url
	viewResult := s.viewBuilder.BuildSome(ctx, entities)
	for _, failure := range viewResult.Failures {
		log.Printf("视频展示失败：video_id = %d,err = %v", failure.VideoID, failure.Err)
	}

	if len(viewResult.Items) == 0 && len(viewResult.Failures) > 0 {
		return nil, apperrors.NewAppError(http.StatusInternalServerError, "视频暂时无法展示")
	}

	// 获取用户对视频的评价信息
	if _, err = s.stampStatus(ctx, accountID, viewResult.Items); err != nil {
		log.Printf("failed to stamp views: %v", err)
	}

	return &FeedListResult{Items: viewResult.Items, NextCursor: next}, nil
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
			var cached video.VideoEntityCache
			if err := json.Unmarshal([]byte(str), &cached); err != nil {
				missedL2 = append(missedL2, missedL1[i]) // 反序列化失败，交给L3
				continue
			}
			entity := cached.ToVideo()
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
		cached := entity.ToVideoEntityCache()
		if b, err := json.Marshal(cached); err == nil {
			_ = s.repo.SetBytes(ctx, key, b, entityCacheTTL) // 回填L2
		} // write back to L2
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
func (s *FeedService) listLatestFromDB(ctx context.Context, account uint, cursor *video.Cursor, limit int) (*FeedListResult, error) {
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

	viewResult := s.viewBuilder.BuildSome(ctx, vs)
	for _, failure := range viewResult.Failures {
		log.Printf("视频展示失败：video_id = %d,err = %v", failure.VideoID, failure.Err)
	}

	if len(viewResult.Items) == 0 && len(viewResult.Failures) > 0 {
		return nil, apperrors.NewAppError(http.StatusInternalServerError, "视频暂时无法展示")
	}

	if _, err := s.stampStatus(ctx, account, viewResult.Items); err != nil {
		log.Printf("failed to stamp views: %v", err)
	}

	// 回填 L2;L1
	for _, v := range vs {
		key := s.repo.Key("video:entity:%d", v.ID)
		videoEntityCache := v.ToVideoEntityCache()
		if b, err := json.Marshal(videoEntityCache); err == nil {
			_ = s.repo.SetBytes(ctx, key, b, entityCacheTTL)
		}
		s.localcache.Set(key, v, l1CacheTTL)
	}

	// nextcursor
	nextCursor := ""
	if hasmore {
		last := vs[len(vs)-1]
		nextCursor = video.EncodeCursor(video.Cursor{CreatedAt: last.CreatedAt, ID: last.ID})
	}

	// 返回可播放信息
	return &FeedListResult{
		Items:      viewResult.Items,
		NextCursor: nextCursor,
	}, nil
}

// rebuildAndRetry ZSET为空的时候重建，从DB中拉取最近的1000条回填 Redis，然后重新走 ListFeed
func (s *FeedService) rebuildAndRetry(ctx context.Context, accountID uint, cursorStr string, cursor *video.Cursor, limit int) (*FeedListResult, error) {
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
		return s.listLatestFromDB(ctx, accountID, cursor, limit)
	}

	if s, ok := res.(string); ok && s == "EMPTY_DB" {
		return &FeedListResult{Items: []video.VideoView{}}, nil
	}

	return s.ListFeed(ctx, accountID, cursorStr, limit)
}

func (s *FeedService) ListByTag(ctx context.Context, accountID uint, tagName, cursorStr string, limit int) (*FeedListResult, error) {
	if strings.TrimSpace(tagName) == "" {
		return nil, apperrors.NewAppError(http.StatusBadRequest, "参数错误")
	}

	if limit <= 0 || limit > 100 {
		limit = 20
	}

	// 解析cursor：空串=首页，不解码；非空才解码
	var cursor *video.Cursor
	if cursorStr != "" {
		cur, err := video.DecodeCursor(cursorStr)
		if err != nil {
			return nil, apperrors.NewAppError(http.StatusBadRequest, "bad_cursor")
		}
		cursor = &cur
	}

	// 查询数据库获取 videos
	videos, err := s.videoSvc.ListByTag(ctx, tagName, cursor, limit+1)
	if err != nil {
		return nil, err
	}
	hasMore := len(videos) > limit
	if hasMore {
		videos = videos[:limit]
	}

	// 签名video，获取可播放url
	viewResult := s.viewBuilder.BuildSome(ctx, videos)
	for _, failure := range viewResult.Failures {
		log.Printf("视频展示失败：video_id = %d,err = %v", failure.VideoID, failure.Err)
	}

	if len(viewResult.Items) == 0 && len(viewResult.Failures) > 0 {
		return nil, apperrors.NewAppError(http.StatusInternalServerError, "视频暂时无法展示")
	}

	// next
	next := ""
	if hasMore {
		last := videos[len(videos)-1]
		cur := video.Cursor{
			CreatedAt: last.CreatedAt,
			ID:        last.ID,
		}
		next = video.EncodeCursor(cur)
	}

	if _, err := s.stampStatus(ctx, accountID, viewResult.Items); err != nil {
		log.Printf("fail to get user's rating status,err:%v", err)
	}

	return &FeedListResult{
		Items:      viewResult.Items,
		NextCursor: next,
	}, nil
}

// stampStatus 查询用户用户对于一批视频的评价，匿名用户保持评价的默认值就行了
func (s *FeedService) stampStatus(ctx context.Context, accountID uint, views []video.VideoView) ([]video.VideoView, error) {
	if accountID == 0 || len(views) == 0 {
		return views, nil
	}

	ids := make([]uint, 0, len(views))
	for _, v := range views {
		ids = append(ids, v.ID)
	}

	// 查询rating
	statMap,err := s.ratingSvc.GetUserRatings(ctx,accountID,ids)
	if err != nil {
		return views,err
	}

	for i,v := range views {
		views[i].Status = statMap[v.ID]
	}
	return views, nil
}

type PopularityResult struct {
	Items   []video.VideoView `json:"items"`
	AsOf    int64             `json:"as_of"`    //时间戳
	Offset  int64             `json:"offset"`   //偏移
	HasMore bool              `json:"has_more"` //是否还有
}

func (s *FeedService) ListByPopularity(ctx context.Context, accountID uint, asof, offset int64, limit int) (*PopularityResult, error) {
	// 首先查看cache 里面有没有当前时间戳的60分钟快照缓存
	now := time.Now().UTC().Truncate(time.Minute) //当前事件
	flag := false
	if asof > 0 { //请求时间戳
		reqNow := time.Unix(asof, 0).UTC().Truncate(time.Minute)
		key := s.repo.Key("hot:video:60m:%s", reqNow.Format("200601021504"))
		ok, err := s.repo.Exists(ctx, key)
		if err != nil {
			return nil, apperrors.NewAppError(http.StatusInternalServerError, "cache failed to get hot")
		}

		if ok {
			now = reqNow
			flag = true
		}
	}

	if !flag { //说明asof快照并没有，需要融合当前时间戳往前的60分钟数据
		keys := make([]string, 0, 60)
		for i := 1; i <= 60; i++ {
			key := s.repo.Key("hot:video:1m:%s", now.Add(-time.Duration(i)*time.Minute).Format("200601021504"))
			keys = append(keys, key)
		}
		dest := s.repo.Key("hot:video:60m:%s", now.Format("200601021504"))
		s.repo.ZUnionStore(ctx, dest, keys, "SUM")
		s.repo.Expire(ctx, dest, 2*time.Minute)
	}

	// 获取到视频热度排行榜单，即对应的视频ids
	dest := s.repo.Key("hot:video:60m:%s", now.Format("200601021504"))
	members, err := s.repo.ZRevRange(ctx, dest, offset, offset+int64(limit))
	if err != nil {
		return nil, apperrors.NewAppError(http.StatusInternalServerError, "cache failed to get hot")
	}

	hasmore := len(members) > limit
	if hasmore {
		members = members[:limit]
	}

	// 查询数据库
	ids := make([]uint, 0, len(members))
	for _, v := range members {
		id, err := strconv.ParseUint(v.Member, 10, 64)
		if err != nil {
			continue
		}
		ids = append(ids, uint(id))
	}

	vs, err := s.GetVideoByIDs(ctx, ids)
	if err != nil {
		return nil, apperrors.NewAppError(http.StatusInternalServerError, "查询视频数据失败")
	}

	// 签发数据
	viewResult := s.viewBuilder.BuildSome(ctx, vs)
	for _, failure := range viewResult.Failures {
		log.Printf("视频展示失败：video_id = %d,err = %v", failure.VideoID, failure.Err)
	}

	if len(viewResult.Items) == 0 && len(viewResult.Failures) > 0 {
		return nil, apperrors.NewAppError(http.StatusInternalServerError, "视频暂时无法展示")
	}

	// 组装数据并返回，data ,as_of,offset,limit
	return &PopularityResult{
		Items:   viewResult.Items,
		AsOf:    now.Unix(),
		Offset:  offset + int64(limit),
		HasMore: hasmore,
	}, nil
}

// ListByFollowing 用户关注流的视频数据
func (s *FeedService) ListByFollowing(ctx context.Context, accountID uint, cursorStr string, limit int) (*FeedListResult, error) {
	key := s.repo.Key("feed:following:%d", accountID)

	cursor := &video.Cursor{
		CreatedAt: time.Now(),
		ID:        0,
	}
	if cursorStr != "" {
		cur, err := video.DecodeCursor(cursorStr)
		if err != nil {
			return nil, apperrors.NewAppError(http.StatusBadRequest, "cursor 参数错误")
		}
		cursor = &cur
	}

	reqTime := float64(time.Now().UnixMilli())
	if cursor != nil {
		reqTime = float64(cursor.CreatedAt.UnixMilli())
	}

	// 确保cache可用，若没有数据，则触发懒加载冷数据
	if err := s.ensureFollowingTimeLine(ctx, accountID); err != nil { //cache不可用，降级DB查询
		return s.listFollowingFromDB(ctx, accountID, cursor, limit)
	}

	// cache 可用，查询关注流数据
	tail, err := s.repo.ZRangeWithScores(ctx, key, 0, 0)
	if err != nil || len(tail) == 0 {
		return s.listFollowingFromDB(ctx, accountID, cursor, limit)
	}

	watermark := tail[0].Score
	if reqTime <= watermark { //热区数据最老的视频都比要查询的视频新，所以直接查询数据库
		return s.listFollowingFromDB(ctx, accountID, cursor, limit)
	}

	// 先查询cache，热区数据
	members, err := s.repo.ZRevRangeByScore(ctx, key, float64(cursor.CreatedAt.UnixMilli()), cursor.ID, limit+1)
	if err != nil {
		return s.listFollowingFromDB(ctx, accountID, cursor, limit)
	}

	hotMore := len(members) > limit
	if hotMore {
		members = members[:limit]
	}

	ids := make([]uint, 0, len(members))
	for _, v := range members {
		id, err := strconv.ParseUint(v.Member, 10, 64)
		if err != nil || id == 0 {
			continue
		}
		ids = append(ids, uint(id))
	}

	entities, err := s.GetVideoByIDs(ctx, ids)
	if err != nil {
		return nil, apperrors.NewAppError(http.StatusInternalServerError, "视频查询失败")
	}

	hasMore := hotMore
	// 判断还需不需要冷数据
	if !hotMore && len(entities) < limit {
		remain := limit - len(entities)
		coldcursor := cursor
		if len(entities) > 0 {
			last := entities[len(entities)-1]
			coldcursor = &video.Cursor{
				CreatedAt: last.CreatedAt,
				ID:        last.ID,
			}
		}

		authors, err := s.followProvider.ListFollowingIDs(ctx, accountID)
		if err != nil {
			return nil, apperrors.NewAppError(http.StatusInternalServerError, "用户关注列表查询失败")
		}

		cold, err := s.repo.ListLatestByAuthors(ctx, authors, coldcursor, remain+1)
		if err != nil {
			return nil, apperrors.NewAppError(http.StatusInternalServerError, "视频查询失败")
		}
		coldMore := len(cold) > remain
		if coldMore {
			cold = cold[:remain]
			hasMore = coldMore
		}
		entities = append(entities, cold...)
	}

	next := ""
	if hasMore && len(entities) > 0 {
		last := entities[len(entities)-1]
		next = video.EncodeCursor(video.Cursor{
			CreatedAt: last.CreatedAt,
			ID:        last.ID,
		})
	}
	// 关注流为空（未关注任何人/关注者无作品）直接返回空，避免 BuildViews 空列表报错
	if len(entities) == 0 {
		return &FeedListResult{Items: []video.VideoView{}, NextCursor: next}, nil
	}

	// 数据查询结束，sign url 获取可播放资源
	viewResult := s.viewBuilder.BuildSome(ctx, entities)
	for _, failure := range viewResult.Failures {
		log.Printf("视频展示失败：video_id = %d,err = %v", failure.VideoID, failure.Err)
	}

	if len(viewResult.Items) == 0 && len(viewResult.Failures) > 0 {
		return nil, apperrors.NewAppError(http.StatusInternalServerError, "视频暂时无法展示")
	}

	// 获取用户对视频的评价信息
	if _, err = s.stampStatus(ctx, accountID, viewResult.Items); err != nil {
		log.Printf("failed to stamp views: %v", err)
	}

	return &FeedListResult{Items: viewResult.Items, NextCursor: next}, nil
}

// listFollowingFromDB 直接DB查询关注作者的视频
func (s *FeedService) listFollowingFromDB(ctx context.Context, accountID uint, cursor *video.Cursor, limit int) (*FeedListResult, error) {
	// 查询用户关注博主id
	authors, err := s.followProvider.ListFollowingIDs(ctx, accountID)
	if err != nil {
		return nil, err
	}
	// 查询博主视频
	vs, err := s.repo.ListLatestByAuthors(ctx, authors, cursor, limit+1)
	if err != nil {
		return nil, apperrors.NewAppError(http.StatusInternalServerError, "查询失败")
	}

	hasMore := len(vs) > limit
	if hasMore {
		vs = vs[:limit]
	}

	// 计算next_cursor
	next := ""
	if hasMore && len(vs) > 0 {
		last := vs[len(vs)-1]
		next = video.EncodeCursor(video.Cursor{
			CreatedAt: last.CreatedAt,
			ID:        last.ID,
		})
	}

	// 关注流为空直接返回空，避免 BuildViews 空列表报错
	if len(vs) == 0 {
		return &FeedListResult{Items: []video.VideoView{}, NextCursor: next}, nil
	}

	// 签发可播放的视频信息
	viewResult := s.viewBuilder.BuildSome(ctx, vs)
	for _, failure := range viewResult.Failures {
		log.Printf("视频展示失败：video_id = %d,err = %v", failure.VideoID, failure.Err)
	}

	if len(viewResult.Items) == 0 && len(viewResult.Failures) > 0 {
		return nil, apperrors.NewAppError(http.StatusInternalServerError, "视频暂时无法展示")
	}

	return &FeedListResult{Items: viewResult.Items, NextCursor: next}, nil
}

// ensureFollowingTimeLine 判断是否存在，不存在懒记载用户关注视频流
func (s *FeedService) ensureFollowingTimeLine(ctx context.Context, accountID uint) error {
	key := s.repo.Key("feed:following:%d", accountID)
	exist, err := s.repo.Exists(ctx, key)
	if err != nil {
		return err
	}

	if exist { // 存在
		return nil
	}

	// 不存在，尝试重建
	authors, err := s.followProvider.ListFollowingIDs(ctx, accountID)
	if err != nil {
		return err
	}

	vs, err := s.repo.ListLatestByAuthors(ctx, authors, nil, 1000)
	if err != nil {
		return err
	}

	// 写入cache
	members := make([]feed.ZMember, len(vs))
	for i, v := range vs {
		members[i].Score = float64(v.CreatedAt.UnixMilli())
		members[i].Member = strconv.FormatUint(uint64(v.ID), 10)
	}

	if len(members) == 0 {
		members = []feed.ZMember{{
			Score:  0,
			Member: "0",
		},
		}
	}
	if err := s.repo.ZAddBatch(ctx, key, members); err != nil {
		return err
	}

	return s.repo.Expire(ctx, key, 7*24*time.Hour)
}

// BackfillFollowing 新关注作者之后，将其最近作品回填到用户的关注流
// 仅在关注流已存在的时候回填（不存在则交给懒加载重建）
func (s *FeedService) BackfillFollowing(ctx context.Context, fanID, vloggerID uint) error {
	key := s.repo.Key("feed:following:%d", fanID)

	// 判断关注流是否存在
	exists, err := s.repo.Exists(ctx, key)
	if err != nil {
		return err
	}

	if !exists { //交给懒加载
		return nil
	}

	backfilllimit := 100
	vs, err := s.repo.ListLatestByAuthors(ctx, []uint{vloggerID}, nil, backfilllimit)
	if err != nil {
		return err
	}

	if len(vs) == 0 {
		return nil
	}

	members := make([]feed.ZMember, 0, len(vs))
	for _, v := range vs {
		members = append(members, feed.ZMember{
			Score:  float64(v.CreatedAt.UnixMilli()),
			Member: strconv.FormatUint(uint64(v.ID), 10),
		})
	}
	if err := s.repo.ZAddBatch(ctx, key, members); err != nil {
		return err
	}
	return s.repo.Expire(ctx, key, 7*24*time.Hour) // 重置 TT
}

// ClearFollowingTimeline 取关时清空用户关注流时间线
func (s *FeedService) ClearFollowingTimeline(ctx context.Context, fanID uint) error {
	return s.repo.Del(ctx, s.repo.Key("feed:following:%d", fanID))
}
