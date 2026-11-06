package feed

import (
	"context"
	"feedsystem/internal/model/feed"
	"feedsystem/internal/model/video"
	apperrors "feedsystem/internal/pkg/errors"
	"log"
	"net/http"
	"strconv"
	"time"
)

// GlobalFeed 全局推荐流(全局时间线热点数据+DB冷区数据)
func (s *FeedService) GlobalFeed(ctx context.Context, accountID uint, cursorStr string, limit int) (*FeedListResult, error) {
	globalFetch := func(cursor *video.Cursor, limit int) ([]video.Video, bool, error) {
		var vs []video.Video
		// 先查询redis 里面的ZSET 保存的数据，获取视频IDs
		if err := s.ensureTimeline(ctx); err != nil { //先确保redis可以使用
			return s.listFromDB(ctx, accountID, cursor, limit)
		}

		// 热区数据不足limit个的时候要查询冷区数据
		// 查看当前redis里面的最老的视频数据
		tail, err := s.repo.ZRangeWithScores(ctx, timelineKey, 0, 0)
		if err != nil || len(tail) == 0 {
			// redis fail -> DB
			return s.listFromDB(ctx, accountID, cursor, limit)
		}

		watermark := tail[0].Score                 // 冷热数据分界线
		reqTime := float64(time.Now().UnixMilli()) // req表示请求视频的时间线，若没有游标则表示请求当前时间之后的视频
		if cursor != nil {
			reqTime = float64(cursor.CreatedAt.UnixMilli())
		}

		if reqTime <= watermark { // 请求的时间比热门最老的视频还要老，那么直接查询DB,不写回ZSET
			return s.listFromDB(ctx, accountID, cursor, limit)
		}

		// 热数据路径，查询缓存中的timeline,获取时间排序的视频
		var curID uint
		if cursor != nil {
			curID = cursor.ID
		}
		members, err := s.repo.ZRevRangeByScore(ctx, timelineKey, reqTime, curID, limit+1)
		if err != nil {
			return s.listFromDB(ctx, accountID, cursor, limit)
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

			if videID == 0 {
				continue
			}
			ids = append(ids, uint(videID))
		}

		//查找三级缓存，获取视频实体信息
		vs, err = s.videoReader.GetByIDs(ctx, ids)
		if err != nil {
			return nil, false, apperrors.NewAppError(http.StatusInternalServerError, "get video failed")
		}

		if len(ids) > 0 && len(vs) == 0 { //如果热区ids存在，但是查找数据库没有，那么直接查询数据库，防止返回空游标
			return s.listFromDB(ctx, accountID, cursor, limit)
		}

		// 冷热缝合：hasMore 初始为"热区是否还有更多"
		hasMore := hotMore
		if !hotMore && len(vs) < limit { // 只有热点数据用完并且不满页的时候，会从DB冷区取出数据
			coldCursor := cursor
			if len(vs) > 0 {
				last := vs[len(vs)-1]
				coldCursor = &video.Cursor{
					CreatedAt: last.CreatedAt,
					ID:        last.ID,
				}
			}

			remain := limit - len(vs)
			cold, err := s.repo.ListLatest(ctx, coldCursor, remain+1)
			if err != nil {
				log.Printf("全站流冷区查询失败: cursor=%+v err=%v", coldCursor, err)
				return nil, false, apperrors.NewAppError(
					http.StatusInternalServerError,
					"视频暂时无法加载",
				)
			} else {
				coldMore := len(cold) > remain
				if coldMore {
					cold = cold[:remain]
				}
				vs = append(vs, cold...)
				hasMore = coldMore // 冷区(DB)是否还有更多
			}
		}

		// 边界盲区修复：热区已耗尽但本页恰好取满 limit 条时，limit+1 探测只覆盖热区 ZSET，
		// 无法确定冷区(DB)是否还有更多。此时假设"可能还有"，交出游标，
		// 下一次请求会因 reqTime <= watermark 落入冷路径，从而接上 DB 冷数据。
		hasMore = hasMore || len(vs) == limit

		return vs, hasMore, nil
	}

	return s.runFeed(ctx, accountID, cursorStr, limit, globalFetch)
}

// ensureTimeline 时间线为空的时候从 DB 懒加载重建
func (s *FeedService) ensureTimeline(ctx context.Context) error {
	tail, err := s.repo.ZRangeWithScores(ctx, timelineKey, 0, 0)
	if err != nil {
		return err
	}

	if len(tail) > 0 {
		return nil
	}

	// cache为空，重建
	_, err, _ = s.group.Do("sf:rebuild:global_time", func() (any, error) {
		vs, err := s.repo.ListLatest(ctx, nil, 1000)
		if err != nil {
			return nil, err
		}

		members := make([]feed.ZMember, 0, len(vs))
		for _, v := range vs {
			members = append(members, feed.ZMember{
				Score:  float64(v.CreatedAt.UnixMilli()),
				Member: strconv.FormatUint(uint64(v.ID), 10),
			})
		}

		if len(members) == 0 {
			members = append(members, feed.ZMember{Score: float64(0), Member: "0"})
		}

		return nil, s.repo.ZAddBatch(ctx, timelineKey, members)
	})

	return err
}

// 从DB中获取最新的视频数据
func (s *FeedService) listFromDB(ctx context.Context, account uint, cursor *video.Cursor, limit int) ([]video.Video, bool, error) {
	// DB中获取数据
	vs, err := s.repo.ListLatest(ctx, cursor, limit+1)
	if err != nil {
		return nil, false, err
	}

	hasMore := len(vs) > limit
	if hasMore {
		vs = vs[:limit]
	}
	return vs, hasMore, err
}
