package feed

import (
	"context"
	"feedsystem/internal/model/feed"
	"feedsystem/internal/model/video"
	apperrors "feedsystem/internal/pkg/errors"
	"net/http"
	"strconv"
	"time"
)

func (s *FeedService) ListByFollowing(ctx context.Context, accountID uint, cursorStr string, limit int) (*FeedListResult, error) {
	// 从用户的关注时间流中取出视频实体
	fetch := func(cursor *video.Cursor, limit int) ([]video.Video, bool, error) {
		key := s.repo.Key("feed:following:%d", accountID)
		cursor, err := decodeCursor(cursorStr)
		if err != nil {
			return nil, false, err
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

		reqTime := float64(time.Now().UnixMilli())
		if cursor != nil {
			reqTime = float64(cursor.CreatedAt.UnixMilli())
		}

		watermark := tail[0].Score
		if reqTime <= watermark { //热区数据最老的视频都比要查询的视频新，所以直接查询数据库
			return s.listFollowingFromDB(ctx, accountID, cursor, limit)
		}

		var curID uint
		if cursor != nil {
			curID = cursor.ID
		}

		// 先查询cache，热区数据
		members, err := s.repo.ZRevRangeByScore(ctx, key, reqTime, curID, limit+1)
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

		vs, err := s.videoReader.GetByIDs(ctx, ids)
		if err != nil {
			return nil, false, apperrors.NewAppError(http.StatusInternalServerError, "视频查询失败")
		}

		hasMore := hotMore || len(vs) == limit
		// 判断还需不需要冷数据
		if !hotMore && len(vs) < limit {
			remain := limit - len(vs)
			coldcursor := cursor
			if len(vs) > 0 {
				last := vs[len(vs)-1]
				coldcursor = &video.Cursor{
					CreatedAt: last.CreatedAt,
					ID:        last.ID,
				}
			}

			authors, err := s.followProvider.ListFollowingIDs(ctx, accountID)
			if err != nil {
				return nil, false, apperrors.NewAppError(http.StatusInternalServerError, "用户关注列表查询失败")
			}

			cold, err := s.repo.ListLatestByAuthors(ctx, authors, coldcursor, remain+1)
			if err != nil {
				return nil, false, apperrors.NewAppError(http.StatusInternalServerError, "视频查询失败")
			}
			coldMore := len(cold) > remain
			if coldMore {
				cold = cold[:remain]
				hasMore = coldMore
			}
			vs = append(vs, cold...)
		}

		return vs, hasMore, nil
	}

	return s.runFeed(ctx, accountID, cursorStr, limit, fetch)
}

// listFollowingFromDB 直接DB查询关注作者的视频
func (s *FeedService) listFollowingFromDB(ctx context.Context, accountID uint, cursor *video.Cursor, limit int) ([]video.Video, bool, error) {
	// 查询用户关注博主id
	authors, err := s.followProvider.ListFollowingIDs(ctx, accountID)
	if err != nil {
		return nil, false, err
	}
	// 查询博主视频
	vs, err := s.repo.ListLatestByAuthors(ctx, authors, cursor, limit+1)
	if err != nil {
		return nil, false, apperrors.NewAppError(http.StatusInternalServerError, "查询失败")
	}

	hasMore := len(vs) > limit
	if hasMore {
		vs = vs[:limit]
	}

	return vs, hasMore, nil
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
