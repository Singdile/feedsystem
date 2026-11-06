package feed

import (
	"context"
	"feedsystem/internal/model/video"
	apperrors "feedsystem/internal/pkg/errors"
	"net/http"
	"strconv"
	"time"
)

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

	vs, err := s.videoReader.GetByIDs(ctx, ids)
	if err != nil {
		return nil, apperrors.NewAppError(http.StatusInternalServerError, "查询视频数据失败")
	}

	// 签发数据
	items, err := s.buildViews(ctx, vs)
	if err != nil {
		return nil, err
	}
	// 组装数据并返回，data ,as_of,offset,limit
	return &PopularityResult{
		Items:   items,
		AsOf:    now.Unix(),
		Offset:  offset + int64(limit),
		HasMore: hasmore,
	}, nil
}
