package feed

import (
	"context"
	"feedsystem/internal/model/feed"
	"feedsystem/internal/model/video"
	"strconv"
	"time"
)

type FollowingMaintainer struct {
	repo FollowRepo
}

type FollowRepo interface {
	Key(format string, a ...any) string
	ListLatestByAuthors(ctx context.Context, authordIDs []uint, cursor *video.Cursor, limit int) ([]video.Video, error)
	Exists(ctx context.Context, key string) (bool, error)
	ZAddBatch(ctx context.Context, key string, members []feed.ZMember) error
	Expire(ctx context.Context, key string, expire time.Duration) error
	Del(ctx context.Context, key string) error
}

func NewFollowingMaintainer(repo FollowRepo) *FollowingMaintainer {
	return &FollowingMaintainer{
		repo: repo,
	}
}

// BackfillFollowing 新关注作者之后，将其最近作品回填到用户的关注流
// 仅在关注流已存在的时候回填（不存在则交给懒加载重建）
func (m *FollowingMaintainer) BackfillFollowing(ctx context.Context, fanID, vloggerID uint) error {
	key := m.repo.Key("feed:following:%d", fanID)

	// 判断关注流是否存在
	exists, err := m.repo.Exists(ctx, key)
	if err != nil {
		return err
	}

	if !exists { //交给懒加载
		return nil
	}

	backfilllimit := 100
	vs, err := m.repo.ListLatestByAuthors(ctx, []uint{vloggerID}, nil, backfilllimit)
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
	if err := m.repo.ZAddBatch(ctx, key, members); err != nil {
		return err
	}
	return m.repo.Expire(ctx, key, 7*24*time.Hour) // 重置 TT
}

// ClearFollowingTimeline 取关时清空用户关注流时间线
func (m *FollowingMaintainer) ClearFollowingTimeline(ctx context.Context, fanID uint) error {
	return m.repo.Del(ctx, m.repo.Key("feed:following:%d", fanID))
}
