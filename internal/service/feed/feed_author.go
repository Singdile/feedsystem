package feed

import (
	"context"
	"feedsystem/internal/model/video"
	apperrors "feedsystem/internal/pkg/errors"
	"net/http"
)

// ListByAuthor 作者作品流（authorID == 0 表示全站），按发布时间倒序 + 游标分页
func (s *FeedService) ListByAuthor(ctx context.Context, accountID, authorID uint, cursorStr string, limit int) (*FeedListResult, error) {
	fetch := func(cursor *video.Cursor, limit int) ([]video.Video, bool, error) {

		if limit <= 0 || limit >= 100 {
			limit = 20
		}

		cursor, err := decodeCursor(cursorStr)
		if err != nil {
			return nil, false, err
		}

		var vs []video.Video
		if authorID > 0 {
			vs, err = s.repo.ListLatestByAuthors(ctx, []uint{authorID}, cursor, limit+1)
		} else {
			vs, err = s.repo.ListLatest(ctx, cursor, limit+1)
		}
		if err != nil {
			return nil, false, apperrors.NewAppError(http.StatusInternalServerError, "查询失败")
		}

		hasMore := len(vs) > limit
		if hasMore {
			vs = vs[:limit]
		}
		return vs, hasMore, nil
	}

	return s.runFeed(ctx, accountID, cursorStr, limit, fetch)

}
