package feed

import (
	"context"
	"feedsystem/internal/model/video"
	apperrors "feedsystem/internal/pkg/errors"
	"net/http"
	"strings"
)

func (s *FeedService) ListByTag(ctx context.Context, accountID uint, tagName, cursorStr string, limit int) (*FeedListResult, error) {
	fetch := func(cursor *video.Cursor, limit int) ([]video.Video, bool, error) {

		if strings.TrimSpace(tagName) == "" {
			return nil, false, apperrors.NewAppError(http.StatusBadRequest, "参数错误")
		}

		cursor, err := decodeCursor(cursorStr)
		if err != nil {
			return nil, false, err
		}

		// 查询数据库获取 videos
		vs, err := s.videoSvc.ListByTag(ctx, tagName, cursor, limit+1)
		if err != nil {
			return nil, false, err
		}
		hasMore := len(vs) > limit
		if hasMore {
			vs = vs[:limit]
		}

		return vs, hasMore, nil
	}
	return s.runFeed(ctx, accountID, cursorStr, limit, fetch)
}
