package video

import (
	"context"
	"errors"
	videomodel "feedsystem/internal/model/video"
	apperrors "feedsystem/internal/pkg/errors"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// 只替换外部依赖，所有用例调用真实的 VideoViewBuilder。
type viewSignerCall struct {
	key    string
	expiry time.Duration
}

// 签名mock
type viewTestSigner struct {
	calls []viewSignerCall //记录builder请求签名的对象key和有效期
	fail  map[string]error //指定什么对象签名要失败
}

func (s *viewTestSigner) PresignedGetObject(_ context.Context, key string, expiry time.Duration) (string, error) {
	s.calls = append(s.calls, viewSignerCall{key: key, expiry: expiry})
	if err := s.fail[key]; err != nil {
		return "", err
	}
	return "https://media.example/" + key, nil
}

// 标签查询mock
type viewTestTags struct {
	calls [][]uint          //记录每次查询的视频id
	tags  map[uint][]string //模拟查询结果
	err   error             // 模拟数据库故障
}

func (g *viewTestTags) GetTagsByVideoIDs(_ context.Context, ids []uint) (map[uint][]string, error) {
	// 保存参数副本，避免被调用方后续修改切片影响断言。
	g.calls = append(g.calls, append([]uint(nil), ids...))
	return g.tags, g.err
}

var (
	_ URLSigner  = (*viewTestSigner)(nil)
	_ TagsGetter = (*viewTestTags)(nil)
)

// 准备统一的视频数据
func viewTestVideo(id uint) videomodel.Video {
	idStr := strconv.FormatUint(uint64(id), 10)
	return videomodel.Video{
		ID:            id,
		AuthorID:      7,
		Username:      "author",
		Title:         "video-" + idStr,
		Description:   "a video description",
		VideoKey:      "videos/" + idStr + ".mp4",
		CoverKey:      "covers/" + idStr + ".jpg",
		LikedCount:    5,
		DislikedCount: 2,
		CommentCount:  8,
		CreatedAt:     time.Date(2026, time.November, 4, 8, 0, 0, 0, time.UTC),
	}
}

func TestVideoViewBuilderBuildOneFields(t *testing.T) {
	signer := &viewTestSigner{}
	tags := &viewTestTags{tags: map[uint][]string{
		42: {"旅行", "摄影"},
		99: {"其他视频的标签"},
	}}
	builder := NewVideoViewBuilder(signer, tags)
	input := viewTestVideo(42)
	original := input

	view, err := builder.BuildOne(t.Context(), input)

	require.NoError(t, err)
	require.Equal(t, videomodel.VideoView{
		ID:            42,
		Title:         "video-42",
		Description:   "a video description",
		Author:        videomodel.Author{ID: 7, Username: "author"},
		PlayURL:       "https://media.example/videos/42.mp4",
		CoverURL:      "https://media.example/covers/42.jpg",
		LikedCount:    5,
		DislikedCount: 2,
		CommentCount:  8,
		CreatedAt:     time.Date(2026, time.November, 4, 8, 0, 0, 0, time.UTC),
		HashTags:      []string{"旅行", "摄影"},
	}, view)
	require.Equal(t, [][]uint{{42}}, tags.calls)
	require.ElementsMatch(t, []viewSignerCall{
		{key: "videos/42.mp4", expiry: time.Hour},
		{key: "covers/42.jpg", expiry: time.Hour},
	}, signer.calls)
	require.Equal(t, original, input)
}

func TestVideoViewBuilderBuildOneFailures(t *testing.T) {
	for _, tc := range []struct {
		name         string
		failedKey    string
		missingCover bool
		message      string
	}{
		{name: "playback_signing", failedKey: "videos/42.mp4", message: "生成播放地址失败"},
		{name: "cover_signing", failedKey: "covers/42.jpg", message: "生成封面地址失败"},
		{name: "missing_cover", missingCover: true, message: "封面数据缺失"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			signer := &viewTestSigner{fail: map[string]error{tc.failedKey: errors.New("storage unavailable")}}
			tags := &viewTestTags{}
			input := viewTestVideo(42)
			if tc.missingCover {
				input.CoverKey = ""
			}

			view, err := NewVideoViewBuilder(signer, tags).BuildOne(t.Context(), input)

			require.Error(t, err)
			require.Equal(t, videomodel.VideoView{}, view)
			var appErr *apperrors.AppError
			require.ErrorAs(t, err, &appErr)
			require.Equal(t, http.StatusInternalServerError, appErr.Status)
			require.Equal(t, tc.message, appErr.Msg)
			require.Empty(t, tags.calls, "无法展示的视频不应继续查询标签")
		})
	}
}

func TestVideoViewBuilderBuildOneOptionalTags(t *testing.T) {
	for _, tc := range []struct {
		name string
		tags map[uint][]string
		err  error
	}{
		{name: "no_tags", tags: map[uint][]string{}},
		{name: "nil_map"},
		{name: "tag_query_failed", err: errors.New("tag database unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tags := &viewTestTags{tags: tc.tags, err: tc.err}
			view, err := NewVideoViewBuilder(&viewTestSigner{}, tags).BuildOne(t.Context(), viewTestVideo(42))

			require.NoError(t, err)
			require.Equal(t, uint(42), view.ID)
			require.Equal(t, "https://media.example/videos/42.mp4", view.PlayURL)
			require.Equal(t, "https://media.example/covers/42.jpg", view.CoverURL)
			require.Equal(t, uint(5), view.LikedCount)
			require.Empty(t, view.HashTags)
			require.Equal(t, [][]uint{{42}}, tags.calls)
		})
	}
}

func TestVideoViewBuilderBuildSomeResults(t *testing.T) {
	for _, tc := range []struct {
		name          string
		failedKeys    []string
		missingCover  bool
		wantIDs       []uint
		failedIndexes []int
	}{
		{name: "all_success", wantIDs: []uint{30, 10, 20, 40}},
		{name: "middle_failure", failedKeys: []string{"videos/10.mp4"}, wantIDs: []uint{30, 20, 40}, failedIndexes: []int{1}},
		{name: "first_and_last_fail", failedKeys: []string{"videos/30.mp4", "videos/40.mp4"}, wantIDs: []uint{10, 20}, failedIndexes: []int{0, 3}},
		{name: "cover_failure", failedKeys: []string{"covers/20.jpg"}, wantIDs: []uint{30, 10, 40}, failedIndexes: []int{2}},
		{name: "missing_cover", missingCover: true, wantIDs: []uint{30, 10, 40}, failedIndexes: []int{2}},
		{name: "all_failed", failedKeys: []string{"videos/30.mp4", "videos/10.mp4", "videos/20.mp4", "videos/40.mp4"}, wantIDs: []uint{}, failedIndexes: []int{0, 1, 2, 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			videos := []videomodel.Video{viewTestVideo(30), viewTestVideo(10), viewTestVideo(20), viewTestVideo(40)}
			if tc.missingCover {
				videos[2].CoverKey = ""
			}
			original := append([]videomodel.Video(nil), videos...)
			signer := &viewTestSigner{fail: make(map[string]error)}
			for _, key := range tc.failedKeys {
				signer.fail[key] = errors.New("signing failed")
			}
			tags := &viewTestTags{tags: map[uint][]string{
				10: {"十"}, 20: {"二十"}, 30: {"三十"}, 40: {"四十"},
			}}

			result := NewVideoViewBuilder(signer, tags).BuildSome(t.Context(), videos)

			require.NotNil(t, result.Items)
			require.NotNil(t, result.Failures)
			require.Equal(t, len(videos), len(result.Items)+len(result.Failures))
			ids := make([]uint, 0, len(result.Items))
			for _, item := range result.Items {
				ids = append(ids, item.ID)
				require.Equal(t, tags.tags[item.ID], item.HashTags)
				require.NotEmpty(t, item.PlayURL)
				require.NotEmpty(t, item.CoverURL)
			}
			require.Equal(t, tc.wantIDs, ids, "成功项保持输入顺序，不能按 ID 排序")
			require.Len(t, result.Failures, len(tc.failedIndexes))
			for i, index := range tc.failedIndexes {
				require.Equal(t, index, result.Failures[i].Index)
				require.Equal(t, videos[index].ID, result.Failures[i].VideoID)
				require.Error(t, result.Failures[i].Err)
			}
			if len(tc.wantIDs) > 0 {
				require.Equal(t, [][]uint{tc.wantIDs}, tags.calls, "整批只查询一次成功视频的标签")
			}
			require.Equal(t, original, videos, "组装不应修改输入实体")
		})
	}
}

func TestVideoViewBuilderBuildSomeEmpty(t *testing.T) {
	for _, tc := range []struct {
		name   string
		videos []videomodel.Video
	}{
		{name: "nil"},
		{name: "empty_slice", videos: []videomodel.Video{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			signer := &viewTestSigner{}
			tags := &viewTestTags{}

			result := NewVideoViewBuilder(signer, tags).BuildSome(t.Context(), tc.videos)

			require.NotNil(t, result.Items)
			require.Empty(t, result.Items)
			require.NotNil(t, result.Failures)
			require.Empty(t, result.Failures)
			require.Empty(t, signer.calls)
			// 空 ID 是否提前返回由实现决定，但不得查询任何非空 ID。
			for _, ids := range tags.calls {
				require.Empty(t, ids)
			}
		})
	}
}

func TestVideoViewBuilderBuildSomeTagFailureKeepsVideos(t *testing.T) {
	signer := &viewTestSigner{fail: map[string]error{"videos/10.mp4": errors.New("signing failed")}}
	tags := &viewTestTags{err: errors.New("tag database unavailable")}
	input := []videomodel.Video{viewTestVideo(30), viewTestVideo(10), viewTestVideo(20)}

	result := NewVideoViewBuilder(signer, tags).BuildSome(t.Context(), input)

	require.Len(t, result.Items, 2)
	require.Equal(t, uint(30), result.Items[0].ID)
	require.Equal(t, uint(20), result.Items[1].ID)
	for _, item := range result.Items {
		require.Empty(t, item.HashTags)
		require.Equal(t, uint(5), item.LikedCount)
		require.Equal(t, uint(8), item.CommentCount)
		require.NotEmpty(t, item.PlayURL)
		require.NotEmpty(t, item.CoverURL)
	}
	require.Len(t, result.Failures, 1, "标签降级不能把成功视频计入失败项")
	require.Equal(t, 1, result.Failures[0].Index)
	require.Equal(t, uint(10), result.Failures[0].VideoID)
	require.Error(t, result.Failures[0].Err)
	require.Equal(t, [][]uint{{30, 20}}, tags.calls)
}
