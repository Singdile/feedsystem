package video

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"feedsystem/internal/model/video"
	"github.com/stretchr/testify/require"
)

// videoTagReaderStub 记录 video_tags 的批量查询参数。
type videoTagReaderStub struct {
	mu    sync.Mutex
	calls [][]uint
	tags  map[uint][]uint
	err   error
}

func (s *videoTagReaderStub) GetTagIDsByVideoIDs(_ context.Context, ids []uint) (map[uint][]uint, error) {
	s.mu.Lock()
	s.calls = append(s.calls, append([]uint(nil), ids...))
	tags, err := s.tags, s.err
	s.mu.Unlock()

	if err != nil {
		return nil, err
	}

	out := make(map[uint][]uint, len(ids))
	for _, id := range ids {
		if v, ok := tags[id]; ok {
			out[id] = append([]uint{}, v...)
		} else {
			out[id] = []uint{}
		}
	}
	return out, nil
}

func (s *videoTagReaderStub) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func (s *videoTagReaderStub) calledIDs() [][]uint {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]uint(nil), s.calls...)
}

func newTagProviderForTest(names map[uint]string, tags *videoTagReaderStub) *TagProvider {
	cache := NewTagNameCache(&tagNameReaderStub{names: names}, time.Minute)
	return NewTagProvider(tags, cache)
}

func TestTagProviderNoFallbackWhenTagIDsPresent(t *testing.T) {
	tags := &videoTagReaderStub{}
	provider := newTagProviderForTest(map[uint]string{1: "go", 2: "redis"}, tags)

	videos := []video.Video{
		{ID: 10, TagIDs: []uint{1, 2}},
		{ID: 20, TagIDs: []uint{}},
	}

	out, err := provider.GetTags(t.Context(), videos)

	require.NoError(t, err)
	require.Equal(t, []string{"go", "redis"}, out[10])
	require.Empty(t, out[20])
	require.Equal(t, 0, tags.callCount(), "TagIDs 已就绪时不应查询 video_tags")
}

func TestTagProviderFillsMissingTagIDs(t *testing.T) {
	tags := &videoTagReaderStub{tags: map[uint][]uint{10: {1, 2}}}
	provider := newTagProviderForTest(map[uint]string{
		1: "go",
		2: "redis",
		3: "mysql",
	}, tags)

	videos := []video.Video{
		{ID: 10, TagIDs: nil},
		{ID: 20, TagIDs: []uint{3}},
	}

	out, err := provider.GetTags(t.Context(), videos)

	require.NoError(t, err)
	require.Equal(t, []string{"go", "redis"}, out[10])
	require.Equal(t, []string{"mysql"}, out[20])
	require.Equal(t, [][]uint{{10}}, tags.calledIDs(), "只查询 TagIDs 为 nil 的视频")
	require.Equal(t, []uint{1, 2}, videos[0].TagIDs, "补齐后应回填到原实体")
}

func TestTagProviderEmptyVideos(t *testing.T) {
	tags := &videoTagReaderStub{}
	provider := newTagProviderForTest(nil, tags)

	out, err := provider.GetTags(t.Context(), nil)

	require.NoError(t, err)
	require.NotNil(t, out)
	require.Empty(t, out)
	require.Equal(t, 0, tags.callCount())
}

func TestTagProviderMissingNamesOmitted(t *testing.T) {
	tags := &videoTagReaderStub{}
	provider := newTagProviderForTest(map[uint]string{1: "go"}, tags)

	videos := []video.Video{{ID: 10, TagIDs: []uint{1, 2}}}

	out, err := provider.GetTags(t.Context(), videos)

	require.NoError(t, err)
	require.Equal(t, []string{"go"}, out[10], "本地 tag 表缺失的名字直接跳过")
}

func TestTagProviderVideoTagsErrorGraceful(t *testing.T) {
	tags := &videoTagReaderStub{err: errors.New("video_tags unavailable")}
	provider := newTagProviderForTest(map[uint]string{1: "go"}, tags)

	videos := []video.Video{
		{ID: 10, TagIDs: nil},
		{ID: 20, TagIDs: []uint{1}},
	}

	out, err := provider.GetTags(t.Context(), videos)

	require.NoError(t, err, "标签是辅助信息，查询失败不应中断 Feed")
	require.Empty(t, out[10])
	require.Equal(t, []string{"go"}, out[20], "已加载 TagIDs 的视频仍应返回标签")
}

func TestTagProviderDeduplicatesTagIDs(t *testing.T) {
	tags := &videoTagReaderStub{}
	provider := newTagProviderForTest(map[uint]string{1: "go", 2: "redis"}, tags)

	videos := []video.Video{
		{ID: 10, TagIDs: []uint{1, 2}},
		{ID: 20, TagIDs: []uint{1}},
	}

	out, err := provider.GetTags(t.Context(), videos)

	require.NoError(t, err)
	require.Equal(t, []string{"go", "redis"}, out[10])
	require.Equal(t, []string{"go"}, out[20])
	require.Equal(t, 0, tags.callCount())
}
