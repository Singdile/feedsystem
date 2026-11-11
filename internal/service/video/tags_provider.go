package video

import (
	"context"
	"feedsystem/internal/model/video"
	"log"
)

type VideoTagReader interface {
	GetTagIDsByVideoIDs(ctx context.Context, ids []uint) (map[uint][]uint, error)
}

type TagProvider struct {
	videoTags VideoTagReader
	tagNames  *TagNameCache
}

func NewTagProvider(videoTags VideoTagReader, tagNames *TagNameCache) *TagProvider {
	return &TagProvider{
		videoTags: videoTags,
		tagNames:  tagNames,
	}
}

func (p *TagProvider) GetTags(ctx context.Context, videos []video.Video) (map[uint][]string, error) {
	if len(videos) == 0 {
		return map[uint][]string{}, nil
	}

	missing := make([]uint, 0, len(videos))
	for i := range videos {
		if videos[i].TagIDs == nil {
			missing = append(missing, videos[i].ID)
		}
	}

	// TagIDs == nil 的视频补充一次 DB 查询对应的 tag id
	if len(missing) > 0 {
		tagIDs, err := p.videoTags.GetTagIDsByVideoIDs(ctx, missing)
		if err != nil {
			log.Printf("DB查询视频tag 失败,videos=%d,err=%v", len(missing), err)
		} else {
			for i := range videos {
				if videos[i].TagIDs == nil {
					videos[i].TagIDs = tagIDs[videos[i].ID]
					if videos[i].TagIDs == nil {
						videos[i].TagIDs = []uint{}
					}
				}
			}
		}

	}

	// 收集所有的 tagID,去重
	set := make(map[uint]struct{})

	for _, v := range videos {
		for _, id := range v.TagIDs {
			set[id] = struct{}{}
		}
	}

	ids := make([]uint, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}

	// 本地 tag 表解析
	names, err := p.tagNames.GetNames(ctx, ids)
	if err != nil {
		return nil, err
	}

	// 组装
	out := make(map[uint][]string, len(videos))
	for _, v := range videos {
		tags := make([]string, 0, len(v.TagIDs))
		for _, id := range v.TagIDs {
			if name, ok := names[id]; ok {
				tags = append(tags, name)
			}
		}

		out[v.ID] = tags
	}

	return out, nil
}
