// Package video 实现service契约的repo
package video

import (
	"context"
	"errors"
	"feedsystem/internal/data"
	"feedsystem/internal/model/feed"
	"feedsystem/internal/model/video"
	apperrors "feedsystem/internal/pkg/errors"
	"io"
	"net/http"
	"time"

	"github.com/minio/minio-go/v7"
	"gorm.io/gorm"
)

const (
	expireTimeUpLoad = 4 * time.Hour

	playExpiry = time.Hour
)

type videoRepo struct {
	mc *data.MinioClient
	db *gorm.DB
}

func NewVideoRepo(mc *data.MinioClient, db *gorm.DB) *videoRepo {
	return &videoRepo{
		mc: mc,
		db: db,
	}
}

func (r *videoRepo) Init(ctx context.Context, objectKey string, totalParts int) (uploadID string, urls []string, err error) {
	uploadID, err = r.mc.MultipartInit(ctx, objectKey)

	if err != nil {
		return "", nil, err
	}

	urls, err = r.mc.PresignPartURLs(ctx, objectKey, uploadID, totalParts, expireTimeUpLoad)
	if err != nil {
		return "", nil, err
	}

	return uploadID, urls, nil
}

func (r *videoRepo) GetLoadStatus(ctx context.Context, objectKey, uploadID string) (loadedParts []int, err error) {
	parts, err := r.mc.ListParts(ctx, objectKey, uploadID)
	if err != nil {
		return nil, err
	}

	loadedParts = make([]int, len(parts))
	for i, p := range parts {
		loadedParts[i] = p.PartNumber
	}
	return loadedParts, nil
}

// Complete 检查对应的对象是否上传完毕
// 1. 上传完毕，返回nil
// 2. 未上传完，返回错误并携带对应未上传分片数组
// 3. 其余错误，返回错误
func (r *videoRepo) Complete(ctx context.Context, objectKey, minioUploadID string, totalParts int) error {
	// 检查完成的分片数量
	loadedParts, err := r.mc.ListParts(ctx, objectKey, minioUploadID)
	if err != nil {
		return err //其余错误
	}

	allParts := make([]bool, totalParts)
	for _, p := range loadedParts {
		allParts[p.PartNumber-1] = true
	}

	var missingParts []int
	for i, p := range allParts {
		if !p {
			missingParts = append(missingParts, i+1)
		}
	}

	// 上传完毕
	if len(missingParts) == 0 {
		completeParts := make([]minio.CompletePart, len(loadedParts))
		for i, p := range loadedParts {
			completeParts[i] = minio.CompletePart{
				PartNumber: p.PartNumber,
				ETag:       p.ETag,
			}
		}
		if err := r.mc.CompleteMultipart(ctx, objectKey, minioUploadID, completeParts); err != nil {
			return err
		}
		return nil
	}

	// 未上传完
	if len(missingParts) > 0 {
		return &video.PartsIncompleteError{MissingParts: missingParts}
	}
	return nil
}

// Create 数据库中创建视频元数据记录
func (r *videoRepo) Create(ctx context.Context, v *video.Video) (*video.Video, error) {
	err := r.db.WithContext(ctx).Create(v).Error
	return v, err
}

// CreateWithOutbox 数据库中创建视频元数据记录并将发布信息放到outbox表
func (r *videoRepo) CreateWithOutbox(ctx context.Context, v *video.Video) (*video.Video, error) {
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 在事务中执行一些 db 操作（从这里开始，您应该使用 'tx' 而不是 'db'）
		if err := tx.Create(v).Error; err != nil {
			// 返回任何错误都会回滚事务
			return err
		}

		// 填写outboxMSg信息
		var m = &feed.OutboxMsg{
			VideoID:   v.ID,
			EventType: "publish",
			Status:    "pending",
		}
		if err := tx.Create(m).Error; err != nil {
			return err
		}

		// 填写tag 以及 videotag
		tags := video.ExtractTags(v.Description)
		if len(tags) > 0 {
			for _, t := range tags {
				tag := video.Tag{
					Name: t,
				}
				if err := tx.Where("name = ?", t).FirstOrCreate(&tag).Error; err != nil {
					return err
				}

				vTag := video.VideoTag{
					VideoID: v.ID,
					TagID:   tag.ID,
				}
				if err := tx.Create(&vTag).Error; err != nil {
					return err
				}
			}
		}
		// 返回 nil 提交事务
		return nil
	})

	return v, err
}

// UploadCover 上传照片数据到对象数据库
func (r *videoRepo) UploadCover(ctx context.Context, objectKey string, read io.Reader, size int64, contentType string) error {
	_, err := r.mc.PutObject(ctx, objectKey, read, size, contentType)
	return err
}

// PresignedGetObject 获取照片数据的预览链接
func (r *videoRepo) PresignedGetObject(ctx context.Context, objectKey string, expiry time.Duration) (string, error) {
	return r.mc.PresignedGetObject(ctx, objectKey, expiry)
}

// FindByID 查找视频信息
func (r *videoRepo) FindByID(ctx context.Context, id uint) (*video.Video, error) {
	if id == 0 {
		return nil, errors.New("invalid video id")
	}

	v := &video.Video{}
	res := r.db.WithContext(ctx).First(v, id)
	return v, res.Error
}

func (r *videoRepo) List(ctx context.Context, authorID uint, cursor *video.Cursor, limit int) ([]video.Video, error) {
	items := []video.Video{}
	q := r.db.Model(&video.Video{})
	if authorID != 0 {
		q = q.Where("author_id = ?", authorID)
	}
	if cursor != nil {
		q = q.Where("(created_at < ?) OR (created_at = ? AND id < ?)", cursor.CreatedAt, cursor.CreatedAt, cursor.ID)
	}
	res := q.Order("created_at DESC, id DESC").Limit(limit).Find(&items)
	return items, res.Error
}

// Delete 注意：实体带 DeletedAt，gorm .Delete 默认是软删；硬删必须 Unscoped
// 删除视频的时候，同步删除video_tag
func (r *videoRepo) Delete(ctx context.Context, id uint) error {
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("video_id = ?", id).Delete(&video.VideoTag{}).Error; err != nil {
			return err
		}
		return tx.Unscoped().Delete(&video.Video{}, id).Error
	})

	return err
}

// RemoveObject 删除视频对象以及对应的封面
func (r *videoRepo) RemoveObject(ctx context.Context, objectKey, coverKey string) error {
	err1 := r.mc.RemoveObject(ctx, objectKey)
	err2 := r.mc.RemoveObject(ctx, coverKey)
	if err1 != nil {
		return err1
	}
	return err2
}

// Abort 中断某次对象的上传，并删除对应的资源
func (r *videoRepo) Abort(ctx context.Context, objectKey, minioUploadID string) error {
	return r.mc.AbortMultipart(ctx, objectKey, minioUploadID)
}

// GetVideosByIDs 接受id数组，返回视频信息数组
func (r *videoRepo) GetVideosByIDs(ctx context.Context, ids []uint) ([]video.Video, error) {
	if len(ids) == 0 {
		return []video.Video{}, nil
	}

	var items []video.Video
	err := r.db.WithContext(ctx).Find(&items, "id IN (?)", ids).Error

	if err != nil {
		return nil, err
	}

	// 按照ids的顺序返回
	byID := make(map[uint]video.Video)
	for _, v := range items {
		byID[v.ID] = v
	}

	out := make([]video.Video, 0, len(ids))
	for _, id := range ids {
		v, ok := byID[id]
		if ok {
			out = append(out, v)
		}
	}

	return out, nil
}

// ListByTag 按照tagName 查找 tags 表里面tagID，再在video_tags 表里面统计涉及到的videoID，并按照cursor信息筛选videoID，最后按照顺序查找video
// select * from videos
// JOIN video_tags ON videos.id = video_tags.video_id
// JOIN tags ON tags.id = video_tags.tag_id)
// where tags.name = ? and ((videos.created_at < ?) OR (videos.created_at = ? AND videos.id < ?))
// orderby videos.created_at DESC,videos.id DESC
// limit ?
func (r *videoRepo) ListByTag(ctx context.Context, tagName string, cursor *video.Cursor, limit int) ([]video.Video, error) {
	var items []video.Video

	q := r.db.WithContext(ctx).Model(&video.Video{}).Joins("JOIN video_tags ON video_tags.video_id = videos.id").Joins("JOIN tags ON tags.id = video_tags.tag_id").Where("tags.name = ?", tagName)

	if cursor != nil {
		q = q.Where("(videos.created_at < ?) OR (videos.created_at = ? AND videos.id < ?)", cursor.CreatedAt, cursor.CreatedAt, cursor.ID)
	}

	err := q.Order("videos.created_at DESC,videos.id DESC").Limit(limit).Find(&items).Error
	return items, err
}

// GetVideoEntitiesByIDs 根据IDs 返回对应的videos
func (r *videoRepo) GetVideoEntitiesByIDs(ctx context.Context, ids []uint) ([]video.Video, error) {
	var videos []video.Video
	err := r.db.Model(&video.Video{}).Where("id in (?)", ids).Find(&videos).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return []video.Video{}, nil
		}
		return nil, err
	}
	return videos, nil
}

// BuildViews 签发视频播放url
func (r *videoRepo) BuildViews(ctx context.Context, vs []video.Video) ([]video.VideoView, error) {
	views := make([]video.VideoView, 0, len(vs))
	for i := range vs {
		v, err := r.signVideoView(ctx, &vs[i])
		if err != nil {
			return nil, err
		}
		views = append(views, *v)
	}
	return views, nil
}

func (r *videoRepo) signVideoView(ctx context.Context, v *video.Video) (*video.VideoView, error) {
	play, err := r.mc.PresignedGetObject(ctx, v.VideoKey, playExpiry)
	if err != nil {
		return nil, apperrors.NewAppError(http.StatusInternalServerError, "生成播放地址失败")
	}
	if v.CoverKey == "" {
		return nil, apperrors.NewAppError(http.StatusInternalServerError, "封面数据缺失")
	} // 强制封面
	cover, err := r.mc.PresignedGetObject(ctx, v.CoverKey, playExpiry)
	if err != nil {
		return nil, apperrors.NewAppError(http.StatusInternalServerError, "生成封面地址失败")
	}

	return &video.VideoView{
		ID:          v.ID,
		Title:       v.Title,
		Description: v.Description,
		Author: video.Author{
			ID:       v.AuthorID,
			Username: v.Username,
		},
		PlayURL:   play,
		CoverURL:  cover,
		CreatedAt: v.CreatedAt,
	}, nil

}
