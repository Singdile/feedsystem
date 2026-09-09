// Package video 实现service契约的repo
package video

import (
	"context"
	"errors"
	"feedsystem/internal/data"
	"feedsystem/internal/model/video"
	"io"
	"time"

	"github.com/minio/minio-go/v7"
	"gorm.io/gorm"
)

const (
	expireTimeUpLoad = 4 * time.Hour
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
func (r *videoRepo) Delete(ctx context.Context, id uint) error {
	return r.db.WithContext(ctx).Unscoped().Delete(&video.Video{}, id).Error
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
