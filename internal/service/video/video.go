// Package videosvc 提供视频上传下载服务
package videosvc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"feedsystem/internal/model/video"
	apperrors "feedsystem/internal/pkg/errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"gorm.io/gorm"
)

const (
	partSize        int64 = 5 << 20
	maxFileSize     int64 = 2 << 30
	sessionTTL            = 24 * time.Hour
	coverMax        int64 = 10 << 20         // 图片最大上传10MB
	coverPreviewTTL       = 15 * time.Minute // 图片预览链接有效时长
	playExpiry            = time.Hour
)

// VideoDB 数据库存储操作
type VideoDB interface {
	Create(ctx context.Context, v *video.Video) (*video.Video, error)
	FindByID(ctx context.Context, id uint) (*video.Video, error)
	List(ctx context.Context, authorID uint, cursor *video.Cursor, limit int) ([]video.Video, error)
	Delete(ctx context.Context, id uint) error
	RemoveObject(ctx context.Context, videoKey, coverKey string) error
}

// ObjectStore 对象存储操作
type ObjectStore interface {
	Init(ctx context.Context, objectKey string, totalParts int) (uploadID string, urls []string, err error)
	GetLoadStatus(ctx context.Context, objectKey, uploadID string) (loadedParts []int, err error)
	Complete(ctx context.Context, objectKey, minioUploadID string, totalParts int) error
	UploadCover(ctx context.Context, objectKey string, read io.Reader, size int64, contentType string) error
	PresignedGetObject(ctx context.Context, objectKey string, expiry time.Duration) (string, error)
}

// VideoRepo 组合接口：service 依赖它一个即可
type VideoRepo interface {
	ObjectStore
	VideoDB
}

type VideoService struct {
	repo  VideoRepo
	cache ChunkCache
}

func NewVideoService(repo VideoRepo, cache ChunkCache) *VideoService {
	return &VideoService{repo: repo, cache: cache}
}

type ChunkCache interface {
	Key(format string, a ...any) string
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key string, val any, ttl time.Duration) (string, error)
	Del(ctx context.Context, key string) error
}

// chunkSession 保存单次上传文件的会话信息。存储到cache,用户通过chunk:resume:{AuthorID}:{FileHash} 查找到该会话对应的 uploadID
// 再从 cache 中使用  chunk:session:{uploadID} 查找到详细的 会话信息
type chunkSession struct {
	AuthorID      uint
	MinioUploadID string
	VideoKey      string
	TotalParts    int
	FileSize      int64
	FileHash      string
}

// InitResult service 领域结果（不带 HTTP 标签），handler 据此组装 InitResp
type InitResult struct {
	UploadID string
	PartURLs []string
}

// Init 接受一个上传资源的请求，返回分片上传地址
func (s *VideoService) Init(ctx context.Context, authorID uint, fileName string, fileSize int64, filehash string) (*InitResult, error) {
	// check param
	if fileName == "" || fileSize <= 0 || fileSize > maxFileSize || filehash == "" {
		return nil, apperrors.NewAppError(http.StatusBadRequest, "invalid parameter")
	}

	uploadID := randHex(16)                              // 我们的会话号，同时作为 MinIO 对象文件名
	totalParts := (fileSize + (partSize - 1)) / partSize // ceil
	// 对象存储的实际的key
	objectKey := fmt.Sprintf("videos/%d/%s/%s.mp4", authorID, time.Now().Format("20060102"), uploadID)
	minioUpLoadID, urls, err := s.repo.Init(ctx, objectKey, int(totalParts))
	if err != nil {
		return nil, err
	}

	// cache
	uploadKey := fmt.Sprintf("chunk:resume:%d:%s", authorID, filehash) //chunk:resume:{AuthorID}:{FileHash}
	if _, err = s.cache.Set(ctx, uploadKey, uploadID, sessionTTL); err != nil {
		return nil, err
	}

	sessionKey := fmt.Sprintf("chunk:session:%s", uploadID) //chunk:session:{uploadID}
	sess := chunkSession{
		AuthorID:      authorID,
		MinioUploadID: minioUpLoadID,
		VideoKey:      objectKey,
		TotalParts:    int(totalParts),
		FileSize:      fileSize,
		FileHash:      filehash,
	}
	sessJSON, err := json.Marshal(sess)
	if err != nil {
		return nil, err
	}
	if _, err = s.cache.Set(ctx, sessionKey, string(sessJSON), sessionTTL); err != nil {
		return nil, err
	}

	return &InitResult{
		UploadID: uploadID,
		PartURLs: urls,
	}, nil

}

// randHex 生成随机的会话号,n表示多少个字节，返回对应的16进制字符串。一个字节可以表达2个16进制数，所以就是返回2*n位16进制数
func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// GetLoadStatus get a Load session's situation. return loaded parts and total parts number
func (s *VideoService) GetLoadStatus(ctx context.Context, uploadID string, authorID uint) (uploadedParts []int, totalParts int, err error) {
	// checkSession 查找session记录并检查是否是当前用户的
	session, err := s.checkSession(ctx, uploadID, authorID)
	if err != nil {
		return nil, 0, err
	}

	uploadedParts, err = s.repo.GetLoadStatus(ctx, session.VideoKey, session.MinioUploadID)
	if err != nil {
		return nil, 0, apperrors.NewAppError(http.StatusInternalServerError, "查询分片失败")
	}

	return uploadedParts, session.TotalParts, nil
}

// CompleteUpload 完成对应任务的拼装。
// 成功，返回 videoKey,nil
// 失败，
// 1. 返回 "",error ，并携带未上传的分片数组
// 2. 返回 "",error ，内部发生错误
func (s *VideoService) CompleteUpload(ctx context.Context, authorID uint, uploadID string) (string, error) {
	// checkSession 查找session记录并检查是否是当前用户的
	session, err := s.checkSession(ctx, uploadID, authorID)
	if err != nil {
		return "", err
	}
	// 调用repo完成拼装
	if err := s.repo.Complete(ctx, session.VideoKey, session.MinioUploadID, session.TotalParts); err != nil {
		return "", err
	}
	// 检查repo拼装结果，成功直接返回; 失败，error携带未上传的分片数据或者内部错误返回
	// 删除session记录
	if err := s.deleteSession(ctx, uploadID, session); err != nil {
		log.Printf("delete session err: %v", err)
	}
	return session.VideoKey, nil
}

// checkSession 查找session记录并检查是否是当前用户的
// 成功，返回session记录，nil
// 失败，返回nil,err
func (s *VideoService) checkSession(ctx context.Context, uploadID string, authorID uint) (*chunkSession, error) {
	if uploadID == "" {
		return nil, apperrors.NewAppError(http.StatusBadRequest, "参数错误")
	}

	// 从cache中按到会话信息，从而获取minioUPloadID
	sess, err := s.cache.Get(ctx, s.cache.Key("chunk:session:%s", uploadID))
	if err != nil {
		return nil, apperrors.NewAppError(http.StatusNotFound, "上传会话不存在或已过期")
	}
	var session chunkSession
	if err := json.Unmarshal([]byte(sess), &session); err != nil {
		return nil, apperrors.NewAppError(http.StatusInternalServerError, "会话数据异常")
	}

	// 检查是否是对应的作者
	if session.AuthorID != authorID {
		return nil, apperrors.NewAppError(http.StatusForbidden, "无权访问该上传")
	}

	return &session, nil
}

// deleteSession 删除session记录
func (s *VideoService) deleteSession(ctx context.Context, uploadID string, session *chunkSession) error {
	if err := s.cache.Del(ctx, s.cache.Key("chunk:session:%s", uploadID)); err != nil {
		return err
	}

	if err := s.cache.Del(ctx, s.cache.Key("chunk:resume:%d:%s", session.AuthorID, session.FileHash)); err != nil {
		return err
	}
	return nil
}

// Publish 提交video的元数据到数据库
func (s *VideoService) Publish(ctx context.Context, video *video.Video) (*video.Video, error) {
	if video == nil {
		return nil, apperrors.NewAppError(http.StatusBadRequest, "参数有问题")
	}

	if strings.TrimSpace(video.Title) == "" {
		return nil, apperrors.NewAppError(http.StatusBadRequest, "标题不能为空")
	}

	if !strings.HasPrefix(video.VideoKey, fmt.Sprintf("videos/%d/", video.AuthorID)) {
		return nil, apperrors.NewAppError(http.StatusBadRequest, "video_key 归属不符") // 归属业务规则
	}

	if !strings.HasPrefix(video.CoverKey, fmt.Sprintf("covers/%d/", video.AuthorID)) {
		return nil, apperrors.NewAppError(http.StatusBadRequest, "cover_key 归属不符")
	}

	return s.repo.Create(ctx, video)
}

// UploadCover 提交图片数据到对象数据库中，返回对象键以及预览地址
func (s *VideoService) UploadCover(ctx context.Context, authorID uint, filename string, filesize int64, content io.Reader) (objectKey string, url string, err error) {
	// 检测文件后缀是否符合条件
	ext := strings.ToLower(filepath.Ext(filename))
	var contentType string
	switch ext {
	case ".jpg", ".jpeg":
		contentType = "image/jpeg"
	case ".png", ".webp":
		contentType = "image/" + strings.TrimPrefix(ext, ".")
	default:
		return "", "", apperrors.NewAppError(http.StatusBadRequest, "仅支持 jpg/jpeg/png/webp")
	}
	// 判断文件大小是否符合条件
	if filesize <= 0 || int64(filesize) > coverMax {
		return "", "", apperrors.NewAppError(http.StatusBadRequest, "图片过大，请限制到10MB")
	}
	// 生成objectKey: covers/authorID/time/16位的随机串.png(/.jpg/.jpeg/.webp)
	objectKey = fmt.Sprintf("covers/%d/%s/%s", authorID, time.Now().Format("20060102"), randHex(16)+ext)

	// 执行对象存储操作，
	err = s.repo.UploadCover(ctx, objectKey, content, filesize, contentType)
	if err != nil {
		return "", "", err
	}
	// 返回预览地址
	url, err = s.repo.PresignedGetObject(ctx, objectKey, coverPreviewTTL)
	if err != nil {
		return "", "", err
	}
	// 返回
	return objectKey, url, nil
}

// 抽公共方法（GetVideo 也改用它）
func (s *VideoService) videoView(ctx context.Context, v *video.Video) (*video.VideoView, error) {
	play, err := s.repo.PresignedGetObject(ctx, v.VideoKey, playExpiry)
	if err != nil {
		return nil, apperrors.NewAppError(http.StatusInternalServerError, "生成播放地址失败")
	}
	if v.CoverKey == "" {
		return nil, apperrors.NewAppError(http.StatusInternalServerError, "封面数据缺失")
	} // 强制封面
	cover, err := s.repo.PresignedGetObject(ctx, v.CoverKey, playExpiry)
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

// GetVideo 根据数据库中的视频ID,返回该视频的播放信息
func (s *VideoService) GetVideo(ctx context.Context, id uint) (*video.VideoView, error) {
	if id == 0 {
		return nil, apperrors.NewAppError(http.StatusBadRequest, "参数错误")
	}

	got, err := s.repo.FindByID(ctx, id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, apperrors.NewAppError(http.StatusNotFound, "视频不存在")
		}
		return nil, apperrors.NewAppError(http.StatusInternalServerError, "查询视频失败")
	}

	return s.videoView(ctx, got)
}

// ListResult 分页结果
type ListResult struct {
	Items      []*video.VideoView `json:"items"`
	NextCursor string             `json:"next_cursor"` // 空串 = 没有更多
}

// ListVideos 根据用户id,分页游标，limit返回视频信息列表
func (s *VideoService) ListVideos(ctx context.Context, authorID uint, cursorStr string, limit int) (*ListResult, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	// 解析cursorStr
	var cursor *video.Cursor
	if cursorStr != "" {
		c, err := video.DecodeCursor(cursorStr)
		if err != nil {
			return nil, apperrors.NewAppError(http.StatusBadRequest, "游标无效")
		}
		cursor = &c
	}

	// 调用repo
	v, err := s.repo.List(ctx, authorID, cursor, limit+1) //多取一条，判断是否有下一页
	if err != nil {
		return nil, apperrors.NewAppError(http.StatusInternalServerError, "查询列表失败")
	}

	hasmore := len(v) > limit
	if hasmore {
		v = v[:limit]
	}

	nextCursorStr := ""
	if len(v) > 0 && hasmore {
		next := video.Cursor{
			CreatedAt: v[len(v)-1].CreatedAt,
			ID:        v[len(v)-1].ID,
		}
		nextCursorStr = video.EncodeCursor(next)
	}

	// 构造videoView列表返回
	videoViews := make([]*video.VideoView, len(v))
	for i := range v {
		videoViews[i], err = s.videoView(ctx, &v[i])
		if err != nil {
			return nil, err
		}
	}

	return &ListResult{
		Items:      videoViews,
		NextCursor: nextCursorStr,
	}, nil
}

func (s *VideoService) DeleteVideo(ctx context.Context, id uint, authorID uint) error {
	if id <= 0 {
		return apperrors.NewAppError(http.StatusBadRequest, "参数错误")
	}

	// 验证该视频是否是该用户的
	v, err := s.repo.FindByID(ctx, id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return apperrors.NewAppError(http.StatusNotFound, "视频不存在")
		}
		return apperrors.NewAppError(http.StatusInternalServerError, "查询视频失败")
	}

	if v.AuthorID != authorID {
		return apperrors.NewAppError(http.StatusUnauthorized, "无权限进行该操作")
	}

	// 进行数据删除操作
	err = s.repo.Delete(ctx, id)
	if err != nil {
		log.Printf("删除视频记录失败，用户:%v，视频ID:%v", authorID, id)
		return apperrors.NewAppError(http.StatusInternalServerError, "内部错误，删除失败")
	}

	// 进行对象数据删除操作,尽力删除
	err = s.repo.RemoveObject(ctx, v.VideoKey, v.CoverKey)
	if err != nil {
		log.Printf("删除视频对象失败,用户:%v,视频key:%v,err:%v", authorID, v.VideoKey, err)
	}
	return nil
}
