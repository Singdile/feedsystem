package video

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gorm.io/gorm"
)

// Video 视频实体
type Video struct {
	ID          uint           `gorm:"primaryKey" json:"id"`
	AuthorID    uint           `gorm:"index;not null" json:"author_id"`
	Username    string         `gorm:"type:varchar(255);not null" json:"username"`
	Title       string         `gorm:"type:varchar(255);not null" json:"title"`
	Description string         `gorm:"type:varchar(1000);default:''" json:"description,omitempty"`
	VideoKey    string         `gorm:"type:varchar(255);not null" json:"-"` // 视频
	CoverKey    string         `gorm:"type:varchar(255);not null" json:"-"` // 封面
	CreatedAt   time.Time      `gorm:"autoCreateTime" json:"created_at"`
	DeletedAt   gorm.DeletedAt `gorm:"index" json:"-"`
}

// InitReq 上传视频请求参数
type InitReq struct {
	FileName string `json:"file_name" binding:"required"`
	FileSize int64  `json:"file_size" binding:"required,min=1"`
	FileHash string `json:"file_hash" binding:"required"`
}

// InitResp 上传开档响应（video_key 由 complete 阶段返回）
type InitResp struct {
	UploadID string   `json:"upload_id"`
	PartURLs []string `json:"part_urls"`
}

type GetLoadStatusResp struct {
	UploadedParts []int `json:"uploaded_parts"`
	TotalParts    int   `json:"total_parts"`
}

// CompleteResp 拼装成功响应
type CompleteResp struct {
	VideoKey string `json:"video_key"`
}

// PartsIncompleteError 分片未全部上传（repo 返回，service/handler 识别后渲染 400）
type PartsIncompleteError struct {
	MissingParts []int
}

func (e *PartsIncompleteError) Error() string { return "分片未全部上传" }

type PublishReq struct {
	Title       string `json:"title" binding:"required"`
	Description string `json:"description,omitempty"`
	VideoKey    string `json:"video_key" binding:"required"` // 视频
	CoverKey    string `json:"cover_key" binding:"required"` // 封面
}

// Author 作者信息
type Author struct {
	ID       uint   `json:"id"`
	Username string `json:"username"`
}

type PublishResp struct {
	ID          uint      `json:"id"`
	Title       string    `json:"title"`
	Description string    `json:"description,omitempty"`
	Author      Author    `json:"author"`
	VideoKey    string    `json:"video_key"`
	CoverKey    string    `json:"cover_key,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

type CoverResp struct {
	CoverKey   string `json:"cover_key"`
	PreviewURL string `json:"preview_url"`
}

type VideoView struct {
	ID          uint      `json:"id"`
	Title       string    `json:"title"`
	Description string    `json:"description,omitempty"`
	Author      Author    `json:"author"`
	PlayURL     string    `json:"play_url"`
	CoverURL    string    `json:"cover_url,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

// Cursor 游标：上一页最后一条的 (created_at, id)
type Cursor struct {
	CreatedAt time.Time
	ID        uint
}

// EncodeCursor 采用base64编码，每6bit映射为对应的字符
func EncodeCursor(cursor Cursor) string {
	return base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("%d,%d", cursor.CreatedAt.UnixMilli(), cursor.ID)))
}

// DecodeCursor 解析cursorStr为cursor
func DecodeCursor(cursorStr string) (Cursor, error) {
	b, err := base64.RawURLEncoding.DecodeString(cursorStr)
	if err != nil {
		return Cursor{}, err
	}
	parts := strings.SplitN(string(b), ",", 2)
	if len(parts) != 2 {
		return Cursor{}, errors.New("Invalid cursor")
	}
	ms, _ := strconv.ParseInt(parts[0], 10, 64)
	id, _ := strconv.ParseUint(parts[1], 10, 64)
	if ms == 0 || id == 0 {
		return Cursor{}, errors.New("Invalid cursor")
	}
	return Cursor{
		CreatedAt: time.UnixMilli(ms),
		ID:        uint(id),
	}, nil
}
