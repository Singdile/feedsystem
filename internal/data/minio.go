package data

import (
	"context"
	"feedsystem/internal/config"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type MinioClient struct {
	client *minio.Client
	bucket string
}

func NewMinioClient(conf config.MinIOConfig) (*MinioClient, error) {
	client, err := minio.New(conf.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(conf.AccessKey, conf.SecretKey, ""),
		Secure: conf.UseSSL,
	})

	if err != nil {
		return nil, err
	}

	// 测试是否存在bucket
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	exists, err := client.BucketExists(ctx, conf.Bucket)

	if err != nil {
		return nil, err
	}

	if !exists {
		if err = client.MakeBucket(ctx, conf.Bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, err
		}
	}

	return &MinioClient{
		client: client,
		bucket: conf.Bucket,
	}, err
}

func (m *MinioClient) core() *minio.Core {
	return &minio.Core{Client: m.client}
}

// PutObject upload file to minio
func (m *MinioClient) PutObject(ctx context.Context, key string, r io.Reader, size int64, contentType string) (minio.UploadInfo, error) {
	return m.client.PutObject(ctx, m.bucket, key, r, size, minio.PutObjectOptions{ContentType: contentType})
}

// PresignedGetObject return a download link, the stream doesn't affect server
func (m *MinioClient) PresignedGetObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	url, err := m.client.PresignedGetObject(ctx, m.bucket, key, expiration, nil)
	if err != nil {
		return "", err
	}

	return url.String(), nil
}

// MultipartInit 开 multipart，返回 MinIO uploadID
func (m *MinioClient) MultipartInit(ctx context.Context, objectKey string) (uploadID string, err error) {
	core := m.core()
	return core.NewMultipartUpload(ctx, m.bucket, objectKey, minio.PutObjectOptions{})
}

// PresignPartURLs  totalParts 每片签发 presigned PUT（reqParams: partNumber + uploadId，expiry=4h）
func (m *MinioClient) PresignPartURLs(ctx context.Context, objectKey, uploadID string, totalParts int, expiry time.Duration) ([]string, error) {
	urls := make([]string, 0, totalParts)
	for partNumber := 1; partNumber <= totalParts; partNumber++ {
		u, err := m.client.Presign(ctx, http.MethodPut, m.bucket, objectKey, expiry, url.Values{
			"partNumber": {strconv.Itoa(partNumber)},
			"uploadId":   {uploadID},
		})
		if err != nil {
			return nil, err
		}
		urls = append(urls, u.String())
	}
	return urls, nil
}

// ListParts 列出已上传 parts（含 ETag/Size），供 status 与 complete 用
func (m *MinioClient) ListParts(ctx context.Context, objectKey, uploadID string) ([]minio.ObjectPart, error) {
	core := m.core()
	marker := 0
	all := []minio.ObjectPart{}
	for {
		res, err := core.ListObjectParts(ctx, m.bucket, objectKey, uploadID, marker, 1000)
		if err != nil {
			return nil, err
		}
		all = append(all, res.ObjectParts...)
		if !res.IsTruncated {
			break
		}
		marker = res.NextPartNumberMarker
	}

	return all, nil
}

// CompleteMultipart 按 parts 列表完成组装（MinIO 存储端拼接）
func (m *MinioClient) CompleteMultipart(ctx context.Context, objectKey, uploadID string, parts []minio.CompletePart) error {
	core := m.core()
	_, err := core.CompleteMultipartUpload(ctx, m.bucket, objectKey, uploadID, parts, minio.PutObjectOptions{})
	return err
}

// AbortMultipart 取消上传
func (m *MinioClient) AbortMultipart(ctx context.Context, objectKey, uploadID string) error {
	return m.core().AbortMultipartUpload(ctx, m.bucket, objectKey, uploadID)
}
