package data

import (
	"feedsystem/internal/config"

	"github.com/minio/minio-go/v7"
)

type MinioClient struct {
	client *minio.Client
	bucket string
}

func NewMinioClient(conf config.MinIOConfig) (*MinioClient, error) {
	client, err := minio.New(conf.Endpoint, &minio.Options{})
	return &MinioClient{
		client: client,
		bucket: conf.Bucket,
	}, err
}
