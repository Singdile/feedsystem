package data

import (
	"bytes"
	"context"
	"feedsystem/internal/config"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestMinio(t *testing.T) *MinioClient {
	cfg := config.MinIOConfig{
		Endpoint:  "localhost:9000",
		AccessKey: "minioadmin",
		SecretKey: "minioadmin",
		Bucket:    "feedtest" + strconv.FormatInt(time.Now().UnixNano(), 36),
	}
	m, err := NewMinioClient(cfg)
	if err != nil {
		t.Skipf("minio 不可达，跳过集成测试: %v", err)
	} // 无容器时静默跳过

	t.Cleanup(func() {

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		_ = removeAllObjects(ctx, m)             //remove all objects
		_ = m.core().RemoveBucket(ctx, m.bucket) // then you can remove bucket
	},
	) // 尽力清理
	return m
}

func getViaHTTP(t *testing.T, url string) []byte {
	t.Helper()
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return b
}

func putViaHTTP(t *testing.T, url string, body []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// 分页列出桶内全部对象 key
func listObjectKeys(ctx context.Context, m *MinioClient) ([]string, error) {
	core := m.core()
	var keys []string
	token := ""
	for {
		res, err := core.ListObjectsV2(m.bucket, "", "", token, "", 1000)
		if err != nil {
			return nil, err
		}
		for _, o := range res.Contents {
			keys = append(keys, o.Key)
		}
		if !res.IsTruncated {
			break
		}
		token = res.NextContinuationToken
	}
	return keys, nil
}

// 清空桶内所有对象（供测试 cleanup 使用）
func removeAllObjects(ctx context.Context, m *MinioClient) error {
	keys, err := listObjectKeys(ctx, m)
	if err != nil {
		return err
	}
	for _, k := range keys {
		if err := m.core().RemoveObject(ctx, m.bucket, k, minio.RemoveObjectOptions{}); err != nil {
			return err
		}
	}
	return nil
}

// TestMinioPutGet test you can really put and get object
func TestMinioPutGet(t *testing.T) {
	m := newTestMinio(t)

	want := []byte("hello world")
	fp := bytes.NewReader(want)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	key := "/hello.txt"

	_, err := m.PutObject(ctx, key, fp, int64(len(want)), "")
	require.NoError(t, err)

	urlstr, err := m.PresignedGetObject(ctx, key, time.Duration(10)*time.Minute)
	require.NoError(t, err)

	got := getViaHTTP(t, urlstr)
	assert.Equal(t, want, got)
}

// TestMinio_MultipartDirectParts 手动模拟一个分片的上传和下载操作
func TestMinio_MultipartDirectParts(t *testing.T) {
	m := newTestMinio(t)
	ctx := context.Background()

	part1 := bytes.Repeat([]byte{0xAB}, 5<<20) // 先构造 5MB
	part2 := []byte("tail-bytes")              // 末尾的
	key := "it/mp-" + strconv.FormatInt(time.Now().UnixNano(), 36) + "/v.mp4"

	uploadID, err := m.MultipartInit(ctx, key) //申请uploadID
	require.NoError(t, err)

	urls, err := m.PresignPartURLs(ctx, key, uploadID, 2, 4*time.Hour) //告诉minio要上传多少分片，获取可以写url地址
	require.NoError(t, err)
	require.Len(t, urls, 2)

	putViaHTTP(t, urls[1], part2) // 故意先传第 2 片 → 验证乱序
	putViaHTTP(t, urls[0], part1) // 再传第 1 片（≥5MB）

	parts, err := m.ListParts(ctx, key, uploadID) //列出已经上传的分片
	require.NoError(t, err)
	require.Len(t, parts, 2)
	assert.Equal(t, 1, parts[0].PartNumber)
	assert.Equal(t, int64(5<<20), parts[0].Size) // 第1片大小
	assert.Equal(t, int64(len(part2)), parts[1].Size)

	cp := []minio.CompletePart{
		{PartNumber: parts[0].PartNumber, ETag: parts[0].ETag},
		{PartNumber: parts[1].PartNumber, ETag: parts[1].ETag},
	}
	require.NoError(t, m.CompleteMultipart(ctx, key, uploadID, cp)) // minio 组装分片，彻底落库

	u, err := m.PresignedGetObject(ctx, key, time.Minute) // 下载落库数据
	require.NoError(t, err)
	got := getViaHTTP(t, u)
	assert.Equal(t, append(part1, part2...), got) // 组装结果 = 两片拼接
}

func TestMinio_MultipartAbort(t *testing.T) {
	m := newTestMinio(t)
	ctx := context.Background()
	key := "it/abort-" + strconv.FormatInt(time.Now().UnixNano(), 36) + "/v.mp4"

	uploadID, err := m.MultipartInit(ctx, key)
	require.NoError(t, err)
	urls, err := m.PresignPartURLs(ctx, key, uploadID, 2, 4*time.Hour)
	require.NoError(t, err)

	putViaHTTP(t, urls[0], bytes.Repeat([]byte{0x11}, 5<<20)) // 只传 1 片
	parts, err := m.ListParts(ctx, key, uploadID)
	require.NoError(t, err)
	require.Len(t, parts, 1)

	require.NoError(t, m.AbortMultipart(ctx, key, uploadID))
	_, err = m.ListParts(ctx, key, uploadID) // abort 后应报错(NoSuchUpload)
	assert.Error(t, err)
}
