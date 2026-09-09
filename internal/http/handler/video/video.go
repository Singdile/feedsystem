// Package video 负责处理视频上传与下载
package video

import (
	"errors"
	"feedsystem/internal/http/response"
	"feedsystem/internal/model/video"
	videosvc "feedsystem/internal/service/video"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	svc *videosvc.VideoService
}

func NewHandler(svc *videosvc.VideoService) *Handler {
	return &Handler{svc: svc}
}

// Init 视频上传登记，获取uploadID，以及分片上传urls
func (h *Handler) Init(c *gin.Context) {
	var req video.InitReq
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}

	authorID, exist := c.Get("user_id")
	if !exist {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}
	author_id, ok := authorID.(uint)
	if !ok {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}

	res, err := h.svc.Init(c.Request.Context(), author_id, req.FileName, req.FileSize, req.FileHash)
	if err != nil {
		response.FromError(c, err)
		return
	}

	response.OK(c, video.InitResp{
		UploadID: res.UploadID,
		PartURLs: res.PartURLs,
	})
}

// GetLoadStatus 获取视频已经上传的分片数据
func (h *Handler) GetLoadStatus(c *gin.Context) {
	uploadID := c.Param("uploadID")
	if uploadID == "" {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}

	authorID, exist := c.Get("user_id")
	if !exist {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}
	authorId, ok := authorID.(uint)
	if !ok {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}

	uploadedParts, totalParts, err := h.svc.GetLoadStatus(c.Request.Context(), uploadID, authorId)

	if err != nil {
		response.FromError(c, err)
		return
	}
	response.OK(c, video.GetLoadStatusResp{
		UploadedParts: uploadedParts,
		TotalParts:    totalParts,
	})

}

// CompleteUpload 视频上传完成，通知minio拼装落库.
// 成功返回对应的Key.
// 失败.
//
//	1.分片未齐，返回缺失分片
//	2.其他错误，返回500
func (h *Handler) CompleteUpload(c *gin.Context) {
	// 读取uploadID 和 user_id
	uploadID := c.Param("uploadID")
	if uploadID == "" {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}

	authorID, exist := c.Get("user_id")
	if !exist {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}
	authorId, ok := authorID.(uint)
	if !ok {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}

	// 通知minio拼装落库
	videoKey, err := h.svc.CompleteUpload(c.Request.Context(), authorId, uploadID)
	if err != nil {
		incompleteErr, ok := errors.AsType[*video.PartsIncompleteError](err)
		if !ok {
			response.FromError(c, err)
			return
		}

		response.Fail(c, http.StatusBadRequest, "分片未全部上传", incompleteErr.MissingParts)
		return
	}

	response.OK(c, video.CompleteResp{VideoKey: videoKey})
}

// AbortUpload 取消视频上传，通知minio删除之前保存的分片
func (h *Handler) AbortUpload(c *gin.Context) {}

// UpLoadConver 上传视频封面
func (h *Handler) UpLoadConver(c *gin.Context) {
	// 参数转换
	authorID, exist := c.Get("user_id")
	if !exist {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}
	authorId, ok := authorID.(uint)
	if !ok {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}

	// single file
	file, err := c.FormFile("file")
	if err != nil {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}
	src, err := file.Open()
	if err != nil {
		response.Fail(c, http.StatusInternalServerError, "读取文件失败")
		return
	}
	defer src.Close()

	// service call
	objectKey, url, err := h.svc.UploadCover(c.Request.Context(), authorId, file.Filename, file.Size, src)

	if err != nil {
		response.FromError(c, err)
		return
	}

	res := video.CoverResp{
		CoverKey:   objectKey,
		PreviewURL: url,
	}
	response.OK(c, res)
}

// Publish 视频上传成功之后，将视频元数据提交到数据库，用户彻底确定提交
func (h *Handler) Publish(c *gin.Context) {
	// 获取视频元数据参数 user_id user_name 从content中获取
	// title,description,videoKey,coverKey 从json格式中获取
	authorID, exist := c.Get("user_id")
	if !exist {
		response.Fail(c, http.StatusBadRequest, "未授权")
		return
	}
	authorId, ok := authorID.(uint)
	if !ok {
		response.Fail(c, http.StatusBadRequest, "未授权")
		return
	}

	username, exist := c.Get("username")
	if !exist {
		response.Fail(c, http.StatusBadRequest, "未授权")
		return
	}

	userName, ok := username.(string)
	if !ok {
		response.Fail(c, http.StatusUnauthorized, "未授权")
		return
	}

	var req video.PublishReq
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}

	v := video.Video{
		AuthorID:    authorId,
		Username:    userName,
		Title:       req.Title,
		Description: req.Description,
		VideoKey:    req.VideoKey,
		CoverKey:    req.CoverKey,
	}

	res, err := h.svc.Publish(c.Request.Context(), &v)
	if err != nil {
		response.FromError(c, err)
		return
	}
	response.OK(c, video.PublishResp{
		ID:          res.ID,
		Title:       res.Title,
		Description: res.Description,
		Author: video.Author{
			ID:       res.AuthorID,
			Username: res.Username,
		},
		VideoKey:  res.VideoKey,
		CoverKey:  res.CoverKey,
		CreatedAt: res.CreatedAt,
	})
}

// GetVideo 任何人拿到视频 id，就返回这条视频的完整可播放信息
func (h *Handler) GetVideo(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}
	view, err := h.svc.GetVideo(c.Request.Context(), uint(id))
	if err != nil {
		response.FromError(c, err)
		return
	}
	response.OK(c, view)
}

// ListVideos 按照时间倒序展示用户视频,采用游标分页
// 参数： 查询的用户id,分页游标cursor_str,显示个数limit
// 返回： 返回一组视频可播放信息
func (h *Handler) ListVideos(c *gin.Context) {
	authorID, _ := strconv.ParseUint(c.Query("author_id"), 10, 64) // 0=全站
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
	cursor := c.Query("cursor")
	if limit < 0 {
		response.Fail(c, 400, "参数错误")
		return
	}

	res, err := h.svc.ListVideos(c.Request.Context(), uint(authorID), cursor, limit)
	if err != nil {
		response.FromError(c, err)
		return
	}
	response.OK(c, res)
}

// DeleteVideo 用户指定删除自己的视频
func (h *Handler) DeleteVideo(c *gin.Context) {
	// 获取视频id
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		response.Fail(c, http.StatusBadRequest, "参数错误")
		return
	}

	// 获取用户id
	authorID, exist := c.Get("user_id")
	if !exist {
		response.Fail(c, http.StatusBadRequest, "未授权")
		return
	}
	authorId, ok := authorID.(uint)
	if !ok {
		response.Fail(c, http.StatusBadRequest, "未授权")
		return
	}

	// 执行删除业务
	err = h.svc.DeleteVideo(c.Request.Context(), uint(id), authorId)
	if err != nil {
		response.FromError(c, err)
		return
	}

	// 返回响应
	response.OK(c, nil)
}
