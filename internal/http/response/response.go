// Package response 定义http响应格式
package response

import (
	apperrors "feedsystem/internal/pkg/errors"
	"net/http"

	"github.com/gin-gonic/gin"
)

type Body struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data []any  `json:"data"`
}

func OK(c *gin.Context, data ...any) {
	c.JSON(http.StatusOK, Body{
		Code: 0,
		Msg:  "ok",
		Data: data,
	})
}

func Fail(c *gin.Context, status int, msg string, data ...any) {
	c.JSON(status, Body{
		Code: status,
		Msg:  msg,
		Data: data,
	})
}

// FromError 统一错误输出
func FromError(c *gin.Context, err error) {
	apperr := apperrors.FromError(err)
	Fail(c, apperr.Status, apperr.Error())
}
