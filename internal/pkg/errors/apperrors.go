// Package apperrors 定义公共的错误类型
package apperrors

import "errors"

// AppError 携带http状态码的业务错误
type AppError struct {
	Status int
	Msg    string
}

func (e *AppError) Error() string {
	return e.Msg
}

func NewAppError(status int, msg string) *AppError {
	return &AppError{
		Status: status,
		Msg:    msg,
	}
}

// FromError 把任意 error 转为 AppError（未知错误 → 500）
func FromError(err error) *AppError {
	if appErr, ok := errors.AsType[*AppError](err); ok {
		return appErr
	}
	return &AppError{
		Status: 500,
		Msg:    err.Error(),
	}
}

// ErrUsernameExists 用户名已存在（唯一索引冲突 1062 时由 repo 返回）
var ErrUsernameExists = errors.New("username already exists")
