package messaging

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRmqPublisher_Nil 覆盖适配器的空值语义：
// 直接调用 nil 指针 / 空结构体不会 panic，而是返回 ErrNilPublisher。
//
// 注意：接口值为 nil 时调用会 panic，所以"MQ 不可用"的降级由调用方
// 判断 pub == nil 完成，而不是依赖接口方法内部判空。
func TestRmqPublisher_Nil(t *testing.T) {
	var nilPublisher *rmqPublisher
	require.ErrorIs(t, nilPublisher.Publish(t.Context(), &Envelope{}), ErrNilPublisher)

	empty := &rmqPublisher{}
	require.ErrorIs(t, empty.Publish(t.Context(), &Envelope{}), ErrNilPublisher)
}
