package worker

import (
	"context"
	"testing"

	"feedsystem/internal/messaging"

	"github.com/stretchr/testify/require"
)

// fakeMessage 是 messaging.Message 的测试实现，用来替代真实的 AMQP delivery
type fakeMessage struct {
	body     []byte
	accepted bool
	requeued bool
}

func (m *fakeMessage) Body() []byte { return m.body }

func (m *fakeMessage) Accept(ctx context.Context) error {
	m.accepted = true
	return nil
}

func (m *fakeMessage) Requeue(ctx context.Context) error {
	m.requeued = true
	return nil
}

var _ messaging.Message = (*fakeMessage)(nil)

// envelopeMessage 把事件包成统一信封，模拟一条真实投递的消息
func envelopeMessage(t *testing.T, typ messaging.EventType, payload any) *fakeMessage {
	t.Helper()

	env, err := messaging.NewEnvelope("evt-test", typ, messaging.CurrentEventVersion, payload)
	require.NoError(t, err)

	body, err := env.Marshal()
	require.NoError(t, err)

	return &fakeMessage{body: body}
}

// rawMessage 构造一条非 Envelope 的消息，用于验证严格解码会拒绝旧格式
func rawMessage(body []byte) *fakeMessage {
	return &fakeMessage{body: body}
}
