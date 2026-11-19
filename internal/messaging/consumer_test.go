package messaging

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ---------- 测试替身 ----------

// fakeMessage 是 Message 的测试实现，只记录是否被 settle。
type fakeMessage struct {
	body     []byte
	accepted atomic.Bool
}

func (m *fakeMessage) Body() []byte { return m.body }

func (m *fakeMessage) Accept(ctx context.Context) error {
	m.accepted.Store(true)
	return nil
}

// fakeSource 按顺序投递消息；消息耗尽后阻塞到 ctx 取消，模拟真实链路的"等消息"状态。
type fakeSource struct {
	mu     sync.Mutex
	msgs   []Message
	idx    int
	closed atomic.Bool
}

func (s *fakeSource) Receive(ctx context.Context) (Message, error) {
	s.mu.Lock()
	if s.idx < len(s.msgs) {
		msg := s.msgs[s.idx]
		s.idx++
		s.mu.Unlock()
		return msg, nil
	}
	s.mu.Unlock()

	<-ctx.Done() // 没有新消息时阻塞
	return nil, ctx.Err()
}

func (s *fakeSource) Close(ctx context.Context) error {
	s.closed.Store(true)
	return nil
}

// fakeFactory 允许指定第 n 次 OpenSource 返回错误，用于测试重连。
type fakeFactory struct {
	mu      sync.Mutex
	calls   int
	errs    []error
	sources []*fakeSource
}

func (f *fakeFactory) OpenSource(ctx context.Context, queue string, prefetch int32) (MessageSource, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	i := f.calls
	f.calls++

	if i < len(f.errs) && f.errs[i] != nil {
		return nil, f.errs[i]
	}
	if len(f.sources) == 0 {
		return &fakeSource{}, nil
	}
	if i >= len(f.sources) {
		return f.sources[len(f.sources)-1], nil
	}
	return f.sources[i], nil
}

func (f *fakeFactory) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// ---------- 辅助函数 ----------

func testConfig() ConsumerConfig {
	return ConsumerConfig{
		Queue:            "test.queue",
		Concurrency:      2,
		Prefetch:         4,
		Timeout:          time.Second,
		DrainTimeout:     time.Second,
		ConnectBackoff:   time.Millisecond,
		ReconnectBackoff: time.Millisecond,
	}
}

// waitConsume 等待 Consume 返回，避免测试挂死。
func waitConsume(t *testing.T, errCh <-chan error) error {
	t.Helper()

	select {
	case err := <-errCh:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Consume did not return in time")
		return nil
	}
}

// startConsume 在后台运行 Consume，返回取消函数与错误通道。
func startConsume(t *testing.T, cfg ConsumerConfig, f SourceFactory, h Handler) (context.CancelFunc, <-chan error) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)

	go func() {
		errCh <- NewConsumer(f).Consume(ctx, cfg, h)
	}()

	return cancel, errCh
}

// ---------- 用例 ----------

// TestConsumerConcurrency 验证并发上限与并发确实生效。
func TestConsumerConcurrency(t *testing.T) {
	const (
		msgCount    = 8
		concurrency = 4
	)

	msgs := make([]Message, 0, msgCount)
	for i := 0; i < msgCount; i++ {
		msgs = append(msgs, &fakeMessage{body: []byte{byte(i)}})
	}

	var current, peak atomic.Int32
	handled := make(chan struct{}, msgCount)

	h := func(ctx context.Context, msg Message) error {
		cur := current.Add(1)
		for {
			old := peak.Load()
			if cur <= old || peak.CompareAndSwap(old, cur) {
				break
			}
		}

		time.Sleep(20 * time.Millisecond)

		current.Add(-1)
		if err := msg.Accept(ctx); err != nil {
			return err
		}
		handled <- struct{}{}
		return nil
	}

	cfg := testConfig()
	cfg.Concurrency = concurrency
	cfg.Prefetch = msgCount

	cancel, errCh := startConsume(t, cfg, &fakeFactory{
		sources: []*fakeSource{{msgs: msgs}},
	}, h)

	for i := 0; i < msgCount; i++ {
		select {
		case <-handled:
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for messages to be handled")
		}
	}

	cancel()
	require.NoError(t, waitConsume(t, errCh))

	require.LessOrEqual(t, peak.Load(), int32(concurrency), "并发数不能超过配置")
	require.Greater(t, peak.Load(), int32(1), "并发应真实生效")

	for _, msg := range msgs {
		require.True(t, msg.(*fakeMessage).accepted.Load(), "每条消息都应被 settle")
	}
}

// TestConsumerTimeout 验证单条消息超时后 worker 被释放。
func TestConsumerTimeout(t *testing.T) {
	msg := &fakeMessage{}
	var elapsed time.Duration

	cfg := testConfig()
	cfg.Concurrency = 1
	cfg.Prefetch = 1
	cfg.Timeout = 20 * time.Millisecond

	h := func(ctx context.Context, m Message) error {
		start := time.Now()
		<-ctx.Done() // 等超时
		elapsed = time.Since(start)
		return m.Accept(ctx)
	}

	cancel, errCh := startConsume(t, cfg, &fakeFactory{
		sources: []*fakeSource{{msgs: []Message{msg}}},
	}, h)

	require.Eventually(t, func() bool { return msg.accepted.Load() }, 2*time.Second, 5*time.Millisecond)

	cancel()
	require.NoError(t, waitConsume(t, errCh))

	require.GreaterOrEqual(t, elapsed, 10*time.Millisecond, "handler 应至少等到接近超时")
	require.Less(t, elapsed, time.Second)
}

// TestConsumerPanicRecovery 验证单条消息 panic 不会终止消费，且消息被 settle。
func TestConsumerPanicRecovery(t *testing.T) {
	panicMsg := &fakeMessage{body: []byte("panic")}
	okMsg := &fakeMessage{body: []byte("ok")}

	var calls atomic.Int32
	handled := make(chan struct{}, 2)

	h := func(ctx context.Context, msg Message) error {
		if calls.Add(1) == 1 {
			panic("boom")
		}
		if err := msg.Accept(ctx); err != nil {
			return err
		}
		handled <- struct{}{}
		return nil
	}

	cfg := testConfig()
	cfg.Concurrency = 1
	cfg.Prefetch = 2

	cancel, errCh := startConsume(t, cfg, &fakeFactory{
		sources: []*fakeSource{{msgs: []Message{panicMsg, okMsg}}},
	}, h)

	select {
	case <-handled:
	case <-time.After(2 * time.Second):
		t.Fatal("panic 之后没有继续消费")
	}

	cancel()
	require.NoError(t, waitConsume(t, errCh))

	require.True(t, panicMsg.accepted.Load(), "panic 的消息也应被 settle")
	require.True(t, okMsg.accepted.Load())
	require.Equal(t, int32(2), calls.Load())
}

// TestConsumerDrainOnCancel 验证取消后，在途消息会先处理完再退出。
func TestConsumerDrainOnCancel(t *testing.T) {
	msg := &fakeMessage{}
	started := make(chan struct{})

	cfg := testConfig()
	cfg.Concurrency = 1
	cfg.Prefetch = 1

	h := func(ctx context.Context, m Message) error {
		close(started)
		time.Sleep(50 * time.Millisecond)
		return m.Accept(ctx)
	}

	cancel, errCh := startConsume(t, cfg, &fakeFactory{
		sources: []*fakeSource{{msgs: []Message{msg}}},
	}, h)

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler 未开始执行")
	}

	start := time.Now()
	cancel()
	require.NoError(t, waitConsume(t, errCh))

	require.GreaterOrEqual(t, time.Since(start), 40*time.Millisecond, "应等待在途消息处理完成")
	require.True(t, msg.accepted.Load())
}

// TestConsumerDrainTimeout 验证超过 DrainTimeout 后返回 ErrDrainTimeout。
func TestConsumerDrainTimeout(t *testing.T) {
	msg := &fakeMessage{}
	started := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	cfg := testConfig()
	cfg.Concurrency = 1
	cfg.Prefetch = 1
	cfg.Timeout = 5 * time.Second
	cfg.DrainTimeout = 50 * time.Millisecond

	h := func(ctx context.Context, m Message) error {
		close(started)
		<-release // 忽略 ctx，模拟卡住的 handler
		return m.Accept(ctx)
	}

	cancel, errCh := startConsume(t, cfg, &fakeFactory{
		sources: []*fakeSource{{msgs: []Message{msg}}},
	}, h)

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler 未开始执行")
	}

	start := time.Now()
	cancel()

	err := waitConsume(t, errCh)
	require.ErrorIs(t, err, ErrDrainTimeout)
	require.Less(t, time.Since(start), time.Second, "应在 DrainTimeout 附近返回")
}

// TestConsumerReconnect 验证打开链路失败后能退避重连并继续消费。
func TestConsumerReconnect(t *testing.T) {
	msg := &fakeMessage{}
	handled := make(chan struct{}, 1)

	cfg := testConfig()
	cfg.Concurrency = 1
	cfg.Prefetch = 1

	h := func(ctx context.Context, m Message) error {
		if err := m.Accept(ctx); err != nil {
			return err
		}
		handled <- struct{}{}
		return nil
	}

	factory := &fakeFactory{
		errs:    []error{errors.New("connect failed")},
		sources: []*fakeSource{{msgs: []Message{msg}}},
	}

	cancel, errCh := startConsume(t, cfg, factory, h)

	select {
	case <-handled:
	case <-time.After(2 * time.Second):
		t.Fatal("重连后没有消费到消息")
	}

	cancel()
	require.NoError(t, waitConsume(t, errCh))

	require.True(t, msg.accepted.Load())
	require.GreaterOrEqual(t, factory.callCount(), 2, "应至少重连一次")
}

// TestConsumerStopWithoutMessages 验证没有消息时收到取消能正常退出。
func TestConsumerStopWithoutMessages(t *testing.T) {
	cfg := testConfig()

	cancel, errCh := startConsume(t, cfg, &fakeFactory{
		sources: []*fakeSource{{}},
	}, func(ctx context.Context, msg Message) error {
		return msg.Accept(ctx)
	})

	cancel()
	require.NoError(t, waitConsume(t, errCh))
}
