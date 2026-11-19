package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"feedsystem/internal/messaging"
	outboxmodel "feedsystem/internal/model/outbox"

	"github.com/stretchr/testify/require"
)

// ---------- 测试替身 ----------

type failedCall struct {
	id         uint
	retryAfter time.Duration
}

type fakeOutboxStore struct {
	mu       sync.Mutex
	msgs     []outboxmodel.Msg
	done     []uint
	failed   []failedCall
	claims   int
	reclaims int
	claimErr error
	doneCh   chan uint
}

func (f *fakeOutboxStore) Claim(ctx context.Context, limit, maxAttempts int) ([]outboxmodel.Msg, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.claims++
	if f.claimErr != nil {
		return nil, f.claimErr
	}

	msgs := f.msgs
	f.msgs = nil
	return msgs, nil
}

func (f *fakeOutboxStore) MarkDone(ctx context.Context, id uint) error {
	f.mu.Lock()
	f.done = append(f.done, id)
	f.mu.Unlock()

	if f.doneCh != nil {
		f.doneCh <- id
	}
	return nil
}

func (f *fakeOutboxStore) MarkFailed(ctx context.Context, id uint, retryAfter time.Duration, cause error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed = append(f.failed, failedCall{id: id, retryAfter: retryAfter})
	return nil
}

func (f *fakeOutboxStore) Reclaim(ctx context.Context, olderThan time.Duration, maxAttempts int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reclaims++
	return 0, nil
}

func (f *fakeOutboxStore) claimCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.claims
}

func (f *fakeOutboxStore) reclaimCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reclaims
}

func (f *fakeOutboxStore) doneIDs() []uint {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uint(nil), f.done...)
}

func (f *fakeOutboxStore) failedCalls() []failedCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]failedCall(nil), f.failed...)
}

type fakePublisher struct {
	mu    sync.Mutex
	errs  []error
	calls int
	envs  []*messaging.Envelope
}

func (p *fakePublisher) Publish(ctx context.Context, env *messaging.Envelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	i := p.calls
	p.calls++
	p.envs = append(p.envs, env)

	if i < len(p.errs) {
		return p.errs[i]
	}
	return nil
}

func (p *fakePublisher) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func (p *fakePublisher) envelopes() []*messaging.Envelope {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*messaging.Envelope(nil), p.envs...)
}

// ---------- 辅助函数 ----------

func outboxTestConfig() OutboxConfig {
	return OutboxConfig{
		BatchSize:      10,
		PublishTimeout: time.Second,
		PollInterval:   time.Millisecond,
		ReclaimAfter:   time.Minute,
		MaxAttempts:    3,
	}
}

func startOutboxPoller(t *testing.T, store OutboxStore, pub messaging.Publisher, cfg OutboxConfig) (context.CancelFunc, <-chan struct{}) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	poller := NewOutboxPoller(store, pub, cfg)
	go func() {
		defer close(done)
		poller.Run(ctx)
	}()

	return cancel, done
}

func stopOutboxPoller(t *testing.T, cancel context.CancelFunc, done <-chan struct{}) {
	t.Helper()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("outbox poller did not stop")
	}
}

// ---------- 用例 ----------

func TestOutboxPollerPublishesAndMarksDone(t *testing.T) {
	store := &fakeOutboxStore{
		msgs: []outboxmodel.Msg{
			{ID: 1, EventType: outboxmodel.EventTypePublish, VideoID: 10, AuthorID: 1, CreateTime: time.Now()},
			{ID: 2, EventType: outboxmodel.EventTypePublish, VideoID: 11, AuthorID: 1, CreateTime: time.Now()},
		},
		doneCh: make(chan uint, 2),
	}
	pub := &fakePublisher{}

	cancel, done := startOutboxPoller(t, store, pub, outboxTestConfig())

	for i := 0; i < 2; i++ {
		select {
		case <-store.doneCh:
		case <-time.After(2 * time.Second):
			t.Fatal("message not marked done in time")
		}
	}
	stopOutboxPoller(t, cancel, done)

	require.ElementsMatch(t, []uint{1, 2}, store.doneIDs())
	require.Empty(t, store.failedCalls())
	require.Equal(t, 2, pub.callCount())

	envs := pub.envelopes()
	require.Len(t, envs, 2)
	require.Equal(t, messaging.EventTypeTimeline, envs[0].Type)
}

func TestOutboxPollerRetriesOnFailure(t *testing.T) {
	store := &fakeOutboxStore{
		msgs: []outboxmodel.Msg{
			{ID: 1, EventType: outboxmodel.EventTypePublish, Attempts: 1, CreateTime: time.Now()},
		},
	}
	pub := &fakePublisher{errs: []error{errors.New("mq down")}}

	cancel, done := startOutboxPoller(t, store, pub, outboxTestConfig())

	require.Eventually(t, func() bool {
		return len(store.failedCalls()) == 1
	}, 2*time.Second, 5*time.Millisecond)

	stopOutboxPoller(t, cancel, done)

	require.Empty(t, store.doneIDs())
	require.Greater(t, store.failedCalls()[0].retryAfter, time.Duration(0), "未耗尽时应退避重试")
}

func TestOutboxPollerExhausted(t *testing.T) {
	store := &fakeOutboxStore{
		msgs: []outboxmodel.Msg{
			{ID: 1, EventType: outboxmodel.EventTypePublish, Attempts: 3, CreateTime: time.Now()},
		},
	}
	pub := &fakePublisher{errs: []error{errors.New("mq down")}}

	cfg := outboxTestConfig()
	cfg.MaxAttempts = 3
	cancel, done := startOutboxPoller(t, store, pub, cfg)

	require.Eventually(t, func() bool {
		return len(store.failedCalls()) == 1
	}, 2*time.Second, 5*time.Millisecond)

	stopOutboxPoller(t, cancel, done)

	require.Zero(t, store.failedCalls()[0].retryAfter, "耗尽时应传 0，标记为 failed")
}

func TestOutboxPollerNilPublisherSkipsClaim(t *testing.T) {
	store := &fakeOutboxStore{}

	cancel, done := startOutboxPoller(t, store, nil, outboxTestConfig())

	time.Sleep(20 * time.Millisecond)
	stopOutboxPoller(t, cancel, done)

	require.Zero(t, store.claimCount(), "publisher 为 nil 时不应认领")
}

func TestOutboxPollerReclaims(t *testing.T) {
	store := &fakeOutboxStore{}

	cancel, done := startOutboxPoller(t, store, &fakePublisher{}, outboxTestConfig())

	require.Eventually(t, func() bool {
		return store.reclaimCount() > 0
	}, time.Second, 5*time.Millisecond)

	stopOutboxPoller(t, cancel, done)
}

func TestOutboxPollerStopsOnCancel(t *testing.T) {
	store := &fakeOutboxStore{}

	cancel, done := startOutboxPoller(t, store, &fakePublisher{}, outboxTestConfig())

	// 空批次会按 PollInterval 等待，取消后应尽快退出
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("poller did not stop after context cancel")
	}
}

func TestOutboxBackoff(t *testing.T) {
	require.GreaterOrEqual(t, outboxBackoff(1), outboxBackoffBase)
	require.Greater(t, outboxBackoff(2), 2*outboxBackoffBase-time.Millisecond)
	require.LessOrEqual(t, outboxBackoff(100), outboxBackoffMax+outboxBackoffMax/5+time.Millisecond)
}
