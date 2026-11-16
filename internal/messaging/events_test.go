package messaging

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// testEvent 是测试用的具体 payload 结构体
type testEvent struct {
	VideoID   uint   `json:"video_id"`
	AccountID uint   `json:"account_id"`
	Action    string `json:"action"`
}

func validPayload() testEvent {
	return testEvent{VideoID: 42, AccountID: 7, Action: "like"}
}

// newValidEnvelope 返回一个字段全部合法的信封，供其他用例复用
func newValidEnvelope(t *testing.T) *Envelope {
	t.Helper()

	env, err := NewEnvelope("evt-1", EventTypeRating, CurrentEventVersion, validPayload())
	require.NoError(t, err)

	return env
}

func TestNewEnvelope_Success(t *testing.T) {
	before := time.Now().UTC()

	env, err := NewEnvelope("evt-1", EventTypeRating, CurrentEventVersion, validPayload())
	require.NoError(t, err)
	require.NotNil(t, env)

	require.Equal(t, "evt-1", env.ID)
	require.Equal(t, EventTypeRating, env.Type)
	require.Equal(t, CurrentEventVersion, env.Version)
	require.WithinDuration(t, before, env.OccurredAt, time.Second)

	// Payload 应该是原始 JSON，而不是 base64 字符串
	var got testEvent
	require.NoError(t, json.Unmarshal(env.Payload, &got))
	require.Equal(t, validPayload(), got)
}

func TestNewEnvelopeAt_UsesGivenTime(t *testing.T) {
	at := time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)

	env, err := NewEnvelopeAt("evt-1", EventTypeTimeline, 2, at, validPayload())
	require.NoError(t, err)
	require.True(t, env.OccurredAt.Equal(at), "OccurredAt = %v, want %v", env.OccurredAt, at)
}

func TestNewEnvelope_InvalidFields(t *testing.T) {
	tests := []struct {
		name      string
		id        string
		eventType EventType
		version   int
		payload   any
		wantErr   error
	}{
		{
			name:      "空 ID",
			id:        "",
			eventType: EventTypeRating,
			version:   1,
			payload:   validPayload(),
			wantErr:   ErrEmptyID,
		},
		{
			name:      "全空格 ID",
			id:        "   ",
			eventType: EventTypeRating,
			version:   1,
			payload:   validPayload(),
			wantErr:   ErrEmptyID,
		},
		{
			name:      "空 Type",
			id:        "evt-1",
			eventType: "",
			version:   1,
			payload:   validPayload(),
			wantErr:   ErrEmptyType,
		},
		{
			name:      "全空格 Type",
			id:        "evt-1",
			eventType: EventType("   "),
			version:   1,
			payload:   validPayload(),
			wantErr:   ErrEmptyType,
		},
		{
			name:      "version 为 0",
			id:        "evt-1",
			eventType: EventTypeRating,
			version:   0,
			payload:   validPayload(),
			wantErr:   ErrInvalidVersion,
		},
		{
			name:      "version 为负数",
			id:        "evt-1",
			eventType: EventTypeRating,
			version:   -1,
			payload:   validPayload(),
			wantErr:   ErrInvalidVersion,
		},
		{
			name:      "payload 为 nil",
			id:        "evt-1",
			eventType: EventTypeRating,
			version:   1,
			payload:   nil,
			wantErr:   ErrEmptyPayload,
		},
		{
			name:      "payload 为空的 RawMessage",
			id:        "evt-1",
			eventType: EventTypeRating,
			version:   1,
			payload:   json.RawMessage{},
			wantErr:   ErrEmptyPayload,
		},
		{
			name:      "payload 为非法 RawMessage",
			id:        "evt-1",
			eventType: EventTypeRating,
			version:   1,
			payload:   json.RawMessage(`{"video_id":`),
			wantErr:   ErrInvalidPayload,
		},
		{
			name:      "payload 为空 []byte",
			id:        "evt-1",
			eventType: EventTypeRating,
			version:   1,
			payload:   []byte{},
			wantErr:   ErrEmptyPayload,
		},
		{
			name:      "payload 为非法 []byte",
			id:        "evt-1",
			eventType: EventTypeRating,
			version:   1,
			payload:   []byte(`not json`),
			wantErr:   ErrInvalidPayload,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env, err := NewEnvelope(tt.id, tt.eventType, tt.version, tt.payload)
			require.ErrorIs(t, err, tt.wantErr)
			require.Nil(t, env)
		})
	}
}

func TestEnvelope_Validate(t *testing.T) {
	tests := []struct {
		name    string
		env     *Envelope
		wantErr error
	}{
		{
			name:    "nil 信封",
			env:     nil,
			wantErr: ErrNilEnvelope,
		},
		{
			name: "合法信封",
			env: &Envelope{
				ID:         "evt-1",
				Type:       EventTypeRating,
				Version:    1,
				OccurredAt: time.Now().UTC(),
				Payload:    json.RawMessage(`{"video_id":42}`),
			},
		},
		{
			name: "缺少 payload",
			env: &Envelope{
				ID:      "evt-1",
				Type:    EventTypeRating,
				Version: 1,
			},
			wantErr: ErrEmptyPayload,
		},
		{
			name: "payload 不是合法 JSON",
			env: &Envelope{
				ID:      "evt-1",
				Type:    EventTypeRating,
				Version: 1,
				Payload: json.RawMessage(`{oops}`),
			},
			wantErr: ErrInvalidPayload,
		},
		{
			name: "缺少 ID",
			env: &Envelope{
				Type:    EventTypeRating,
				Version: 1,
				Payload: json.RawMessage(`{}`),
			},
			wantErr: ErrEmptyID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.env.Validate()
			if tt.wantErr == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestEnvelope_Marshal_IsRawJSON(t *testing.T) {
	env := newValidEnvelope(t)

	b, err := env.Marshal()
	require.NoError(t, err)

	s := string(b)

	// payload 必须是内联 JSON 对象
	require.Contains(t, s, `"payload":{"video_id":42,"account_id":7,"action":"like"}`)

	// []byte 会被 encoding/json 编码成 base64，json.RawMessage 不会
	require.NotContains(t, s, "eyJ2aWRlb19pZCI6")

	var decoded struct {
		ID         string          `json:"id"`
		Type       string          `json:"type"`
		Version    int             `json:"version"`
		OccurredAt time.Time       `json:"occurred_at"`
		Payload    json.RawMessage `json:"payload"`
	}
	require.NoError(t, json.Unmarshal(b, &decoded))
	require.Equal(t, env.ID, decoded.ID)
	require.Equal(t, string(env.Type), decoded.Type)
	require.Equal(t, env.Version, decoded.Version)
	require.True(t, env.OccurredAt.Equal(decoded.OccurredAt))
	require.JSONEq(t, string(env.Payload), string(decoded.Payload))
}

func TestEnvelope_Marshal_Errors(t *testing.T) {
	var nilEnv *Envelope
	_, err := nilEnv.Marshal()
	require.ErrorIs(t, err, ErrNilEnvelope)

	invalid := &Envelope{
		ID:      "evt-1",
		Type:    EventTypeRating,
		Version: 1,
		Payload: json.RawMessage(`not json`),
	}
	_, err = invalid.Marshal()
	require.ErrorIs(t, err, ErrInvalidPayload)
}

func TestUnmarshalEnvelope_Success(t *testing.T) {
	env := newValidEnvelope(t)

	b, err := env.Marshal()
	require.NoError(t, err)

	got, err := UnmarshalEnvelope(b)
	require.NoError(t, err)
	require.Equal(t, env.ID, got.ID)
	require.Equal(t, env.Type, got.Type)
	require.Equal(t, env.Version, got.Version)
	require.True(t, env.OccurredAt.Equal(got.OccurredAt))
	require.JSONEq(t, string(env.Payload), string(got.Payload))
}

func TestUnmarshalEnvelope_Errors(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr error
	}{
		{
			name: "整体不是 JSON",
			body: `not json`,
		},
		{
			name:    "缺少 ID",
			body:    `{"type":"rating","version":1,"payload":{}}`,
			wantErr: ErrEmptyID,
		},
		{
			name:    "缺少 Type",
			body:    `{"id":"evt-1","version":1,"payload":{}}`,
			wantErr: ErrEmptyType,
		},
		{
			name:    "缺少 version",
			body:    `{"id":"evt-1","type":"rating","payload":{}}`,
			wantErr: ErrInvalidVersion,
		},
		{
			name:    "缺少 payload",
			body:    `{"id":"evt-1","type":"rating","version":1}`,
			wantErr: ErrEmptyPayload,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env, err := UnmarshalEnvelope([]byte(tt.body))
			if tt.wantErr == nil {
				require.Error(t, err)
			} else {
				require.ErrorIs(t, err, tt.wantErr)
			}
			require.Nil(t, env)
		})
	}
}

func TestEnvelope_Decode(t *testing.T) {
	env := newValidEnvelope(t)

	var got testEvent
	require.NoError(t, env.Decode(&got))
	require.Equal(t, validPayload(), got)
}

func TestEnvelope_Decode_Errors(t *testing.T) {
	var nilEnv *Envelope
	var got testEvent
	require.ErrorIs(t, nilEnv.Decode(&got), ErrNilEnvelope)

	empty := &Envelope{ID: "evt-1", Type: EventTypeRating, Version: 1}
	require.ErrorIs(t, empty.Decode(&got), ErrEmptyPayload)

	// payload 与目标类型不匹配
	var num int
	require.Error(t, newValidEnvelope(t).Decode(&num))
}

func TestMarshalPayload(t *testing.T) {
	// 结构体：编码一次
	b, err := marshalPayload(validPayload())
	require.NoError(t, err)
	require.JSONEq(t, `{"video_id":42,"account_id":7,"action":"like"}`, string(b))

	// json.RawMessage：原样保留
	b, err = marshalPayload(json.RawMessage(`{"video_id": 42}`))
	require.NoError(t, err)
	require.JSONEq(t, `{"video_id":42}`, string(b))

	// []byte：按 JSON 处理，绝不能变成 base64
	b, err = marshalPayload([]byte(`{"video_id":43}`))
	require.NoError(t, err)
	require.JSONEq(t, `{"video_id":43}`, string(b))

	// 标量也是合法 JSON
	b, err = marshalPayload("plain")
	require.NoError(t, err)
	require.Equal(t, `"plain"`, string(b))
}

func TestMarshalPayload_Errors(t *testing.T) {
	_, err := marshalPayload(nil)
	require.ErrorIs(t, err, ErrEmptyPayload)

	_, err = marshalPayload(json.RawMessage{})
	require.ErrorIs(t, err, ErrEmptyPayload)

	_, err = marshalPayload(json.RawMessage(`{`))
	require.ErrorIs(t, err, ErrInvalidPayload)

	_, err = marshalPayload([]byte{})
	require.ErrorIs(t, err, ErrEmptyPayload)

	_, err = marshalPayload([]byte(`x`))
	require.ErrorIs(t, err, ErrInvalidPayload)

	// 无法序列化的类型
	_, err = marshalPayload(make(chan int))
	require.Error(t, err)
	require.Contains(t, err.Error(), "marshal payload")
}

func TestEventTypeConstants(t *testing.T) {
	types := []EventType{
		EventTypeTimeline,
		EventTypeRating,
		EventTypeComment,
		EventTypeSocial,
		EventTypePopularity,
		EventTypeNotification,
	}

	seen := make(map[EventType]struct{}, len(types))
	for _, et := range types {
		require.NotEmpty(t, et)
		_, dup := seen[et]
		require.False(t, dup, "duplicate event type: %s", et)
		seen[et] = struct{}{}
	}

	require.Equal(t, 1, CurrentEventVersion)
}
