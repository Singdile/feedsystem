package messaging

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

type decodeSample struct {
	VideoID   uint   `json:"video_id"`
	AccountID uint   `json:"account_id"`
	Action    string `json:"action"`
}

func mustEnvelopeBody(t *testing.T, typ EventType, payload any) []byte {
	t.Helper()

	env, err := NewEnvelope("evt-decode", typ, CurrentEventVersion, payload)
	require.NoError(t, err)

	body, err := env.Marshal()
	require.NoError(t, err)

	return body
}

func TestDecodeEvent(t *testing.T) {
	want := decodeSample{VideoID: 42, AccountID: 7, Action: "like"}

	t.Run("合法信封", func(t *testing.T) {
		var got decodeSample
		require.NoError(t, DecodeEvent(mustEnvelopeBody(t, EventTypeRating, want), EventTypeRating, &got))
		require.Equal(t, want, got)
	})

	t.Run("类型不匹配", func(t *testing.T) {
		var got decodeSample

		err := DecodeEvent(mustEnvelopeBody(t, EventTypeComment, want), EventTypeRating, &got)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unexpected event type")
		require.Zero(t, got)
	})

	t.Run("旧格式裸 JSON 不再兼容", func(t *testing.T) {
		body, err := json.Marshal(want)
		require.NoError(t, err)

		var got decodeSample
		require.Error(t, DecodeEvent(body, EventTypeRating, &got))
	})

	t.Run("非法 JSON", func(t *testing.T) {
		var got decodeSample
		require.Error(t, DecodeEvent([]byte("not-json"), EventTypeRating, &got))
	})

	t.Run("缺少 id", func(t *testing.T) {
		body := []byte(`{"type":"rating","version":1,"payload":{}}`)

		var got decodeSample
		require.ErrorIs(t, DecodeEvent(body, EventTypeRating, &got), ErrEmptyID)
	})

	t.Run("缺少 version", func(t *testing.T) {
		body := []byte(`{"id":"evt-1","type":"rating","payload":{}}`)

		var got decodeSample
		require.ErrorIs(t, DecodeEvent(body, EventTypeRating, &got), ErrInvalidVersion)
	})

	t.Run("缺少 payload", func(t *testing.T) {
		body := []byte(`{"id":"evt-1","type":"rating","version":1}`)

		var got decodeSample
		require.ErrorIs(t, DecodeEvent(body, EventTypeRating, &got), ErrEmptyPayload)
	})

	t.Run("payload 含未知字段", func(t *testing.T) {
		body := []byte(`{"id":"evt-1","type":"rating","version":1,"payload":{"video_id":1,"unknown":"x"}}`)

		var got decodeSample
		require.NoError(t, DecodeEvent(body, EventTypeRating, &got))
		require.Equal(t, uint(1), got.VideoID)
	})

	t.Run("目标不是指针", func(t *testing.T) {
		require.Error(t, DecodeEvent(mustEnvelopeBody(t, EventTypeRating, want), EventTypeRating, decodeSample{}))
	})
}
