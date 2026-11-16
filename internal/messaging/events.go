// 统一消息类型，供后续所有的事件使用
package messaging

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type EventType string

const (
	EventTypeTimeline     EventType = "timeline"
	EventTypeRating       EventType = "rating"
	EventTypeComment      EventType = "comment"
	EventTypeSocial       EventType = "social"
	EventTypePopularity   EventType = "popularity"
	EventTypeNotification EventType = "notification"
)

const CurrentEventVersion = 1

var (
	ErrNilEnvelope    = errors.New("messaging: nil envelope")
	ErrEmptyID        = errors.New("messaging: envelope id is required")
	ErrEmptyType      = errors.New("messaging: event type is required")
	ErrInvalidVersion = errors.New("messaging: version must be positive")
	ErrEmptyPayload   = errors.New("messaging: payload is empty")
	ErrInvalidPayload = errors.New("messaging: payload is not valid json")
)

// Envelope 是所有事件在 MQ 上的统一信封
//
// ID 是幂等键：同一事件在重试/重投时必须保持不变
type Envelope struct {
	ID         string          `json:"id"`
	Type       EventType       `json:"type"`
	Version    int             `json:"version"`
	OccurredAt time.Time       `json:"occurred_at"`
	Payload    json.RawMessage `json:"payload"` // 本质[]byte,但是encoding/json 不会像[]byte类型一样编码为base64，而是直接保留
}

// NewEnvelope 使用当前时间作为 OccurredAt
func NewEnvelope(id string, eventType EventType, version int, payload any) (*Envelope, error) {
	return NewEnvelopeAt(id, eventType, version, time.Now().UTC(), payload)
}

func NewEnvelopeAt(id string, eventType EventType, version int, occurredAt time.Time, payload any) (*Envelope, error) {
	// payload 序列化
	body, err := marshalPayload(payload)
	if err != nil {
		return nil, err
	}

	// 组装
	envelope := &Envelope{
		ID:         id,
		Type:       eventType,
		Version:    version,
		OccurredAt: occurredAt,
		Payload:    body,
	}

	// 检查是否符合基本标准
	if err := envelope.Validate(); err != nil {
		return nil, err
	}
	return envelope, nil
}

// Validate 校验信封的基本字段
func (e *Envelope) Validate() error {
	if e == nil {
		return ErrNilEnvelope
	}

	if strings.TrimSpace(e.ID) == "" {
		return ErrEmptyID
	}

	if strings.TrimSpace(string(e.Type)) == "" {
		return ErrEmptyType
	}

	if e.Version <= 0 {
		return ErrInvalidVersion
	}

	if len(e.Payload) == 0 {
		return ErrEmptyPayload
	}

	if !json.Valid(e.Payload) {
		return ErrInvalidPayload
	}

	return nil
}

// Marshal 序列化整个信封
func (e *Envelope) Marshal() ([]byte, error) {
	if e == nil {
		return nil, ErrNilEnvelope
	}

	if err := e.Validate(); err != nil {
		return nil, err
	}

	return json.Marshal(e)
}

// UnmarshalEnvelope 从字节序列反序列化并校验
func UnmarshalEnvelope(b []byte) (*Envelope, error) {
	var envelope Envelope
	if err := json.Unmarshal(b, &envelope); err != nil {
		return nil, fmt.Errorf("messaging: unmarshal envelope: %w", err)
	}

	if err := envelope.Validate(); err != nil {
		return nil, err
	}

	return &envelope, nil
}

func marshalPayload(payload any) (json.RawMessage, error) {
	switch v := payload.(type) {
	case nil:
		return nil, ErrEmptyPayload
	case json.RawMessage:
		if len(v) == 0 {
			return nil, ErrEmptyPayload
		}

		if !json.Valid(v) { //检查是否是合法的json格式
			return nil, ErrInvalidPayload
		}

		return v, nil
	case []byte:
		if len(v) == 0 {
			return nil, ErrEmptyPayload
		}
		if !json.Valid(v) { //检查是否是合法的json格式
			return nil, ErrInvalidPayload
		}
		return json.RawMessage(v), nil //转化为rawmessage类型
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("messaging: marshal payload: %w", err)
		}

		if len(b) == 0 {
			return nil, ErrEmptyPayload
		}

		return b, nil
	}
}

// Decode 把 payload 解析成具体事件结构体
func (e *Envelope) Decode(v any) error {
	if e == nil {
		return ErrNilEnvelope
	}
	if len(e.Payload) == 0 {
		return ErrEmptyPayload
	}

	if err := json.Unmarshal(e.Payload, v); err != nil {
		return fmt.Errorf("messaging: decode payload: %w", err)
	}
	return nil
}
