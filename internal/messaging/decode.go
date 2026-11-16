package messaging

import (
	"fmt"
)

// DecodeEvent 严格解码： body必须是合法的 Envelope，并且里面是event是我们想要的
func DecodeEvent(body []byte, want EventType, v any) error {
	env, err := UnmarshalEnvelope(body)
	if err != nil {
		return err
	}

	if env.Type != want {
		return fmt.Errorf("messaging: unexpected event type %q, want %v", env.Type, want)
	}

	return env.Decode(v)
}
