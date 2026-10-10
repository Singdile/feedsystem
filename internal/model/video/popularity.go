package video

import ()

type PopularityEvent struct {
	EventID string `json:"event_id"`
	VideoID uint   `json:"video_id"`
	Change  int64  `json:"change"`
}
