package account

type Social struct {
	ID         uint `gorm:"primaryKey" json:"id"`
	FollowerID uint `gorm:"uniqueIndex:idx_follow_pair;not null" json:"follower_id"`
	VloggerID  uint `gorm:"uniqueIndex:idx_follow_pair;not null;index" json:"vlogger_id"`
}

type SocialEvent struct {
	EventID    string `json:"event_id"`
	Action     string `json:"action"` // follow,unfollow
	FollowerID uint   `json:"follower_id"`
	VloggerID  uint   `json:"vlogger_id"`
}
