package account

type Social struct {
	ID         uint `gorm:"primaryKey" json:"id"`
	FollowerID uint `gorm:"uniqueIndex:idx_follow_pair;not null" json:"follower_id"`
	VloggerID  uint `gorm:"uniqueIndex:idx_follow_pair;not null;index" json:"vlogger_id"`
}
