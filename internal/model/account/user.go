// Package account 账号模块实体定义
package account

type User struct {
	ID           uint   `gorm:"primaryKey" json:"id"`
	Username     string `gorm:"unique" json:"username"`
	Password     string `json:"-"`
	Token        string `json:"-"`
	RefreshToken string `json:"-"`
	Bio          string `gorm:"type:varchar(255)" json:"bio,omitempty"` // 用户简介
}
