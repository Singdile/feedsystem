// Package account 账号模块实体定义
package account

type User struct {
	ID           uint   `gorm:"primaryKey" json:"id"`
	Username     string `gorm:"unique" json:"username"`
	Password     string `json:"-"`
	RefreshToken string `gorm:"size:64;default:'';not null"`            //随机串，时效比较长
	Bio          string `gorm:"type:varchar(255)" json:"bio,omitempty"` // 用户简介
}

type RegisterReq struct {
	Username string `json:"username" binding:"required"`
	Password string `json:"password" binding:"required"`
}

type LoginReq struct {
	Username string `json:"username" binding:"required"`
	Password string `json:"password" binding:"required"`
}
