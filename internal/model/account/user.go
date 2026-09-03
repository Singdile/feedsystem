// Package account 账号模块实体定义
package account

type User struct {
	ID           uint   `gorm:"primaryKey" json:"id"`
	Username     string `gorm:"unique" json:"username"`
	Password     string `json:"-"`
	RefreshToken string `gorm:"size:64;default:'';not null" json:"-"`   //随机串，时效比较长
	Bio          string `gorm:"type:varchar(255)" json:"bio,omitempty"` // 用户简介
}

// Profile 用户公开信息，用于展示
type Profile struct {
	ID       uint   `json:"id"`
	Username string `json:"username"`
	Bio      string `json:"bio,omitempty"` // 用户简介
}

func (u User) ToProfile() Profile {
	return Profile{
		ID:       u.ID,
		Username: u.Username,
		Bio:      u.Bio,
	}
}

type RegisterReq struct {
	Username string `json:"username" binding:"required"`
	Password string `json:"password" binding:"required"`
}

type LoginReq struct {
	Username string `json:"username" binding:"required"`
	Password string `json:"password" binding:"required"`
}

type ChangePasswordReq struct {
	UserID      uint   `json:"user_id"`
	NewPassword string `json:"newpassword"`
}

type RefreshTokenReq struct {
	RefreshToken string `json:"refresh_token" binding:"required"`
}

type LogoutReq struct {
	RefreshToken string `json:"refresh_token" binding:"required"`
}
