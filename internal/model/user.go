// Package model 定义实体
package model

type User struct {
    ID uint `gorm:"primaryKey" json:"id"`
    Username string `gorm:"unique" json:"username"`
    Passwored string
    Token string
    RefreshToken string
    Bio string `gorm:"type:varchar(255)" json:"bio,omitempty"` //用户简介
}


