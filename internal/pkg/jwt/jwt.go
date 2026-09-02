// Package jwt 提供jwt认证工具
package jwt

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	AccessTokenTTL  = 15 * time.Minute   // access token 有效期
	RefreshTokenTTL = 7 * 24 * time.Hour // refresh token 有效期
)

// CustomClaims 定义jwt token 里面携带什么信息
type CustomClaims struct {
	AccountID uint   `json:"account_id"`
	Username  string `json:"username"`
	TokenType string `json:"token_type"`
	jwt.RegisteredClaims
}

var cachedSecret []byte

// JwtSecrete 返回签名密钥（优先 JWT_SECRET，缺省生成随机并警告）
func JwtSecrete() []byte {
	if cachedSecret != nil {
		return cachedSecret
	}
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		b := make([]byte, 32)
		if _, err := rand.Read(b); err != nil {
			log.Printf("jwt: cannot generate secret: %v", err)
			cachedSecret = []byte("fallback-unsafe-key")
			return cachedSecret
		}
		secret = hex.EncodeToString(b)
		log.Printf("jwt: JWT_SECRET not set, generated random key. Tokens invalid after restart.")
	}
	cachedSecret = []byte(secret)
	return cachedSecret
}

// GenerateToken 签发 access token（HS256，15 分钟有效）
func GenerateToken(key []byte, accountID uint, username string) (string, error) {
	now := time.Now()
	claims := CustomClaims{
		AccountID: accountID,
		Username:  username,
		TokenType: "access",
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Minute * 15)), // 过期时间
			IssuedAt:  jwt.NewNumericDate(now),                       //签发时间
			NotBefore: jwt.NewNumericDate(now),                       //起效时间
		},
	}
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, &claims)
	tokenstr, err := t.SignedString(key)
	if err != nil {
		return "", err
	}
	return tokenstr, nil
}

// GenernateRefreshToken 签发随机的refresh token（32 字节转为16进制，变为64个字符）
func GenerateRefreshToken() string {
	b := make([]byte, 32)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// ParseToken 解析并校验 access token（验签名 + 过期 + 算法）
func ParseToken(key []byte, tokenstr string) (*CustomClaims, error) {
	// ParseWithClaims 会判断是否过期，签名之后是否一致; 提供keyfunc,我们自己检测了算法是否一致，然后返回密钥
	token, err := jwt.ParseWithClaims(tokenstr, &CustomClaims{}, func(t *jwt.Token) (any, error) {
		if t.Method == nil || t.Method.Alg() != jwt.SigningMethodHS256.Alg() {
			return nil, errors.New("unexpecred signing method")
		}
		return key, nil
	})

	// 判断错误
	if err != nil {
		return nil, err
	}

	claim, ok := token.Claims.(*CustomClaims)
	if !ok || !token.Valid {
		return nil, errors.New("invalid token claims")
	}
	return claim, nil

}
