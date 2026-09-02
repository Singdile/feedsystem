package jwt

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testKey = "feedsystem-test-secret"

var key = []byte(testKey)

// test the whole flow
func TestGenerateAndParseToken(t *testing.T) {
	token, err := GenerateToken(key, 5, "testID")
	require.NoError(t, err)

	claims, err := ParseToken(key, token)
	require.NoError(t, err)
	assert.Equal(t, uint(5), claims.AccountID)
	assert.Equal(t, "testID", claims.Username)
	assert.Equal(t, "access", claims.TokenType)
	assert.True(t, claims.ExpiresAt.After(time.Now()))
	assert.True(t, claims.ExpiresAt.Before(time.Now().Add(16*time.Minute)))
}
