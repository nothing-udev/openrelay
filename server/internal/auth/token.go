package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"
)

// tokenTTL is how long a token is valid after issuance.
// 5 minutes covers any reasonable client startup delay.
const tokenTTL = 5 * 60 // seconds

// GenerateToken returns a token for the given joinCode.
// Format: "<unix_ts>.<hmac_hex>"
// HMAC = HMAC-SHA256(secret, "<joinCode>:<unix_ts>")
func GenerateToken(secret []byte, joinCode string) string {
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	return ts + "." + sign(secret, joinCode, ts)
}

// ValidateToken returns true when the token is valid for joinCode.
// If secret is empty, auth is disabled and every token (even "") is accepted.
func ValidateToken(secret []byte, joinCode, token string) bool {
	if len(secret) == 0 {
		return true // auth disabled
	}
	parts := strings.SplitN(token, ".", 2)
	if len(parts) != 2 {
		return false
	}
	ts, sig := parts[0], parts[1]

	t, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return false
	}
	age := time.Now().Unix() - t
	if age < -30 || age > int64(tokenTTL) {
		return false
	}

	expected := sign(secret, joinCode, ts)
	return hmac.Equal([]byte(sig), []byte(expected))
}

func sign(secret []byte, joinCode, ts string) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(joinCode + ":" + ts))
	return hex.EncodeToString(mac.Sum(nil))
}
