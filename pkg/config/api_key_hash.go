package config

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

func HashAPIKey(apiKey string, pepper string) string {
	mac := hmac.New(sha256.New, []byte(pepper))
	_, _ = mac.Write([]byte(apiKey))
	return hex.EncodeToString(mac.Sum(nil))
}
