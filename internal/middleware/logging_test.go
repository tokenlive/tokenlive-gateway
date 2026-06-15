package middleware

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMaskHeader(t *testing.T) {
	headers := http.Header{}
	headers.Add("Authorization", "Bearer sk-proj-123456789")
	headers.Add("Content-Type", "application/json")
	headers.Add("X-API-Key", "my-secret-key-123")
	headers.Add("X-Normal-Header", "normal-value")

	masked := maskHeader(headers)

	// Verify sensitive headers are masked
	assert.Contains(t, masked["Authorization"][0], "****")
	assert.NotEqual(t, "Bearer sk-proj-123456789", masked["Authorization"][0])

	assert.Contains(t, masked["X-Api-Key"][0], "****")
	assert.NotEqual(t, "my-secret-key-123", masked["X-Api-Key"][0])

	// Verify non-sensitive headers are untouched
	assert.Equal(t, "application/json", masked["Content-Type"][0])
	assert.Equal(t, "X-Normal-Header", http.CanonicalHeaderKey("X-Normal-Header"))
	assert.Equal(t, "normal-value", masked["X-Normal-Header"][0])
}
