package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type fakeAPIKeyValidator struct{}

func (fakeAPIKeyValidator) VerifyKey(context.Context, string) (string, string, string, string, string, string, error) {
	return "usr_1", "tenant_a", "wsp_1", "tenant_a", "ak_1", "hash_1", nil
}

func TestAuthMiddlewareInjectsAPIKeyIdentityHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(NewAuthMiddleware(&AuthConfig{
		Validator: fakeAPIKeyValidator{},
		Logger:    zap.NewNop(),
	}))
	router.GET("/ping", func(c *gin.Context) {
		if got := c.Request.Header.Get("X-API-Key-ID"); got != "ak_1" {
			t.Fatalf("X-API-Key-ID = %q, want ak_1", got)
		}
		if got := c.Request.Header.Get("X-API-Key-Hash"); got != "hash_1" {
			t.Fatalf("X-API-Key-Hash = %q, want hash_1", got)
		}
		c.Status(http.StatusNoContent)
	})

	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	req.Header.Set("Authorization", "Bearer tl_live_test")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
}
