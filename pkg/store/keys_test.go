package store

import "testing"

func TestRedisKeyApiKeyHash(t *testing.T) {
	got := RedisKeyApiKeyHash("hash123")
	want := "aigw:apikey_hash:hash123"
	if got != want {
		t.Fatalf("RedisKeyApiKeyHash() = %q, want %q", got, want)
	}
}
