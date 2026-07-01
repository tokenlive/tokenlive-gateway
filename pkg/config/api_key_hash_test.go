package config

import "testing"

func TestHashAPIKeyMatchesGatewayHMACSHA256(t *testing.T) {
	got := HashAPIKey("tl_live_example", "pepper")
	want := "06bfbed9282f1dcb96bd25c7bef96d9b49de0be5f3777b44f4f71cfcca8821b1"
	if got != want {
		t.Fatalf("HashAPIKey() = %q, want %q", got, want)
	}
}

func TestHashAPIKeyEmptyPepperStillDeterministic(t *testing.T) {
	got := HashAPIKey("tl_live_example", "")
	want := "569f98d4f3a9dd99afb439d169cf4ee2bcc98ffd60181c4a6476627dff65010a"
	if got != want {
		t.Fatalf("HashAPIKey() = %q, want %q", got, want)
	}
}
