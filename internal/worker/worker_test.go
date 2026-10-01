package worker

import (
	"testing"
	"time"
)

func TestSign(t *testing.T) {
	got := sign("2026-10-01T00:00:00Z.{\"event_id\":\"evt_123\"}", "test-secret")
	want := "sha256=46de481c4322f053020f1e8e7cf8b95f80abd4d64cc26f28b2110c2abc87c150"
	if got != want {
		t.Fatalf("sign() = %q, want %q", got, want)
	}
}

func TestRetryable(t *testing.T) {
	for _, test := range []struct {
		code int
		want bool
	}{
		{0, true}, {408, true}, {409, true}, {429, true}, {500, true}, {503, true},
		{400, false}, {401, false}, {403, false}, {404, false}, {422, false},
	} {
		if got := retryable(test.code); got != test.want {
			t.Errorf("retryable(%d) = %v, want %v", test.code, got, test.want)
		}
	}
}

func TestRetryDelay(t *testing.T) {
	for _, test := range []struct {
		attempt int
		want    time.Duration
		ok      bool
	}{
		{1, time.Minute, true},
		{2, 5 * time.Minute, true},
		{3, 15 * time.Minute, true},
		{4, 15 * time.Minute, true},
		{5, 0, false},
	} {
		got, ok := retryDelay(test.attempt)
		if got != test.want || ok != test.ok {
			t.Errorf("retryDelay(%d) = (%s, %v), want (%s, %v)", test.attempt, got, ok, test.want, test.ok)
		}
	}
}
