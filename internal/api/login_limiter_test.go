package api

import (
	"testing"
	"time"
)

func TestLoginLimiterBoundsAttemptsAndResets(t *testing.T) {
	t.Parallel()
	limiter := newLoginLimiter()
	now := time.Date(2026, 8, 13, 0, 0, 0, 0, time.UTC)
	limiter.now = func() time.Time { return now }
	for attempt := range loginAttempts {
		if !limiter.allow("192.0.2.1:1234", "User@example.com") {
			t.Fatalf("attempt %d unexpectedly rejected", attempt+1)
		}
	}
	if limiter.allow("192.0.2.1:9999", "user@example.com") {
		t.Fatal("limit bypassed by changing source port or email casing")
	}
	limiter.reset("192.0.2.1:4321", "USER@example.com")
	if !limiter.allow("192.0.2.1:1234", "user@example.com") {
		t.Fatal("successful-login reset did not restore allowance")
	}
	now = now.Add(loginWindow)
	if !limiter.allow("192.0.2.1:1234", "user@example.com") {
		t.Fatal("expired window did not restore allowance")
	}
}

func TestLoginLimiterSeparatesSources(t *testing.T) {
	t.Parallel()
	limiter := newLoginLimiter()
	for range loginAttempts {
		if !limiter.allow("192.0.2.1:1234", "user@example.com") {
			t.Fatal("initial source rejected")
		}
	}
	if !limiter.allow("192.0.2.2:1234", "user@example.com") {
		t.Fatal("independent source was rate limited")
	}
}
