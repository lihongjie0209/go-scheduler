package auth

import (
	"strings"
	"testing"
	"time"
)

func TestPasswordHash(t *testing.T) {
	t.Parallel()
	hash, err := HashPassword("correct horse battery staple")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(hash, "correct") {
		t.Fatal("hash contains password")
	}
	if !VerifyPassword(hash, "correct horse battery staple") {
		t.Fatal("valid password rejected")
	}
	if VerifyPassword(hash, "wrong password value") {
		t.Fatal("invalid password accepted")
	}
}
func TestJWT(t *testing.T) {
	t.Parallel()
	manager, err := NewManager(strings.Repeat("s", 32), "scheduler", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := manager.Issue("user-1", true)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := manager.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if claims.Subject != "user-1" || !claims.PlatformAdmin {
		t.Fatalf("unexpected claims: %+v", claims)
	}
}
