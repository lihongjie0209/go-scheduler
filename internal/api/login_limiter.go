package api

import (
	"crypto/sha256"
	"encoding/hex"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	loginWindow     = time.Minute
	loginAttempts   = 5
	maxLoginWindows = 10_000
)

type loginWindowState struct {
	started  time.Time
	attempts int
}

type loginLimiter struct {
	mu      sync.Mutex
	windows map[string]loginWindowState
	now     func() time.Time
}

func newLoginLimiter() *loginLimiter {
	return &loginLimiter{windows: make(map[string]loginWindowState), now: time.Now}
}

func (l *loginLimiter) allow(remoteAddress, email string) bool {
	now := l.now()
	key := loginLimitKey(remoteAddress, email)
	l.mu.Lock()
	defer l.mu.Unlock()
	state, exists := l.windows[key]
	if exists && now.Sub(state.started) < loginWindow {
		if state.attempts >= loginAttempts {
			return false
		}
		state.attempts++
		l.windows[key] = state
		return true
	}
	if !exists && len(l.windows) >= maxLoginWindows {
		for candidate, candidateState := range l.windows {
			if now.Sub(candidateState.started) >= loginWindow {
				delete(l.windows, candidate)
			}
		}
		if len(l.windows) >= maxLoginWindows {
			return false
		}
	}
	l.windows[key] = loginWindowState{started: now, attempts: 1}
	return true
}

func (l *loginLimiter) reset(remoteAddress, email string) {
	l.mu.Lock()
	delete(l.windows, loginLimitKey(remoteAddress, email))
	l.mu.Unlock()
}

func loginLimitKey(remoteAddress, email string) string {
	host, _, err := net.SplitHostPort(remoteAddress)
	if err != nil {
		host = remoteAddress
	}
	emailHash := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(email))))
	return host + ":" + hex.EncodeToString(emailHash[:])
}
