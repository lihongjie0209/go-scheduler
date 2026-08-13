package store

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestBoundedNotificationError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		message string
	}{
		{name: "ASCII", message: strings.Repeat("x", maxNotificationErrorBytes+100)},
		{name: "multibyte boundary", message: strings.Repeat("界", maxNotificationErrorBytes)},
		{name: "invalid UTF-8", message: strings.Repeat("x", maxNotificationErrorBytes-1) + "\xff\xfe"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := boundedNotificationError(tt.message)
			if len(got) > maxNotificationErrorBytes {
				t.Fatalf("error length = %d, want <= %d", len(got), maxNotificationErrorBytes)
			}
			if !utf8.ValidString(got) {
				t.Fatal("bounded error is not valid UTF-8")
			}
		})
	}
}
