package store

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"
)

type failingNotificationCipher struct{}

func (failingNotificationCipher) Encrypt([]byte) ([]byte, int, error) {
	return nil, 0, errors.New("not used")
}
func (failingNotificationCipher) Decrypt([]byte, int) ([]byte, error) {
	return nil, errors.New("secret key details must not escape")
}

func TestLoadNotificationDeliveryConfigIsFailClosedAndRedacted(t *testing.T) {
	t.Parallel()
	version := 1
	for _, test := range []struct {
		name    string
		cipher  HeaderCipher
		version *int
	}{
		{name: "missing cipher", version: &version},
		{name: "missing version", cipher: failingNotificationCipher{}},
		{name: "decrypt failure", cipher: failingNotificationCipher{}, version: &version},
	} {
		t.Run(test.name, func(t *testing.T) {
			plain, err := loadNotificationDeliveryConfig(test.cipher, []byte("ciphertext"), test.version)
			if !errors.Is(err, ErrNotificationConfigUnreadable) || plain != nil {
				t.Fatalf("plain=%s error=%v", plain, err)
			}
			if strings.Contains(err.Error(), "secret key details") {
				t.Fatal("decryption error leaked provider details")
			}
		})
	}
}

func TestValidNotificationEventJobID(t *testing.T) {
	t.Parallel()
	valid := "3e44d267-8f43-4224-a972-b5fe637d95d2"
	if got := validNotificationEventJobID(valid); got != valid {
		t.Fatalf("valid job ID = %q", got)
	}
	if got := validNotificationEventJobID("3e44d2678f434224a972b5fe637d95d2"); got != valid {
		t.Fatalf("compact job ID normalized to %q", got)
	}
	for _, raw := range []string{"", "not-a-uuid", " 3e44d267-8f43-4224-a972-b5fe637d95d2"} {
		if got := validNotificationEventJobID(raw); got != "" {
			t.Errorf("invalid job ID %q normalized to %q", raw, got)
		}
	}
}

func TestEncodeNotificationConfigRequiresCipher(t *testing.T) {
	t.Parallel()
	plaintext, encrypted, version, err := (&Store{}).encodeNotificationConfig(json.RawMessage(`{"url":"https://hooks.example.com?token=secret"}`))
	if err == nil || !strings.Contains(err.Error(), "requires store cipher") {
		t.Fatalf("encode error = %v", err)
	}
	if plaintext != nil || encrypted != nil || version != nil {
		t.Fatalf("unencrypted notification config returned data: plaintext=%s encrypted=%x version=%v", plaintext, encrypted, version)
	}
}

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
