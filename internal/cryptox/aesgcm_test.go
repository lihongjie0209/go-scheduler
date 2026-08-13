package cryptox

import (
	"bytes"
	"encoding/base64"
	"testing"
)

func TestKeyringRoundTrip(t *testing.T) {
	t.Parallel()
	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
	ring, err := NewKeyring(3, key)
	if err != nil {
		t.Fatal(err)
	}
	plain := []byte(`{"Authorization":"secret"}`)
	encrypted, version, err := ring.Encrypt(plain)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encrypted, []byte("secret")) {
		t.Fatal("ciphertext leaked plaintext")
	}
	got, err := ring.Decrypt(encrypted, version)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("got %s", got)
	}
}
func TestKeyringRejectsTampering(t *testing.T) {
	t.Parallel()
	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{9}, 32))
	ring, _ := NewKeyring(1, key)
	encrypted, _, _ := ring.Encrypt([]byte("secret"))
	encrypted[len(encrypted)-1] ^= 1
	if _, err := ring.Decrypt(encrypted, 1); err == nil {
		t.Fatal("expected authentication failure")
	}
}
