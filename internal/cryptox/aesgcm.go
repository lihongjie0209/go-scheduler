package cryptox

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
)

type Keyring struct {
	version int
	aead    cipher.AEAD
}

func NewKeyring(version int, encodedKey string) (*Keyring, error) {
	raw, err := base64.StdEncoding.DecodeString(encodedKey)
	if err != nil {
		return nil, fmt.Errorf("decode master key: %w", err)
	}
	block, err := aes.NewCipher(raw)
	if err != nil {
		return nil, fmt.Errorf("create AES cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}
	if version < 1 {
		return nil, fmt.Errorf("key version must be positive")
	}
	return &Keyring{version: version, aead: aead}, nil
}
func (k *Keyring) Encrypt(plain []byte) ([]byte, int, error) {
	nonce := make([]byte, k.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, 0, fmt.Errorf("generate nonce: %w", err)
	}
	return k.aead.Seal(nonce, nonce, plain, nil), k.version, nil
}
func (k *Keyring) Decrypt(ciphertext []byte, version int) ([]byte, error) {
	if version != k.version {
		return nil, fmt.Errorf("unsupported key version %d", version)
	}
	nonceSize := k.aead.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("ciphertext is too short")
	}
	plain, err := k.aead.Open(nil, ciphertext[:nonceSize], ciphertext[nonceSize:], nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt: %w", err)
	}
	return plain, nil
}
