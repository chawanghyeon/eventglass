package alerts

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
)

type SecretCipher struct {
	aead  cipher.AEAD
	keyID string
}

func NewSecretCipher(key [32]byte) (*SecretCipher, error) {
	if key == ([32]byte{}) {
		return nil, errors.New("alert encryption key is required")
	}
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(key[:])
	return &SecretCipher{aead: aead, keyID: hex.EncodeToString(digest[:8])}, nil
}

func (cipher *SecretCipher) KeyID() string { return cipher.keyID }

func (cipher *SecretCipher) Encrypt(secret string) ([]byte, error) {
	if cipher == nil || cipher.aead == nil || len(secret) > 4096 {
		return nil, errors.New("invalid alert secret")
	}
	if secret == "" {
		return nil, nil
	}
	nonce := make([]byte, cipher.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return cipher.aead.Seal(nonce, nonce, []byte(secret), []byte(cipher.keyID)), nil
}

func (cipher *SecretCipher) Decrypt(keyID string, encrypted []byte) ([]byte, error) {
	if cipher == nil || keyID != cipher.keyID || len(encrypted) < cipher.aead.NonceSize()+cipher.aead.Overhead() {
		return nil, errors.New("alert secret key unavailable")
	}
	nonce := encrypted[:cipher.aead.NonceSize()]
	return cipher.aead.Open(nil, nonce, encrypted[cipher.aead.NonceSize():], []byte(keyID))
}
