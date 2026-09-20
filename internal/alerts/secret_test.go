package alerts

import "testing"

func TestSecretCipherBindsKeyIDAndUsesFreshNonce(t *testing.T) {
	key := [32]byte{1}
	cipher, err := NewSecretCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	first, _ := cipher.Encrypt("secret")
	second, _ := cipher.Encrypt("secret")
	if string(first) == string(second) {
		t.Fatal("encryption nonce was reused")
	}
	plain, err := cipher.Decrypt(cipher.KeyID(), first)
	if err != nil || string(plain) != "secret" {
		t.Fatalf("decrypt: %q %v", plain, err)
	}
	if _, err := cipher.Decrypt("wrong", first); err == nil {
		t.Fatal("wrong key id accepted")
	}
}
