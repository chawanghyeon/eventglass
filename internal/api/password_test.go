package api

import (
	"strings"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/resource"
)

func TestPasswordHasherArgon2PolicyAndStableCSRF(t *testing.T) {
	hasher, err := NewPasswordHasher(resource.NewBudget(64 << 20))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := hasher.Hash("correct horse battery staple")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(encoded, "$argon2id$v=19$m=65536,t=3,p=2$") {
		t.Fatalf("hash parameters=%q", encoded)
	}
	valid, err := hasher.Verify("correct horse battery staple", encoded)
	if err != nil || !valid {
		t.Fatalf("valid=%v err=%v", valid, err)
	}
	valid, err = hasher.Verify("wrong password", encoded)
	if err != nil || valid {
		t.Fatalf("wrong valid=%v err=%v", valid, err)
	}
	if _, err := hasher.Hash("short"); err != errInvalidPassword {
		t.Fatalf("short password=%v", err)
	}
	if _, _, _, err := parsePasswordPHC(strings.Replace(encoded, "m=65536", "m=131072", 1)); err == nil {
		t.Fatal("oversized Argon2 parameters were accepted")
	}
	secret := []byte("01234567890123456789012345678901")
	first, firstHash := csrfForSecret(secret)
	second, secondHash := csrfForSecret(secret)
	if first != second || firstHash != secondHash || len(first) != 43 {
		t.Fatalf("csrf token is unstable: %q %q", first, second)
	}
	other, _ := csrfForSecret([]byte("11234567890123456789012345678901"))
	if other == first {
		t.Fatal("different sessions share a CSRF token")
	}
}

func TestPasswordUnknownAccountUsesValidDummyHash(t *testing.T) {
	hasher, err := NewPasswordHasher(resource.NewBudget(64 << 20))
	if err != nil {
		t.Fatal(err)
	}
	if err := hasher.VerifyUnknown("arbitrary unknown password"); err != nil {
		t.Fatal(err)
	}
}

func TestPasswordHashRequiresFullMemoryReservation(t *testing.T) {
	hasher, err := NewPasswordHasher(resource.NewBudget((64 << 20) - 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hasher.Hash("correct horse battery staple"); err != resource.ErrLimited {
		t.Fatalf("undersized hash budget=%v", err)
	}
}
