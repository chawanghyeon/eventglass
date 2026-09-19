package api

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/chawanghyeon/eventglass/internal/resource"
	"golang.org/x/crypto/argon2"
)

const (
	argonMemoryKiB = 64 * 1024
	argonTime      = 3
	argonThreads   = 2
	argonKeyBytes  = 32
	argonSaltBytes = 16
	passwordMin    = 12
	passwordMax    = 1024
)

var errInvalidPassword = errors.New("invalid password")

type PasswordHasher struct {
	budget *resource.Budget
	queue  chan struct{}
	active chan struct{}
	dummy  string
}

func NewPasswordHasher(budget *resource.Budget) (*PasswordHasher, error) {
	if budget == nil {
		return nil, errors.New("password hash budget is required")
	}
	hasher := &PasswordHasher{budget: budget, queue: make(chan struct{}, 8), active: make(chan struct{}, 1)}
	// This fixed, syntactically valid hash follows the exact expensive path for
	// unknown accounts. It is not a credential and is never accepted as one.
	hasher.dummy = "$argon2id$v=19$m=65536,t=3,p=2$ZXZlbnRnbGFzcy1kdW1teQ$3fS7jbTBnU4p8kMPSY1XANFKxf3f6OUt0OXbKsCB9BA"
	return hasher, nil
}

func (hasher *PasswordHasher) Hash(password string) (string, error) {
	if len(password) < passwordMin || len(password) > passwordMax {
		return "", errInvalidPassword
	}
	release, err := hasher.admit()
	if err != nil {
		return "", err
	}
	defer release()
	salt := make([]byte, argonSaltBytes)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	key := argon2.IDKey([]byte(password), salt, argonTime, argonMemoryKiB, argonThreads, argonKeyBytes)
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s", argon2.Version, argonMemoryKiB, argonTime, argonThreads,
		base64.RawStdEncoding.EncodeToString(salt), base64.RawStdEncoding.EncodeToString(key)), nil
}

func (hasher *PasswordHasher) Verify(password, encoded string) (bool, error) {
	if len(password) > passwordMax {
		return false, errInvalidPassword
	}
	parameters, salt, expected, err := parsePasswordPHC(encoded)
	if err != nil {
		return false, err
	}
	release, err := hasher.admit()
	if err != nil {
		return false, err
	}
	defer release()
	actual := argon2.IDKey([]byte(password), salt, parameters.time, parameters.memory, parameters.threads, uint32(len(expected)))
	return subtle.ConstantTimeCompare(actual, expected) == 1, nil
}

func (hasher *PasswordHasher) VerifyUnknown(password string) error {
	_, err := hasher.Verify(password, hasher.dummy)
	return err
}

func (hasher *PasswordHasher) admit() (func(), error) {
	select {
	case hasher.queue <- struct{}{}:
	default:
		return nil, resource.ErrLimited
	}
	hasher.active <- struct{}{}
	permit, err := hasher.budget.Acquire(argonMemoryKiB << 10)
	if err != nil {
		<-hasher.active
		<-hasher.queue
		return nil, err
	}
	return func() {
		permit.Release()
		<-hasher.active
		<-hasher.queue
	}, nil
}

type argonParameters struct {
	memory  uint32
	time    uint32
	threads uint8
}

func parsePasswordPHC(encoded string) (argonParameters, []byte, []byte, error) {
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[0] != "" || parts[1] != "argon2id" || parts[2] != "v=19" {
		return argonParameters{}, nil, nil, errors.New("unsupported password hash")
	}
	values := strings.Split(parts[3], ",")
	if len(values) != 3 {
		return argonParameters{}, nil, nil, errors.New("invalid password hash parameters")
	}
	parse := func(value, prefix string, bits int) (uint64, error) {
		if !strings.HasPrefix(value, prefix) {
			return 0, errors.New("invalid password hash parameters")
		}
		return strconv.ParseUint(strings.TrimPrefix(value, prefix), 10, bits)
	}
	memory, err := parse(values[0], "m=", 32)
	if err != nil {
		return argonParameters{}, nil, nil, err
	}
	timeCost, err := parse(values[1], "t=", 32)
	if err != nil {
		return argonParameters{}, nil, nil, err
	}
	threads, err := parse(values[2], "p=", 8)
	if err != nil {
		return argonParameters{}, nil, nil, err
	}
	if memory < 8*1024 || memory > argonMemoryKiB || timeCost < 1 || timeCost > argonTime || threads < 1 || threads > argonThreads {
		return argonParameters{}, nil, nil, errors.New("password hash parameters exceed policy")
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil || len(salt) < 16 || len(salt) > 32 {
		return argonParameters{}, nil, nil, errors.New("invalid password hash salt")
	}
	key, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil || len(key) != argonKeyBytes {
		return argonParameters{}, nil, nil, errors.New("invalid password hash output")
	}
	return argonParameters{memory: uint32(memory), time: uint32(timeCost), threads: uint8(threads)}, salt, key, nil
}
