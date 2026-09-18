package ingest

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strings"
)

func NewAcceptanceID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(value[:])
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:], nil
}

func normalizeEventID(value string) (string, bool) {
	normalized := strings.ToLower(strings.ReplaceAll(value, "-", ""))
	if len(normalized) != 32 || allZero(normalized) {
		return "", false
	}
	if _, err := hex.DecodeString(normalized); err != nil {
		return "", false
	}
	return normalized, true
}

func normalizeTraceID(value string, width int) (string, bool) {
	normalized := strings.ToLower(value)
	if len(normalized) != width || allZero(normalized) {
		return "", false
	}
	if _, err := hex.DecodeString(normalized); err != nil {
		return "", false
	}
	return normalized, true
}

func allZero(value string) bool {
	return strings.Trim(value, "0") == ""
}

func recordID(parts ...string) string {
	hash := sha256.New()
	hash.Write([]byte("eventglass-record-id-v1\x00"))
	var length [8]byte
	for _, part := range parts {
		binary.BigEndian.PutUint64(length[:], uint64(len(part)))
		hash.Write(length[:])
		hash.Write([]byte(part))
	}
	return hex.EncodeToString(hash.Sum(nil))
}
