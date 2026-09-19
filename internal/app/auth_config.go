package app

import (
	"encoding/hex"
	"errors"
	"os"
	"strings"
)

func readHexSecret(path string) ([32]byte, error) {
	if path == "" {
		return [32]byte{}, errors.New("secret file path is required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return [32]byte{}, err
	}
	decoded, err := hex.DecodeString(strings.TrimSpace(string(data)))
	if err != nil || len(decoded) != 32 {
		return [32]byte{}, errors.New("secret file must contain exactly 32 bytes as lowercase hexadecimal")
	}
	if strings.TrimSpace(string(data)) != hex.EncodeToString(decoded) {
		return [32]byte{}, errors.New("secret file must use canonical lowercase hexadecimal")
	}
	var result [32]byte
	copy(result[:], decoded)
	return result, nil
}
