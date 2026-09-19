package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
)

// FileEvidence is content evidence computed from finished immutable bytes.
// Blocks are always fixed at DefaultBlockSize except for the final block.
type FileEvidence struct {
	Bytes       int64    `json:"bytes"`
	SHA256      string   `json:"sha256"`
	BlockSize   int64    `json:"block_size"`
	BlockSHA256 []string `json:"block_sha256"`
}

func InspectFile(path string) (FileEvidence, error) {
	file, err := os.Open(path)
	if err != nil {
		return FileEvidence{}, err
	}
	defer file.Close()

	whole := sha256.New()
	buffer := make([]byte, DefaultBlockSize)
	evidence := FileEvidence{BlockSize: DefaultBlockSize, BlockSHA256: make([]string, 0)}
	for {
		count, readErr := io.ReadFull(file, buffer)
		if count > 0 {
			block := sha256.Sum256(buffer[:count])
			evidence.BlockSHA256 = append(evidence.BlockSHA256, hex.EncodeToString(block[:]))
			_, _ = whole.Write(buffer[:count])
			evidence.Bytes += int64(count)
		}
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
		if readErr != nil {
			return FileEvidence{}, readErr
		}
	}
	evidence.SHA256 = hex.EncodeToString(whole.Sum(nil))
	return evidence, nil
}

func VerifyFile(path string, expected FileEvidence) error {
	if expected.Bytes <= 0 || expected.BlockSize != DefaultBlockSize || len(expected.BlockSHA256) != int((expected.Bytes+DefaultBlockSize-1)/DefaultBlockSize) {
		return errors.New("invalid file evidence")
	}
	actual, err := InspectFile(path)
	if err != nil {
		return err
	}
	if actual.Bytes != expected.Bytes || actual.SHA256 != expected.SHA256 || len(actual.BlockSHA256) != len(expected.BlockSHA256) {
		return errors.New("file evidence mismatch")
	}
	for index := range actual.BlockSHA256 {
		if actual.BlockSHA256[index] != expected.BlockSHA256[index] {
			return errors.New("file block checksum mismatch")
		}
	}
	return nil
}
