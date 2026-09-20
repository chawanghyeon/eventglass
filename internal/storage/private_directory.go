package storage

import (
	"errors"
	"os"
)

// EnsurePrivateDirectory rejects a symlink or a publicly accessible scratch root.
func EnsurePrivateDirectory(path string) error {
	if path == "" {
		return errors.New("scratch directory is required")
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return errors.New("scratch directory must be private")
	}
	return nil
}
