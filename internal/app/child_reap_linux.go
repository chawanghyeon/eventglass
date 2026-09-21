package app

import (
	"errors"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

var nativeSubreaper struct {
	once sync.Once
	err  error
}

func prepareNativeReaping() error {
	// A container may not have an init process which reaps grandchildren. Adopt
	// orphaned native descendants before starting the first child. Wait only on
	// each native child's private group, never on another exec.Cmd's child.
	nativeSubreaper.once.Do(func() { nativeSubreaper.err = unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) })
	return nativeSubreaper.err
}

func reapNativeGroup(pid int) error {
	for {
		var status syscall.WaitStatus
		_, err := syscall.Wait4(-pid, &status, 0, nil)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if errors.Is(err, syscall.ECHILD) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
