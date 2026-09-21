package app

import (
	"errors"
	"syscall"
	"time"
)

func prepareNativeReaping() error { return nil }

func reapNativeGroup(pid int) error {
	// Darwin reparents orphans to init rather than to a configurable subreaper.
	// Keep ownership until the terminated group disappears, not merely until
	// its leader returns from Wait. Init may remove an orphan's PID slightly
	// later; Darwin offers no Linux-style orphan adoption/Wait4 contract.
	for {
		err := syscall.Kill(-pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		// An orphan adopted by init can still occupy the group while no
		// longer being signalable by this user. EPERM is not proof of exit.
		if err != nil && !errors.Is(err, syscall.EPERM) {
			return err
		}
		time.Sleep(time.Millisecond)
	}
}
