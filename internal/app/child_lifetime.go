//go:build linux || darwin

package app

import (
	"context"
	"errors"
	"os/exec"
	"syscall"
	"time"
)

// Native children only produce private scratch; durable publication belongs to
// the parent. Bound cooperative exit before KILL without reserving seconds of
// unusable credit in a busy worker's small spare-time budget. Reaping is still
// unconditional and any longer join is charged, never abandoned.
const nativeChildStopGrace = 100 * time.Millisecond

// runNativeChild owns the entire child process group, including cancellation
// escalation and reaping. Callers retain permits and scratch until it returns.
// Commands must be made with exec.Command, not a second cancellation owner.
func runNativeChild(ctx context.Context, command *exec.Cmd) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := prepareNativeReaping(); err != nil {
		return err
	}
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	// Bound inherited pipe draining if a defective child exits while a
	// descendant holds stdout/stderr. Group termination/reaping below still
	// happens before returning to the permit owner.
	command.WaitDelay = nativeChildStopGrace
	if err := command.Start(); err != nil {
		return err
	}
	joined := make(chan error, 1)
	go func() { joined <- command.Wait() }()
	var result error
	select {
	case result = <-joined:
	case <-ctx.Done():
		terminationErr := signalNativeGroup(command.Process.Pid, syscall.SIGTERM)
		timer := time.NewTimer(nativeChildStopGrace)
		select {
		case result = <-joined:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
			terminationErr = errors.Join(terminationErr, signalNativeGroup(command.Process.Pid, syscall.SIGKILL))
			result = <-joined
		}
		result = errors.Join(ctx.Err(), terminationErr, result)
	}
	// Even a successful leader may not leave live descendants behind. Never
	// trust its exit status alone as permission to remove shared scratch.
	if err := syscall.Kill(-command.Process.Pid, 0); err == nil || errors.Is(err, syscall.EPERM) {
		result = errors.Join(result, errors.New("native child left a process group"), signalNativeGroup(command.Process.Pid, syscall.SIGKILL))
	}
	return errors.Join(result, reapNativeGroup(command.Process.Pid))
}

func signalNativeGroup(pid int, signal syscall.Signal) error {
	err := syscall.Kill(-pid, signal)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}
