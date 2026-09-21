//go:build linux || darwin

package app

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"testing"
	"time"
)

func TestNativeChildCancellationKillsAndReapsDescendants(t *testing.T) {
	checkNativeDescendantLifetime(t, "stubborn")
}

func TestNativeChildCannotReportSuccessAndLeaveDescendants(t *testing.T) {
	checkNativeDescendantLifetime(t, "orphan")
}

func checkNativeDescendantLifetime(t *testing.T, mode string) {
	t.Helper()
	pidPath := filepath.Join(t.TempDir(), "descendant.pid")
	ctx, cancel := context.WithCancel(context.Background())
	command := exec.Command(os.Args[0], "-test.run=^TestNativeChildHelperProcess$")
	command.Env = append(os.Environ(), "EVENTGLASS_NATIVE_HELPER_MODE="+mode, "EVENTGLASS_NATIVE_HELPER_PID="+pidPath)
	var stdout, stderr boundedBuffer
	command.Stdout, command.Stderr = &stdout, &stderr
	finished := make(chan struct{})
	var result error
	go func() { defer close(finished); result = runNativeChild(ctx, command) }()
	var descendant int
	t.Cleanup(func() {
		cancel()
		// Failure cleanup is restricted to the PID written by our private
		// helper, never the test runner's or an inherited process group.
		if descendant > 0 {
			_ = syscall.Kill(descendant, syscall.SIGKILL)
		}
		<-finished
	})
	readyDeadline := time.Now().Add(5 * time.Second)
	for descendant == 0 && time.Now().Before(readyDeadline) {
		data, err := os.ReadFile(pidPath)
		if err == nil {
			descendant, err = strconv.Atoi(string(data))
			if err != nil || descendant <= 0 {
				t.Fatalf("invalid helper PID: %q", data)
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if descendant == 0 {
		t.Fatal("native descendant did not start")
	}
	if mode == "stubborn" {
		cancel()
	}
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("native process group did not join after termination")
	}
	if mode == "stubborn" && !errors.Is(result, context.Canceled) {
		t.Fatalf("cancellation result=%v", result)
	}
	if mode == "orphan" && result == nil {
		t.Fatal("leader exit zero hid a live descendant")
	}
	if err := syscall.Kill(-command.Process.Pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("native process group still exists after join: %v", err)
	}
	if runtime.GOOS == "darwin" {
		// Darwin can remove the terminated process from its group before init
		// removes its PID. Unlike Linux's subreaper, we cannot Wait4 that orphan.
		// The group must already be gone above; only allow eventual PID reaping.
		deadline := time.Now().Add(time.Second)
		for syscall.Kill(descendant, 0) == nil && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
	}
	if err := syscall.Kill(descendant, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("descendant was not reaped: pid=%d err=%v", descendant, err)
	}
	descendant = 0 // It has been reaped; cleanup must not target a reused PID.
}

func TestNativeChildSuccessAndCanceledBeforeStart(t *testing.T) {
	unrelated := exec.Command(os.Args[0], "-test.run=^TestNativeChildHelperProcess$")
	unrelated.Env = append(os.Environ(), "EVENTGLASS_NATIVE_HELPER_MODE=success")
	if err := unrelated.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if unrelated.ProcessState == nil {
			_ = unrelated.Process.Kill()
			_ = unrelated.Wait()
		}
	})
	command := exec.Command(os.Args[0], "-test.run=^TestNativeChildHelperProcess$")
	command.Env = append(os.Environ(), "EVENTGLASS_NATIVE_HELPER_MODE=success")
	if err := runNativeChild(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	if err := unrelated.Wait(); err != nil {
		t.Fatalf("native group reaped another command's child: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	command = exec.Command("/eventglass-nonexistent-private-test-command")
	if err := runNativeChild(ctx, command); !errors.Is(err, context.Canceled) || command.Process != nil {
		t.Fatalf("canceled child started: process=%v err=%v", command.Process, err)
	}
}

func TestNativeChildHelperProcess(t *testing.T) {
	mode := os.Getenv("EVENTGLASS_NATIVE_HELPER_MODE")
	if mode == "" {
		return
	}
	if mode == "success" {
		os.Exit(0)
	}
	if mode != "descendant" && mode != "stubborn" && mode != "orphan" {
		os.Exit(2)
	}
	signal.Ignore(syscall.SIGTERM)
	pidPath := os.Getenv("EVENTGLASS_NATIVE_HELPER_PID")
	if !filepath.IsAbs(pidPath) {
		os.Exit(2)
	}
	if mode == "descendant" {
		if err := os.WriteFile(pidPath+".ready", []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
			os.Exit(2)
		}
		if err := os.Rename(pidPath+".ready", pidPath); err != nil {
			os.Exit(2)
		}
		for {
			time.Sleep(time.Hour)
		}
	}
	child := exec.Command(os.Args[0], "-test.run=^TestNativeChildHelperProcess$")
	child.Env = append(os.Environ(), "EVENTGLASS_NATIVE_HELPER_MODE=descendant")
	child.Stdout, child.Stderr = os.Stdout, os.Stderr
	if err := child.Start(); err != nil {
		os.Exit(2)
	}
	if mode == "orphan" {
		for {
			if _, err := os.Stat(pidPath); err == nil {
				os.Exit(0)
			}
			time.Sleep(time.Millisecond)
		}
	}
	_ = child.Wait()
	os.Exit(0)
}
