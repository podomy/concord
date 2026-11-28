// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package proc_test

import (
	"bytes"
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/proc"
)

// A true binary exits zero with its code visible on Exited.
func TestOsLauncherTrueExitsZero(t *testing.T) {
	t.Parallel()

	h, err := proc.OsLauncher{}.Start(context.Background(), proc.StartConfig{ID: uuid.New(), BinPath: "/bin/true"})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	select {
	case st := <-h.Exited():
		if st.Code != 0 {
			t.Fatalf("code = %d, want 0", st.Code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no exit within bound")
	}
}

// A false binary exits non-zero; the wait error is reported alongside.
func TestOsLauncherFalseExitsNonZero(t *testing.T) {
	t.Parallel()

	h, err := proc.OsLauncher{}.Start(context.Background(), proc.StartConfig{ID: uuid.New(), BinPath: "/bin/false"})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	select {
	case st := <-h.Exited():
		if st.Code == 0 {
			t.Fatal("code = 0, want non-zero")
		}
		if st.Err == nil {
			t.Fatal("missing wait error for non-zero exit")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no exit within bound")
	}
}

// Stdout, env, and args reach the child: /bin/sh echoes back.
func TestOsLauncherWiresStdioEnvArgs(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	cfg := proc.StartConfig{
		ID:      uuid.New(),
		BinPath: "/bin/sh",
		Args:    []string{"-c", "echo $CONCORD_PROBE_TAG"},
		Env:     []string{"CONCORD_PROBE_TAG=hello-proc"},
		Stdout:  &out,
	}
	h, err := proc.OsLauncher{}.Start(context.Background(), cfg)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	select {
	case st := <-h.Exited():
		if st.Code != 0 {
			t.Fatalf("code = %d, want 0", st.Code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no exit within bound")
	}
	if out.String() != "hello-proc\n" {
		t.Fatalf("stdout = %q, want hello-probe echo", out.String())
	}
}

// A missing binary fails Start with no handle and no spawn.
func TestOsLauncherMissingBinaryFails(t *testing.T) {
	t.Parallel()

	h, err := proc.OsLauncher{}.Start(context.Background(), proc.StartConfig{ID: uuid.New(), BinPath: "/nonexistent-concord-probe"})
	if err == nil {
		t.Fatal("expected start error, got nil")
	}
	if h != nil {
		t.Fatal("expected nil handle on start failure")
	}
}

// Signal delivery works: SIGTERM ends a sleeping child.
func TestOsLauncherSignalTerminatesSleep(t *testing.T) {
	t.Parallel()

	h, err := proc.OsLauncher{}.Start(context.Background(), proc.StartConfig{ID: uuid.New(), BinPath: "/bin/sleep", Args: []string{"30"}})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if h.PID() <= 0 {
		t.Fatalf("pid = %d, want positive", h.PID())
	}
	err = h.Signal(syscall.SIGTERM)
	if err != nil {
		t.Fatalf("signal: %v", err)
	}
	select {
	case <-h.Exited():
	case <-time.After(5 * time.Second):
		t.Fatal("no exit after SIGTERM within bound")
	}
}

// The fake records configs and replays its scripted status.
func TestFakeLauncherRecordsAndReplays(t *testing.T) {
	t.Parallel()

	fake := &proc.FakeLauncher{Status: proc.ExitStatus{Code: 3}}
	h, err := fake.Start(context.Background(), proc.StartConfig{ID: uuid.New(), BinPath: "/bin/app", Args: []string{"a"}})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if fake.Calls() != 1 {
		t.Fatalf("calls = %d, want 1", fake.Calls())
	}
	st := <-h.Exited()
	if st.Code != 3 {
		t.Fatalf("code = %d, want 3", st.Code)
	}
}

// The fake StartErr fails Start with no handle.
func TestFakeLauncherStartErr(t *testing.T) {
	t.Parallel()

	fake := &proc.FakeLauncher{StartErr: errors.New("boom")}
	h, err := fake.Start(context.Background(), proc.StartConfig{})
	if err == nil {
		t.Fatal("expected start error, got nil")
	}
	if h != nil {
		t.Fatal("expected nil handle on start failure")
	}
}

// Nil streams discard: a noisy child with nowhere to write still exits.
func TestOsLauncherNilStreamsDiscard(t *testing.T) {
	t.Parallel()

	h, err := proc.OsLauncher{}.Start(context.Background(), proc.StartConfig{ID: uuid.New(), BinPath: "/bin/sh", Args: []string{"-c", "echo out; echo err >&2"}})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	select {
	case st := <-h.Exited():
		if st.Code != 0 {
			t.Fatalf("code = %d, want 0", st.Code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no exit within bound")
	}
}
