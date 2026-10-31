// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cn

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestBuildPortRulespec(t *testing.T) {
	tests := []struct {
		name          string
		cidr          string
		hostPort      uint16
		containerPort uint16
		want          []string
		wantErr       bool
	}{
		{
			name:          "basic mapping",
			cidr:          "10.0.0.5/24",
			hostPort:      8080,
			containerPort: 80,
			want:          []string{"-p", "tcp", "--dport", "8080", "-j", "DNAT", "--to-destination", "10.0.0.5:80"},
		},
		{
			name:          "max ports",
			cidr:          "10.0.0.1/16",
			hostPort:      65535,
			containerPort: 65535,
			want:          []string{"-p", "tcp", "--dport", "65535", "-j", "DNAT", "--to-destination", "10.0.0.1:65535"},
		},
		{
			name:          "zero ports",
			cidr:          "10.0.0.1/16",
			hostPort:      0,
			containerPort: 0,
			want:          []string{"-p", "tcp", "--dport", "0", "-j", "DNAT", "--to-destination", "10.0.0.1:0"},
		},
		{
			name:          "missing slash",
			cidr:          "10.0.0.5",
			hostPort:      8080,
			containerPort: 80,
			wantErr:       true,
		},
		{
			name:          "empty cidr",
			cidr:          "",
			hostPort:      8080,
			containerPort: 80,
			wantErr:       true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildPortRulespec(tt.cidr, tt.hostPort, tt.containerPort)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error for CIDR %q, got nil", tt.cidr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("expected %v, got %v", tt.want, got)
			}
		})
	}
}

// installIptablesStub writes stub as an executable iptables on PATH and
// prepares env-based control and log files. It returns the log path.
func installIptablesStub(t *testing.T, stub, logFile, envLog, envFail, envPresent string) string {
	t.Helper()
	dir := t.TempDir()
	stubPath := filepath.Join(dir, "iptables")
	err := os.WriteFile(stubPath, []byte(stub), 0o755) //nolint:gosec // test stub must be executable to run via PATH
	if err != nil {
		t.Fatalf("write fake iptables: %v", err)
	}
	logPath := filepath.Join(dir, logFile)
	t.Setenv(envLog, logPath)
	t.Setenv(envFail, "")
	t.Setenv(envPresent, "")
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return logPath
}

// fakeIptables installs a stub iptables on PATH. The stub appends each
// argv line to a log file and answers -C probes from
// FAKE_IPTABLES_RULE_PRESENT (1 means present). Setting
// FAKE_IPTABLES_FAIL to 1 makes every call exit 2. It returns the log path.
func fakeIptables(t *testing.T) string {
	t.Helper()
	stub := `#!/bin/sh
if [ "$FAKE_IPTABLES_FAIL" = "1" ]; then
	echo "iptables: Something broke." >&2
	exit 2
fi
echo "$@" >> "$FAKE_IPTABLES_LOG"
for a in "$@"; do
	if [ "$a" = "-C" ]; then
		if [ "$FAKE_IPTABLES_RULE_PRESENT" = "1" ]; then
			exit 0
		fi
		echo "iptables: Bad rule (does a matching rule exist in that chain?)." >&2
		exit 1
	fi
done
exit 0
`
	return installIptablesStub(t, stub, "argv.log", "FAKE_IPTABLES_LOG", "FAKE_IPTABLES_FAIL", "FAKE_IPTABLES_RULE_PRESENT")
}

// loggedLines reads stub argv lines, returning nil when nothing ran.
func loggedLines(t *testing.T, logPath string) []string {
	t.Helper()
	data, err := os.ReadFile(logPath) //nolint:gosec // logPath is a temp dir file created by the test
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("read iptables log: %v", err)
	}
	text := strings.TrimSpace(string(data))
	if text == "" {
		return nil
	}
	return strings.Split(text, "\n")
}

// assertWaitFlag fails when any logged invocation lacks the -w flag.
func assertWaitFlag(t *testing.T, lines []string) {
	t.Helper()
	for _, line := range lines {
		if !strings.Contains(" "+line+" ", " -w ") {
			t.Fatalf("expected -w wait flag in %q", line)
		}
	}
}

func TestSetupMasqueradeRuleBytes(t *testing.T) {
	logPath := fakeIptables(t)
	err := SetupMasquerade(context.Background())
	if err != nil {
		t.Fatalf("setup masquerade: %v", err)
	}
	lines := loggedLines(t, logPath)
	if len(lines) != 2 {
		t.Fatalf("expected -C probe plus -A append, got %v", lines)
	}
	want := "-w -t nat -A POSTROUTING -s 10.0.0.0/16 ! -o cn0 -j MASQUERADE"
	if lines[1] != want {
		t.Fatalf("expected %q, got %q", want, lines[1])
	}
	assertWaitFlag(t, lines)
}

func TestTeardownMasqueradeDeletesPresent(t *testing.T) {
	logPath := fakeIptables(t)
	t.Setenv("FAKE_IPTABLES_RULE_PRESENT", "1")
	err := TeardownMasquerade()
	if err != nil {
		t.Fatalf("teardown masquerade: %v", err)
	}
	lines := loggedLines(t, logPath)
	if len(lines) != 2 {
		t.Fatalf("expected -C probe plus -D delete, got %v", lines)
	}
	want := "-w -t nat -D POSTROUTING -s 10.0.0.0/16 ! -o cn0 -j MASQUERADE"
	if lines[1] != want {
		t.Fatalf("expected %q, got %q", want, lines[1])
	}
	assertWaitFlag(t, lines)
}

func TestAddPortMappingRuleBytes(t *testing.T) {
	logPath := fakeIptables(t)
	err := AddPortMapping(context.Background(), 8080, "10.0.0.5/24", 80)
	if err != nil {
		t.Fatalf("add port mapping: %v", err)
	}
	lines := loggedLines(t, logPath)
	if len(lines) != 2 {
		t.Fatalf("expected -C probe plus -A append, got %v", lines)
	}
	want := "-w -t nat -A PREROUTING -p tcp --dport 8080 -j DNAT --to-destination 10.0.0.5:80"
	if lines[1] != want {
		t.Fatalf("expected %q, got %q", want, lines[1])
	}
	assertWaitFlag(t, lines)
}

func TestAppendUniqueSkipsExisting(t *testing.T) {
	logPath := fakeIptables(t)
	t.Setenv("FAKE_IPTABLES_RULE_PRESENT", "1")
	err := appendUnique(context.Background(), "nat", "POSTROUTING",
		"-s", "10.0.0.0/16", "!", "-o", "cn0", "-j", "MASQUERADE")
	if err != nil {
		t.Fatalf("append unique: %v", err)
	}
	lines := loggedLines(t, logPath)
	if len(lines) != 1 {
		t.Fatalf("expected only the -C probe, got %v", lines)
	}
	if !strings.Contains(lines[0], "-C") {
		t.Fatalf("expected a -C probe, got %q", lines[0])
	}
	if strings.Contains(lines[0], "-A") {
		t.Fatalf("must not append an existing rule, got %q", lines[0])
	}
	assertWaitFlag(t, lines)
}

func TestAppendUniqueAddsMissing(t *testing.T) {
	logPath := fakeIptables(t)
	err := appendUnique(context.Background(), "nat", "POSTROUTING", "-j", "MASQUERADE")
	if err != nil {
		t.Fatalf("append unique: %v", err)
	}
	lines := loggedLines(t, logPath)
	if len(lines) != 2 {
		t.Fatalf("expected -C probe plus -A append, got %v", lines)
	}
	if !strings.Contains(lines[1], "-A") {
		t.Fatalf("expected an -A append, got %q", lines[1])
	}
	assertWaitFlag(t, lines)
}

func TestDeleteIfExistsSkipsMissing(t *testing.T) {
	logPath := fakeIptables(t)
	err := deleteIfExists(context.Background(), "nat", "POSTROUTING", "-j", "MASQUERADE")
	if err != nil {
		t.Fatalf("delete if exists: %v", err)
	}
	lines := loggedLines(t, logPath)
	if len(lines) != 1 {
		t.Fatalf("expected only the -C probe, got %v", lines)
	}
	if !strings.Contains(lines[0], "-C") {
		t.Fatalf("expected a -C probe, got %q", lines[0])
	}
	if strings.Contains(lines[0], "-D") {
		t.Fatalf("must not delete a missing rule, got %q", lines[0])
	}
	assertWaitFlag(t, lines)
}

func TestDeleteIfExistsDeletesPresent(t *testing.T) {
	logPath := fakeIptables(t)
	t.Setenv("FAKE_IPTABLES_RULE_PRESENT", "1")
	err := deleteIfExists(context.Background(), "nat", "POSTROUTING", "-j", "MASQUERADE")
	if err != nil {
		t.Fatalf("delete if exists: %v", err)
	}
	lines := loggedLines(t, logPath)
	if len(lines) != 2 {
		t.Fatalf("expected -C probe plus -D delete, got %v", lines)
	}
	if !strings.Contains(lines[1], "-D") {
		t.Fatalf("expected a -D delete, got %q", lines[1])
	}
	assertWaitFlag(t, lines)
}

func TestRemovePortMappingSkipsMissing(t *testing.T) {
	logPath := fakeIptables(t)
	err := RemovePortMapping(context.Background(), 8080, "10.0.0.5/24", 80)
	if err != nil {
		t.Fatalf("remove port mapping: %v", err)
	}
	lines := loggedLines(t, logPath)
	if len(lines) != 1 {
		t.Fatalf("expected only the -C probe, got %v", lines)
	}
	assertWaitFlag(t, lines)
}

func TestIptablesProbeFailureSurfaced(t *testing.T) {
	fakeIptables(t)
	t.Setenv("FAKE_IPTABLES_FAIL", "1")
	err := appendUnique(context.Background(), "nat", "POSTROUTING", "-j", "MASQUERADE")
	if err == nil {
		t.Fatal("expected error when iptables fails")
	}
	if !strings.Contains(err.Error(), "Something broke") {
		t.Fatalf("expected stderr in error, got %v", err)
	}
	err = deleteIfExists(context.Background(), "nat", "POSTROUTING", "-j", "MASQUERADE")
	if err == nil {
		t.Fatal("expected error when iptables fails")
	}
}
