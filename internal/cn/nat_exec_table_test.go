// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cn

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

// xtbFakeIptables installs a PATH stub named iptables for exact argv tests.
// The stub records each argv line to a log file and answers -C probes from
// XTB_IPTABLES_PRESENT (1 means present). Setting XTB_IPTABLES_FAIL to 1
// makes every call print to stderr and exit 2. Setting XTB_IPTABLES_NOCHAIN
// to 1 makes -C print No chain and exit 1. Setting XTB_IPTABLES_BADOPT to 1
// makes -C print unknown option and exit 1. It returns the log path.
// Env names and log file differ from existing tests to avoid collisions.
func xtbFakeIptables(t *testing.T) string {
	t.Helper()
	stub := `#!/bin/sh
if [ "$XTB_IPTABLES_FAIL" = "1" ]; then
	echo "iptables: xtb probe broke." >&2
	exit 2
fi
echo "$@" >> "$XTB_IPTABLES_LOG"
for a in "$@"; do
	if [ "$a" = "-C" ]; then
		if [ "$XTB_IPTABLES_NOCHAIN" = "1" ]; then
			echo "iptables: No chain/target/match by that name." >&2
			exit 1
		fi
		if [ "$XTB_IPTABLES_BADOPT" = "1" ]; then
			echo "iptables: unknown option \"--bad-opt\"." >&2
			exit 1
		fi
		if [ "$XTB_IPTABLES_PRESENT" = "1" ]; then
			exit 0
		fi
		echo "iptables: Bad rule (does a matching rule exist in that chain?)." >&2
		exit 1
	fi
done
exit 0
`
	return installIptablesStub(t, stub, "xtb-argv.log", "XTB_IPTABLES_LOG", "XTB_IPTABLES_FAIL", "XTB_IPTABLES_PRESENT")
}

// xtbFakeSleepingIptables installs a PATH stub that logs argv then sleeps.
// It replaces itself with sleep so context cancel kills the sleep.
func xtbFakeSleepingIptables(t *testing.T) string {
	t.Helper()
	stub := `#!/bin/sh
echo "$@" >> "$XTB_IPTABLES_LOG"
exec sleep 30
`
	return installIptablesStub(t, stub, "xtb-argv.log", "XTB_IPTABLES_LOG", "XTB_IPTABLES_FAIL", "XTB_IPTABLES_PRESENT")
}

// xtbLoggedLines reads stub argv lines, returning nil when nothing ran.
func xtbLoggedLines(t *testing.T, logPath string) []string {
	t.Helper()
	data, err := os.ReadFile(logPath) //nolint:gosec // logPath is a temp dir file created by the test
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("read xtb iptables log: %v", err)
	}
	text := strings.TrimSpace(string(data))
	if text == "" {
		return nil
	}
	return strings.Split(text, "\n")
}

// xtbAssertWaitFlag fails when any logged invocation lacks the -w flag.
func xtbAssertWaitFlag(t *testing.T, lines []string) {
	t.Helper()
	for _, line := range lines {
		if !strings.Contains(" "+line+" ", " -w ") {
			t.Fatalf("expected -w wait flag in %q", line)
		}
	}
}

// xtbAssertWaitFirst fails when any logged invocation does not start with -w.
func xtbAssertWaitFirst(t *testing.T, lines []string) {
	t.Helper()
	for _, line := range lines {
		if !strings.HasPrefix(line, "-w ") {
			t.Fatalf("expected -w first in %q", line)
		}
	}
}

// xtbAssertNoFlag fails when any logged line contains the given flag token.
func xtbAssertNoFlag(t *testing.T, lines []string, flag string) {
	t.Helper()
	needle := " " + flag + " "
	for _, line := range lines {
		if strings.Contains(" "+line+" ", needle) {
			t.Fatalf("expected no %s in %q", flag, line)
		}
	}
}

// xtbCheckLines fails when logged argv does not match want exactly.
func xtbCheckLines(t *testing.T, lines, want []string) {
	t.Helper()
	if len(lines) != len(want) {
		t.Fatalf("expected %v, got %v", want, lines)
	}
	for i := range want {
		if lines[i] != want[i] {
			t.Fatalf("line %d: expected %q, got %q", i, want[i], lines[i])
		}
	}
}

// xtbCheckPortMapping validates probe plus append lines and required tokens.
func xtbCheckPortMapping(t *testing.T, lines []string, wantProbe, wantAppend string) {
	t.Helper()
	if len(lines) != 2 {
		t.Fatalf("expected probe plus append, got %v", lines)
	}
	if lines[0] != wantProbe {
		t.Fatalf("probe: expected %q, got %q", wantProbe, lines[0])
	}
	if lines[1] != wantAppend {
		t.Fatalf("append: expected %q, got %q", wantAppend, lines[1])
	}
	for _, token := range []string{"-p tcp", "--dport", "-j DNAT", "--to-destination"} {
		if !strings.Contains(lines[1], token) {
			t.Fatalf("expected %q in %q", token, lines[1])
		}
	}
}

func TestXtbExactArgvTable(t *testing.T) {
	tests := []struct {
		name    string
		present string
		run     func() error
		want    []string
	}{
		{
			name:    "setup probe plus append",
			present: "",
			run: func() error {
				return SetupMasquerade(context.Background())
			},
			want: []string{
				"-w -t nat -C POSTROUTING -s 10.0.0.0/16 ! -o cn0 -j MASQUERADE",
				"-w -t nat -A POSTROUTING -s 10.0.0.0/16 ! -o cn0 -j MASQUERADE",
			},
		},
		{
			name:    "teardown probe plus delete",
			present: "1",
			run:     TeardownMasquerade,
			want: []string{
				"-w -t nat -C POSTROUTING -s 10.0.0.0/16 ! -o cn0 -j MASQUERADE",
				"-w -t nat -D POSTROUTING -s 10.0.0.0/16 ! -o cn0 -j MASQUERADE",
			},
		},
		{
			name:    "add probe plus append",
			present: "",
			run: func() error {
				return AddPortMapping(context.Background(), 8080, "10.0.0.5/24", 80)
			},
			want: []string{
				"-w -t nat -C PREROUTING -p tcp --dport 8080 -j DNAT --to-destination 10.0.0.5:80",
				"-w -t nat -A PREROUTING -p tcp --dport 8080 -j DNAT --to-destination 10.0.0.5:80",
			},
		},
		{
			name:    "remove probe plus delete",
			present: "1",
			run: func() error {
				return RemovePortMapping(context.Background(), 8080, "10.0.0.5/24", 80)
			},
			want: []string{
				"-w -t nat -C PREROUTING -p tcp --dport 8080 -j DNAT --to-destination 10.0.0.5:80",
				"-w -t nat -D PREROUTING -p tcp --dport 8080 -j DNAT --to-destination 10.0.0.5:80",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logPath := xtbFakeIptables(t)
			if tt.present != "" {
				t.Setenv("XTB_IPTABLES_PRESENT", tt.present)
			}
			err := tt.run()
			if err != nil {
				t.Fatalf("run %s: %v", tt.name, err)
			}
			lines := xtbLoggedLines(t, logPath)
			xtbCheckLines(t, lines, tt.want)
			xtbAssertWaitFlag(t, lines)
			xtbAssertWaitFirst(t, lines)
		})
	}
}

func TestXtbIdempotencyTable(t *testing.T) {
	tests := []struct {
		name    string
		present string
		forbid  string
		run     func(ctx context.Context) error
	}{
		{
			name:    "append present issues only probe",
			present: "1",
			forbid:  "-A",
			run: func(ctx context.Context) error {
				return appendUnique(ctx, "nat", "POSTROUTING",
					"-s", "10.0.0.0/16", "!", "-o", "cn0", "-j", "MASQUERADE")
			},
		},
		{
			name:    "add present issues only probe",
			present: "1",
			forbid:  "-A",
			run: func(ctx context.Context) error {
				return AddPortMapping(ctx, 8080, "10.0.0.5/24", 80)
			},
		},
		{
			name:    "delete missing issues only probe",
			present: "",
			forbid:  "-D",
			run: func(ctx context.Context) error {
				return deleteIfExists(ctx, "nat", "POSTROUTING",
					"-s", "10.0.0.0/16", "!", "-o", "cn0", "-j", "MASQUERADE")
			},
		},
		{
			name:    "remove missing issues only probe",
			present: "",
			forbid:  "-D",
			run: func(ctx context.Context) error {
				return RemovePortMapping(ctx, 8080, "10.0.0.5/24", 80)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logPath := xtbFakeIptables(t)
			if tt.present != "" {
				t.Setenv("XTB_IPTABLES_PRESENT", tt.present)
			}
			err := tt.run(context.Background())
			if err != nil {
				t.Fatalf("run %s: %v", tt.name, err)
			}
			lines := xtbLoggedLines(t, logPath)
			if len(lines) != 1 {
				t.Fatalf("expected only the -C probe, got %v", lines)
			}
			if !strings.Contains(" "+lines[0]+" ", " -C ") {
				t.Fatalf("expected a -C probe, got %q", lines[0])
			}
			xtbAssertNoFlag(t, lines, tt.forbid)
			xtbAssertWaitFlag(t, lines)
		})
	}
}

func TestXtbProbeFailureTable(t *testing.T) {
	tests := []struct {
		name string
		run  func(ctx context.Context) error
	}{
		{
			name: "append surfaces stderr",
			run: func(ctx context.Context) error {
				return appendUnique(ctx, "nat", "POSTROUTING", "-j", "MASQUERADE")
			},
		},
		{
			name: "delete surfaces stderr",
			run: func(ctx context.Context) error {
				return deleteIfExists(ctx, "nat", "POSTROUTING", "-j", "MASQUERADE")
			},
		},
		{
			name: "setup surfaces stderr",
			run:  SetupMasquerade,
		},
		{
			name: "add surfaces stderr",
			run: func(ctx context.Context) error {
				return AddPortMapping(ctx, 8080, "10.0.0.5/24", 80)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			xtbFakeIptables(t)
			t.Setenv("XTB_IPTABLES_FAIL", "1")
			err := tt.run(context.Background())
			if err == nil {
				t.Fatalf("expected error for %s, got nil", tt.name)
			}
			if !strings.Contains(err.Error(), "xtb probe broke") {
				t.Fatalf("expected stderr text in error, got %v", err)
			}
		})
	}
}

func TestXtbContextCancelTable(t *testing.T) {
	tests := []struct {
		name string
		run  func(ctx context.Context) error
	}{
		{
			name: "setup cancelled",
			run:  SetupMasquerade,
		},
		{
			name: "add cancelled",
			run: func(ctx context.Context) error {
				return AddPortMapping(ctx, 8080, "10.0.0.5/24", 80)
			},
		},
		{
			name: "remove cancelled",
			run: func(ctx context.Context) error {
				return RemovePortMapping(ctx, 8080, "10.0.0.5/24", 80)
			},
		},
		{
			name: "append cancelled",
			run: func(ctx context.Context) error {
				return appendUnique(ctx, "nat", "POSTROUTING", "-j", "MASQUERADE")
			},
		},
		{
			name: "delete cancelled",
			run: func(ctx context.Context) error {
				return deleteIfExists(ctx, "nat", "POSTROUTING", "-j", "MASQUERADE")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logPath := xtbFakeIptables(t)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := tt.run(ctx)
			if err == nil {
				t.Fatalf("expected context error for %s, got nil", tt.name)
			}
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("expected context error for %s, got %v", tt.name, err)
			}
			lines := xtbLoggedLines(t, logPath)
			xtbAssertNoFlag(t, lines, "-A")
		})
	}
}

func TestXtbMalformedCIDRTable(t *testing.T) {
	tests := []struct {
		name string
		cidr string
	}{
		{name: "no slash", cidr: "10.0.0.5"},
		{name: "empty", cidr: ""},
		{name: "plain word", cidr: "notaCIDR"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logPath := xtbFakeIptables(t)
			err := AddPortMapping(context.Background(), 8080, tt.cidr, 80)
			if err == nil {
				t.Fatalf("expected error for CIDR %q, got nil", tt.cidr)
			}
			err = RemovePortMapping(context.Background(), 8080, tt.cidr, 80)
			if err == nil {
				t.Fatalf("expected error for CIDR %q, got nil", tt.cidr)
			}
			lines := xtbLoggedLines(t, logPath)
			if len(lines) != 0 {
				t.Fatalf("expected no exec for bad CIDR %q, got %v", tt.cidr, lines)
			}
		})
	}
}

func TestXtbMultiRulePortMappingTable(t *testing.T) {
	tests := []struct {
		name          string
		hostPort      uint16
		cidr          string
		containerPort uint16
		wantProbe     string
		wantAppend    string
	}{
		{
			name:          "web",
			hostPort:      8080,
			cidr:          "10.0.0.5/24",
			containerPort: 80,
			wantProbe:     "-w -t nat -C PREROUTING -p tcp --dport 8080 -j DNAT --to-destination 10.0.0.5:80",
			wantAppend:    "-w -t nat -A PREROUTING -p tcp --dport 8080 -j DNAT --to-destination 10.0.0.5:80",
		},
		{
			name:          "tls",
			hostPort:      8443,
			cidr:          "10.0.0.9/24",
			containerPort: 443,
			wantProbe:     "-w -t nat -C PREROUTING -p tcp --dport 8443 -j DNAT --to-destination 10.0.0.9:443",
			wantAppend:    "-w -t nat -A PREROUTING -p tcp --dport 8443 -j DNAT --to-destination 10.0.0.9:443",
		},
		{
			name:          "max ports",
			hostPort:      65535,
			cidr:          "10.0.0.1/16",
			containerPort: 65535,
			wantProbe:     "-w -t nat -C PREROUTING -p tcp --dport 65535 -j DNAT --to-destination 10.0.0.1:65535",
			wantAppend:    "-w -t nat -A PREROUTING -p tcp --dport 65535 -j DNAT --to-destination 10.0.0.1:65535",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logPath := xtbFakeIptables(t)
			err := AddPortMapping(context.Background(), tt.hostPort, tt.cidr, tt.containerPort)
			if err != nil {
				t.Fatalf("add port mapping %s: %v", tt.name, err)
			}
			lines := xtbLoggedLines(t, logPath)
			xtbCheckPortMapping(t, lines, tt.wantProbe, tt.wantAppend)
			xtbAssertWaitFlag(t, lines)
			xtbAssertWaitFirst(t, lines)
		})
	}
}

func xtbCheckRuleExists(t *testing.T, present, noChain, badOpt string, wantExist, wantErr bool, wantText, name string) {
	t.Helper()
	xtbFakeIptables(t)
	if present != "" {
		t.Setenv("XTB_IPTABLES_PRESENT", present)
	}
	if noChain != "" {
		t.Setenv("XTB_IPTABLES_NOCHAIN", noChain)
	}
	if badOpt != "" {
		t.Setenv("XTB_IPTABLES_BADOPT", badOpt)
	}
	exists, err := ruleExists(context.Background(), "nat", "POSTROUTING", []string{"-j", "MASQUERADE"})
	if wantErr {
		if err == nil {
			t.Fatalf("expected error for %s, got nil", name)
		}
		if !strings.Contains(err.Error(), wantText) {
			t.Fatalf("expected %q in error, got %v", wantText, err)
		}
		if exists {
			t.Fatalf("expected exists false for %s", name)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error for %s: %v", name, err)
	}
	if exists != wantExist {
		t.Fatalf("expected exists %v for %s, got %v", wantExist, name, exists)
	}
}

func TestXtbRuleExistsStderrTable(t *testing.T) {
	tests := []struct {
		name      string
		present   string
		noChain   string
		badOpt    string
		wantExist bool
		wantErr   bool
		wantText  string
	}{
		{
			name:      "absent rule is not an error",
			present:   "",
			wantExist: false,
			wantErr:   false,
		},
		{
			name:      "present rule exists",
			present:   "1",
			wantExist: true,
			wantErr:   false,
		},
		{
			name:      "missing chain fails closed",
			noChain:   "1",
			wantExist: false,
			wantErr:   true,
			wantText:  "No chain",
		},
		{
			name:      "bad option fails closed",
			badOpt:    "1",
			wantExist: false,
			wantErr:   true,
			wantText:  "unknown option",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			xtbCheckRuleExists(t, tt.present, tt.noChain, tt.badOpt, tt.wantExist, tt.wantErr, tt.wantText, tt.name)
		})
	}
}

func xtbCheckDeleteSurfaces(t *testing.T, run func(ctx context.Context) error, want, forbid, name string) {
	t.Helper()
	logPath := xtbFakeIptables(t)
	if want == "No chain" {
		t.Setenv("XTB_IPTABLES_NOCHAIN", "1")
	} else {
		t.Setenv("XTB_IPTABLES_BADOPT", "1")
	}
	err := run(context.Background())
	if err == nil {
		t.Fatalf("expected error for %s, got nil", name)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("expected %q in error, got %v", want, err)
	}
	lines := xtbLoggedLines(t, logPath)
	if len(lines) != 1 {
		t.Fatalf("expected only the -C probe, got %v", lines)
	}
	xtbAssertNoFlag(t, lines, forbid)
	xtbAssertWaitFirst(t, lines)
}

func TestXtbDeleteMissingChainSurfacesTable(t *testing.T) {
	tests := []struct {
		name   string
		run    func(ctx context.Context) error
		want   string
		forbid string
	}{
		{
			name: "delete surfaces missing chain",
			run: func(ctx context.Context) error {
				return deleteIfExists(ctx, "nat", "POSTROUTING", "-j", "MASQUERADE")
			},
			want:   "No chain",
			forbid: "-D",
		},
		{
			name: "append surfaces missing chain",
			run: func(ctx context.Context) error {
				return appendUnique(ctx, "nat", "POSTROUTING", "-j", "MASQUERADE")
			},
			want:   "No chain",
			forbid: "-A",
		},
		{
			name: "append surfaces bad option",
			run: func(ctx context.Context) error {
				return appendUnique(ctx, "nat", "POSTROUTING", "-j", "MASQUERADE")
			},
			want:   "unknown option",
			forbid: "-A",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			xtbCheckDeleteSurfaces(t, tt.run, tt.want, tt.forbid, tt.name)
		})
	}
}

func TestXtbErrorCarriesFullArgvTable(t *testing.T) {
	tests := []struct {
		name string
		run  func(ctx context.Context) error
	}{
		{
			name: "append error carries argv",
			run: func(ctx context.Context) error {
				return appendUnique(ctx, "nat", "POSTROUTING", "-j", "MASQUERADE")
			},
		},
		{
			name: "delete error carries argv",
			run: func(ctx context.Context) error {
				return deleteIfExists(ctx, "nat", "POSTROUTING", "-j", "MASQUERADE")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			xtbFakeIptables(t)
			t.Setenv("XTB_IPTABLES_FAIL", "1")
			err := tt.run(context.Background())
			if err == nil {
				t.Fatalf("expected error for %s, got nil", tt.name)
			}
			if !strings.Contains(err.Error(), "-w -t nat") {
				t.Fatalf("expected full argv with -w -t nat in error, got %v", err)
			}
			if !strings.Contains(err.Error(), "xtb probe broke") {
				t.Fatalf("expected stderr text in error, got %v", err)
			}
		})
	}
}

func TestXtbTeardownTimeout(t *testing.T) {
	xtbFakeSleepingIptables(t)
	old := iptablesWaitTimeout
	t.Cleanup(func() {
		iptablesWaitTimeout = old
	})
	iptablesWaitTimeout = 100 * time.Millisecond
	start := time.Now()
	err := TeardownMasquerade()
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("expected timeout error, got nil")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("teardown hung, elapsed %v", elapsed)
	}
}
