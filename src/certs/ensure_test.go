// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package certs_test

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/netip"
	"os"
	"slices"
	"testing"

	"github.com/google/uuid"

	"github.com/podomy/concord/src/certs"
)

func TestEnsureFailsWithoutCA(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	_, err := certs.Ensure(uuid.New(), netip.Addr{})
	if err == nil {
		t.Fatal("expected error without CA")
	}
}

func TestEnsureMintsNodeAndReuses(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	provisionCA(t)

	id := uuid.New()
	paths, err := certs.Ensure(id, netip.Addr{})
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	assertRegularFile(t, paths.CA)
	assertRegularFile(t, paths.CAKey)
	assertRegularFile(t, paths.Cert)
	assertRegularFile(t, paths.Key)

	again, err := certs.Ensure(id, netip.Addr{})
	if err != nil {
		t.Fatalf("ensure reuse: %v", err)
	}
	if again.CA != paths.CA {
		t.Fatalf("ca path = %q, want %q", again.CA, paths.CA)
	}
}

func TestEnsureRemintsInvalidNodeKeepsCA(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	provisionCA(t)

	id := uuid.New()
	paths, err := certs.Ensure(id, netip.Addr{})
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	caBefore, err := os.ReadFile(paths.CA)
	if err != nil {
		t.Fatalf("read ca: %v", err)
	}
	if err := os.WriteFile(paths.Cert, []byte("not-a-cert"), 0o600); err != nil {
		t.Fatalf("corrupt cert: %v", err)
	}
	if _, err := certs.Ensure(id, netip.Addr{}); err != nil {
		t.Fatalf("ensure after corrupt: %v", err)
	}
	caAfter, err := os.ReadFile(paths.CA)
	if err != nil {
		t.Fatalf("read ca after: %v", err)
	}
	if string(caBefore) != string(caAfter) {
		t.Fatal("CA cert changed after node remint")
	}
}

func TestEnsureSharedCATwoNodeIDs(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	provisionCA(t)

	pathsA, err := certs.Ensure(uuid.New(), netip.Addr{})
	if err != nil {
		t.Fatalf("ensure A: %v", err)
	}
	leafA := loadLeaf(t, pathsA)

	// Another node id with the same CA files (drop only node material).
	if err := os.Remove(pathsA.Cert); err != nil {
		t.Fatalf("remove cert: %v", err)
	}
	if err := os.Remove(pathsA.Key); err != nil {
		t.Fatalf("remove key: %v", err)
	}
	pathsB, err := certs.Ensure(uuid.New(), netip.Addr{})
	if err != nil {
		t.Fatalf("ensure B: %v", err)
	}
	leafB := loadLeaf(t, pathsB)

	caPool := x509.NewCertPool()
	caPEM, err := os.ReadFile(pathsB.CA)
	if err != nil {
		t.Fatalf("read ca: %v", err)
	}
	if !caPool.AppendCertsFromPEM(caPEM) {
		t.Fatal("ca pool")
	}
	if _, err := leafA.Verify(x509.VerifyOptions{Roots: caPool}); err != nil {
		t.Fatalf("A verify: %v", err)
	}
	if _, err := leafB.Verify(x509.VerifyOptions{Roots: caPool}); err != nil {
		t.Fatalf("B verify: %v", err)
	}
}

func TestEnsureAdvertiseIPInSANs(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	provisionCA(t)

	id := uuid.New()
	adv := netip.MustParseAddr("10.0.0.5")
	paths, err := certs.Ensure(id, adv)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}

	leaf := loadLeaf(t, paths)
	if !hasDNSName(leaf, id.String()) {
		t.Fatalf("DNSNames %v missing node id", leaf.DNSNames)
	}
	if !hasIP(leaf, net.IPv4(10, 0, 0, 5)) {
		t.Fatalf("IPAddresses %v missing advertise IP", leaf.IPAddresses)
	}
}

func provisionCA(t *testing.T) {
	t.Helper()
	if err := certs.WriteCA(); err != nil {
		t.Fatalf("write ca: %v", err)
	}
}

func loadLeaf(t *testing.T, paths certs.Paths) *x509.Certificate {
	t.Helper()
	return loadLeafFrom(t, paths.Cert, paths.Key)
}

func loadLeafFrom(t *testing.T, certFile, keyFile string) *x509.Certificate {
	t.Helper()
	pair, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("load pair: %v", err)
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	return leaf
}

func hasDNSName(cert *x509.Certificate, name string) bool {
	return slices.Contains(cert.DNSNames, name)
}

func hasIP(cert *x509.Certificate, want net.IP) bool {
	for _, ip := range cert.IPAddresses {
		if ip.Equal(want) {
			return true
		}
	}
	return false
}

func assertRegularFile(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if !info.Mode().IsRegular() {
		t.Fatalf("%s is not a regular file", path)
	}
}
