// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package integration_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/podomy/concord/src/journal"
	"github.com/podomy/concord/src/journalreader"
	"github.com/podomy/concord/src/journalview"
	"github.com/podomy/concord/src/kvstore"
	"github.com/podomy/concord/src/peerdiscovery"
	"github.com/podomy/concord/src/peersync"
	"github.com/podomy/concord/src/transport"
)

func TestTwoNodeSmoke(t *testing.T) {
	dirA, dirB := t.TempDir(), t.TempDir()
	caDER, caKey := provisionCerts(t, dirA, dirB)

	idA := uuid.New()
	idB := uuid.New()

	mintNode(t, idA, caDER, caKey, dirA, netip.Addr{})
	mintNode(t, idB, caDER, caKey, dirB, netip.Addr{})

	kvA := openKV(t, filepath.Join(dirA, "concord", "bbolt.db"))
	jA := openJSONL(t, filepath.Join(dirA, "concord", "journal.jsonl"))
	_, viewsA := initViews(t, kvA, filepath.Join(dirA, "concord", "journal.jsonl"))

	kvB := openKV(t, filepath.Join(dirB, "concord", "bbolt.db"))
	jB := openJSONL(t, filepath.Join(dirB, "concord", "journal.jsonl"))
	eventsByIDB, viewsB := initViews(t, kvB, filepath.Join(dirB, "concord", "journal.jsonl"))

	logger := zaptest.NewLogger(t)

	// Point auto-path functions (journalreader.OpenJSONLReader used by
	// transport.postSync) at A's temp dir so the HTTP handler reads the
	// correct journal.
	t.Setenv("XDG_CONFIG_HOME", dirA)

	// Node A
	ctxA, cancelA := context.WithCancel(t.Context())
	t.Cleanup(cancelA)

	peerA := startMemberlist(t, logger, idA, netip.MustParseAddrPort("127.0.0.1:17946"), nil, netip.Addr{})
	t.Cleanup(func() { shutDown(t, peerA) })

	origPort := transport.Port
	transport.Port = "18443"
	t.Cleanup(func() { transport.Port = origPort })

	caPathA := filepath.Join(dirA, "concord", "certs", "ca.crt")
	certPathA := filepath.Join(dirA, "concord", "certs", "node.crt")
	keyPathA := filepath.Join(dirA, "concord", "certs", "node.key")
	if err := transport.Start(ctxA, logger, caPathA, certPathA, keyPathA); err != nil {
		t.Fatalf("A transport: %v", err)
	}

	clientA, err := transport.NewClient(caPathA, certPathA, keyPathA)
	if err != nil {
		t.Fatalf("A client: %v", err)
	}
	_ = clientA // unused in this test (only B pulls)

	if err = journalview.RecordNodeStarted(ctxA, logger, jA, viewsA, idA, netip.MustParseAddrPort("127.0.0.1:17946")); err != nil {
		t.Fatalf("A record started: %v", err)
	}

	// Node B
	ctxB, cancelB := context.WithCancel(t.Context())
	t.Cleanup(cancelB)

	peerB := startMemberlist(t, logger, idB, netip.MustParseAddrPort("127.0.0.1:17947"),
		[]netip.AddrPort{netip.MustParseAddrPort("127.0.0.1:17946")}, netip.Addr{})
	t.Cleanup(func() { shutDown(t, peerB) })

	caPathB := filepath.Join(dirB, "concord", "certs", "ca.crt")
	certPathB := filepath.Join(dirB, "concord", "certs", "node.crt")
	keyPathB := filepath.Join(dirB, "concord", "certs", "node.key")
	clientB, err := transport.NewClient(caPathB, certPathB, keyPathB)
	if err != nil {
		t.Fatalf("B client: %v", err)
	}

	transport.Port = "18443"
	go peersync.RunPullLoop(ctxB, logger, idB, peerB, clientB, jB, viewsB, eventsByIDB)

	// Let memberlist gossip propagate so B sees A as alive.
	time.Sleep(2 * time.Second)

	// Record a test event on A
	testEvent := journal.NewEvent(idA, "smoke.test", json.RawMessage(`{}`))
	if err := journalview.RecordEvent(ctxA, jA, viewsA, testEvent); err != nil {
		t.Fatalf("record event: %v", err)
	}

	// Wait for B to pull and apply A's event
	got := waitForEvent(t, eventsByIDB, testEvent.ID, 20*time.Second)

	if got.NodeID != idA {
		t.Fatalf("event node_id = %s, want %s", got.NodeID, idA)
	}
	if got.Type != "smoke.test" {
		t.Fatalf("event type = %s, want smoke.test", got.Type)
	}

	t.Log("smoke test PASSED: event synced from A to B via pull loop")
}

// provisionCerts creates subdirs and a shared CA cert+key in both dirs.
func provisionCerts(t *testing.T, dirs ...string) (caDER []byte, caKey *rsa.PrivateKey) {
	t.Helper()

	for _, dir := range dirs {
		if err := os.MkdirAll(filepath.Join(dir, "concord", "certs"), 0o700); err != nil {
			t.Fatal(err)
		}
	}

	caDER, caKey = generateCA(t)
	for _, dir := range dirs {
		writePEM(t, filepath.Join(dir, "concord", "certs", "ca.crt"), "CERTIFICATE", caDER)
		writePEM(t, filepath.Join(dir, "concord", "certs", "ca.key"), "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(caKey))
	}

	return caDER, caKey
}

// --- helpers ---

func generateCA(t *testing.T) (der []byte, key *rsa.PrivateKey) {
	t.Helper()

	var err error
	key, err = rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Concord CA"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(10 * 365 * 24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}

	der, err = x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}

	return der, key
}

func mintNode(t *testing.T, id uuid.UUID, caDER []byte, caKey *rsa.PrivateKey, dir string, advertise netip.Addr) {
	t.Helper()

	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}

	nodeKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate node key: %v", err)
	}

	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatalf("generate serial: %v", err)
	}

	ips := []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback}
	if advertise.IsValid() {
		ips = append(ips, advertise.AsSlice())
	}

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "Concord Node: " + id.String()},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{id.String(), "localhost"},
		IPAddresses:  ips,
	}

	nodeDER, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &nodeKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create node cert: %v", err)
	}

	writePEM(t, filepath.Join(dir, "concord", "certs", "node.crt"), "CERTIFICATE", nodeDER)
	writePEM(t, filepath.Join(dir, "concord", "certs", "node.key"), "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(nodeKey))
}

func writePEM(t *testing.T, path, blockType string, der []byte) {
	t.Helper()

	// #nosec G304 — test only, paths are under t.TempDir.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	defer func() { _ = f.Close() }() //nolint:errcheck // best-effort

	if err := pem.Encode(f, &pem.Block{Type: blockType, Bytes: der}); err != nil {
		t.Fatalf("encode %s: %v", path, err)
	}
}

func openKV(t *testing.T, path string) *kvstore.KVStore {
	t.Helper()

	kv, err := kvstore.OpenDBPath(path)
	if err != nil {
		t.Fatalf("open kv %s: %v", path, err)
	}
	return kv
}

func openJSONL(t *testing.T, path string) *journal.JSONL {
	t.Helper()

	j, err := journal.OpenJSONLPath(path)
	if err != nil {
		t.Fatalf("open journal %s: %v", path, err)
	}
	return j
}

func initViews(t *testing.T, kv *kvstore.KVStore, journalPath string) (*journalview.EventsByID, []journalview.View) {
	t.Helper()

	eventsByID := journalview.NewEventsByID(kv)
	eventsByNode := journalview.NewEventsByNode(kv)
	eventsByType := journalview.NewEventsByType(kv)
	views := []journalview.View{eventsByID, eventsByNode, eventsByType}

	ctx := context.Background()

	jr, err := journalreader.OpenJSONLReaderPath(journalPath)
	if err != nil {
		t.Fatalf("open journal reader: %v", err)
	}
	defer func() {
		if err := jr.Close(); err != nil {
			t.Errorf("close journal reader: %v", err)
		}
	}()

	for _, view := range views {
		if err := view.Rebuild(ctx, jr); err != nil {
			t.Fatalf("rebuild view: %v", err)
		}
	}

	return eventsByID, views
}

func startMemberlist(t *testing.T, logger *zap.Logger, id uuid.UUID, bind netip.AddrPort, join []netip.AddrPort, advertise netip.Addr) *peerdiscovery.MemberService {
	t.Helper()

	node := peerdiscovery.Node{
		ID:      id,
		Address: bind,
	}

	ms, err := peerdiscovery.Start(logger, node, join, advertise)
	if err != nil {
		t.Fatalf("memberlist start: %v", err)
	}
	return ms
}

func shutDown(t *testing.T, ms *peerdiscovery.MemberService) {
	t.Helper()

	if err := ms.Shutdown(); err != nil {
		t.Logf("memberlist shutdown: %v", err)
	}
}

func waitForEvent(t *testing.T, byID *journalview.EventsByID, id uuid.UUID, timeout time.Duration) *journal.Event {
	t.Helper()

	pollCtx, pollCancel := context.WithTimeout(t.Context(), timeout)
	defer pollCancel()

	for {
		if pollCtx.Err() != nil {
			t.Fatalf("timed out waiting for event %s", id)
		}
		e, err := byID.Get(pollCtx, id)
		if err == nil && e != nil {
			return e
		}
		time.Sleep(200 * time.Millisecond)
	}
}
