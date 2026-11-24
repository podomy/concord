// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: BUSL-1.1

package registry

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	sdkfileart "github.com/podomy/concord-sdk/fileart"

	"github.com/podomy/concord/internal/logs"
	"github.com/podomy/concord/internal/peerdiscovery"
)

// Push and pull against the real embedded registry through the external
// SDK fileart client: this proves the exact request shapes (monolithic
// upload with digest query, manifest with a custom layer type under a
// tag, HEAD semantics) are accepted by the registry Concord runs.
func TestFileartPushPullAgainstRealRegistry(t *testing.T) {
	setupFileartCompatKeys(t)
	ctx, client := startFileartCompatRegistry(t)
	digest := pushFileartUntilReady(t, ctx, client)
	pullFileartAndVerify(t, ctx, client, digest)
}

// setupFileartCompatKeys points XDG at a temp dir and writes a member key.
func setupFileartCompatKeys(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)
	keyDir := filepath.Join(dir, "concord", "memberservice")
	err := os.MkdirAll(keyDir, 0o700)
	if err != nil {
		t.Fatal(err)
	}
	err = os.WriteFile(filepath.Join(keyDir, "secret.key"), fileartCompatBytesRepeat(32), 0o600)
	if err != nil {
		t.Fatal(err)
	}
}

// startFileartCompatRegistry starts peer discovery and the registry on 18444.
func startFileartCompatRegistry(t *testing.T) (context.Context, *sdkfileart.Client) {
	t.Helper()

	peerService, err := peerdiscovery.Start(logs.NewNop(), peerdiscovery.Node{
		ID:      uuid.New(),
		Address: netip.MustParseAddrPort("127.0.0.1:0"),
	}, nil, netip.Addr{}, peerdiscovery.NoiseIdentity{})
	if err != nil {
		t.Fatalf("start peer service: %v", err)
	}
	t.Cleanup(func() {
		err := peerService.Shutdown()
		if err != nil {
			t.Errorf("shutdown peer service: %v", err)
		}
	})

	oldPort := Port
	Port = 18444
	t.Cleanup(func() { Port = oldPort })

	reg, err := New(uuid.New())
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	ctx := t.Context()
	err = reg.Start(ctx, peerService, logs.NewNop())
	if err != nil {
		t.Fatalf("start registry: %v", err)
	}
	t.Cleanup(reg.Stop)

	return ctx, sdkfileart.NewClient("127.0.0.1:18444")
}

// pushFileartUntilReady retries the first push until the registry accepts it.
func pushFileartUntilReady(t *testing.T, ctx context.Context, client *sdkfileart.Client) string {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for {
		digest, err := client.Push(ctx, []byte("field calibration v3"))
		if err == nil {
			return digest
		}
		if time.Now().After(deadline) {
			t.Fatalf("push against real registry: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// pullFileartAndVerify pulls the digest back and checks bytes plus manifest tag.
func pullFileartAndVerify(t *testing.T, ctx context.Context, client *sdkfileart.Client, digest string) {
	t.Helper()

	dest := filepath.Join(t.TempDir(), "out")
	err := client.PullTo(ctx, digest, dest)
	if err != nil {
		t.Fatalf("pull against real registry: %v", err)
	}
	got, err := os.ReadFile(dest) //nolint:gosec // test reads a file it just pulled
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "field calibration v3" {
		t.Fatalf("pull = %q", got)
	}
	err = checkFileartManifestStored(digest)
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
}

// checkFileartManifestStored fetches the manifest back by tag: the tag is what
// registry sync replicates, so an accepted-but-untagged manifest would
// still strand the blob on one node.
func checkFileartManifestStored(digest string) error {
	tag := strings.TrimPrefix(digest, "sha256:")
	resp, err := http.Get("http://127.0.0.1:18444/v2/concord-files/manifests/" + tag) //nolint:noctx // test-only loopback fetch
	if err != nil {
		return fmt.Errorf("get manifest: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body) //nolint:errcheck // best-effort body drain
		_ = resp.Body.Close()                 //nolint:errcheck // best-effort response close
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("manifest status = %d", resp.StatusCode)
	}
	return nil
}

func fileartCompatBytesRepeat(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = 0x07
	}
	return out
}
