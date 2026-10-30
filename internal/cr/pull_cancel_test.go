// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cr

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/podomy/concord/sdk/fileart"
)

// writeChunkedBody streams data in 32KB flushes with a delay between
// chunks so a mid-stream cancel lands inside io.Copy.
func writeChunkedBody(w http.ResponseWriter, data []byte, delay time.Duration) {
	fl, ok := w.(http.Flusher)
	if !ok {
		_, _ = w.Write(data) //nolint:errcheck // test stub
		return
	}
	for off := 0; off < len(data); off += 32 * 1024 {
		end := min(off+32*1024, len(data))
		_, _ = w.Write(data[off:end]) //nolint:errcheck // test stub
		fl.Flush()
		time.Sleep(delay)
	}
}

// slowLayerServer serves config fast and the layer slowly in small
// flushes so a mid stream cancel lands inside io.Copy.
func slowLayerServer(t *testing.T, repo string, manifestRaw []byte, blobs map[string][]byte, slowDigest string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/"+repo+"/manifests/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(manifestRaw) //nolint:errcheck // test stub
	})
	mux.HandleFunc("/v2/"+repo+"/blobs/", func(w http.ResponseWriter, r *http.Request) {
		digest := strings.TrimPrefix(r.URL.Path, "/v2/"+repo+"/blobs/")
		data, ok := blobs[digest]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		if digest != slowDigest {
			_, _ = w.Write(data) //nolint:errcheck // test stub
			return
		}
		writeChunkedBody(w, data, 20*time.Millisecond)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// Cancel mid stream aborts Pull, removes the partial rootfs, and
// leaves no temp part files behind.
func TestPullCancelMidStreamCleansRootfs(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	data := make([]byte, 4<<20)
	x := uint32(0x12345678)
	for i := range data {
		x = x*1664525 + 1013904223
		data[i] = byte(x >> 24)
	}
	layer := gzipBytes(t, makeTarFile(t, "big.bin", data))
	layerDigest := testDigest(layer)
	cfgRaw := mustMarshal(t, ConfigFile{Architecture: "amd64", OS: "linux"})
	configDigest := testDigest(cfgRaw)
	repo := "testowner/cancelpull"
	manifest := testManifest{
		SchemaVersion: 2,
		MediaType:     "application/vnd.oci.image.manifest.v1+json",
		Config:        testDescriptor{MediaType: "application/vnd.oci.image.config.v1+json", Digest: configDigest, Size: int64(len(cfgRaw))},
		Layers:        []testDescriptor{{MediaType: "application/vnd.oci.image.layer.v1.tar+gzip", Digest: layerDigest, Size: int64(len(layer))}},
	}
	manifestRaw := mustMarshal(t, manifest)
	blobs := map[string][]byte{configDigest: cfgRaw, layerDigest: layer}
	srv := slowLayerServer(t, repo, manifestRaw, blobs, layerDigest)
	host := strings.TrimPrefix(srv.URL, "http://")
	bundleDir := t.TempDir()
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	puller := &ImagePuller{}
	_, err := puller.Pull(ctx, host+"/"+repo+":latest", bundleDir)
	if err == nil {
		t.Fatalf("Pull succeeded after cancel")
	}
	rootfs := filepath.Join(bundleDir, "rootfs")
	if _, serr := os.Stat(rootfs); !os.IsNotExist(serr) {
		t.Fatalf("partial rootfs left behind")
	}
	entries, err := os.ReadDir(bundleDir)
	if err != nil {
		t.Fatalf("read bundle dir: %v", err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".part") {
			t.Fatalf("temp part left behind: %s", e.Name())
		}
	}
}

// Cancel during PullTo removes the temp part file and the dest.
func TestPullToCancelRemovesPart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	data := make([]byte, 2<<20)
	for i := range data {
		data[i] = byte((i * 7) % 251)
	}
	sum := testDigest(data)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		writeChunkedBody(w, data, 10*time.Millisecond)
	}))
	t.Cleanup(srv.Close)
	host := strings.TrimPrefix(srv.URL, "http://")
	client := fileart.NewClient(host)
	destDir := t.TempDir()
	dest := filepath.Join(destDir, "out")
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	err := client.PullTo(ctx, sum, dest)
	if err == nil {
		t.Fatalf("PullTo succeeded after cancel")
	}
	if _, serr := os.Stat(dest); !os.IsNotExist(serr) {
		t.Fatalf("dest left behind after cancel")
	}
	entries, err := os.ReadDir(destDir)
	if err != nil {
		t.Fatalf("read dest dir: %v", err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".part") {
			t.Fatalf("temp part left behind: %s", e.Name())
		}
		if strings.HasPrefix(e.Name(), ".pull-") {
			t.Fatalf("temp pull file left behind: %s", e.Name())
		}
	}
}
