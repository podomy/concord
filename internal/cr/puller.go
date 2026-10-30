// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cr

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/podomy/concord/internal/registry"
)

// Puller a definition of an interface that is required to be a puller.
type Puller interface {
	Pull(ctx context.Context, image, bundleDir string) (*PullResult, error)
}

// ImagePuller pulls OCI container images from the local registry
// at localhost:8444 and extracts them into rootfs directories.
type ImagePuller struct {
	registry string
}

// NewImagePuller creates an ImagePuller targeting the local registry.
func NewImagePuller() *ImagePuller {
	return &ImagePuller{registry: "localhost:" + strconv.Itoa(registry.Port)}
}

// PullResult contains the extracted rootfs path and the image configuration
// for use by the bundle builder.
type PullResult struct {
	Config *ConfigFile
	RootFS string
}

// Pull fetches the image, resolves the platform, and extracts layers
// into the rootfs directory under bundleDir. A stale rootfs from a
// previous attempt is removed first so a failed start retries cleanly
// instead of wedging every later tick on an already-exists error.
// A failed extraction also removes the partial rootfs for the same
// reason.
func (p *ImagePuller) Pull(ctx context.Context, ref, bundleDir string) (*PullResult, error) {
	rootfs := filepath.Join(bundleDir, "rootfs")

	parsed, err := parseImageReference(ref)
	if err != nil {
		return nil, fmt.Errorf("parse image reference: %w", err)
	}

	client := &http.Client{}

	configDigest, layerDigests, err := fetchImageManifest(ctx, client, parsed)
	if err != nil {
		return nil, fmt.Errorf("fetch manifest: %w", err)
	}

	cfg, err := fetchImageConfig(ctx, client, parsed, configDigest)
	if err != nil {
		return nil, fmt.Errorf("image config file: %w", err)
	}

	err = os.RemoveAll(rootfs)
	if err != nil {
		return nil, fmt.Errorf("clear stale rootfs: %w", err)
	}

	err = os.MkdirAll(rootfs, 0o700)
	if err != nil {
		return nil, fmt.Errorf("create rootfs: %w", err)
	}

	err = extractLayers(ctx, client, parsed, layerDigests, rootfs)
	if err != nil {
		_ = os.RemoveAll(rootfs) //nolint:errcheck // best-effort cleanup so the next tick retries
		return nil, err
	}

	return &PullResult{RootFS: rootfs, Config: cfg}, nil
}

// extractTar extracts a tar stream into root, handling OCI whiteouts.
func extractTar(ctx context.Context, r io.Reader, root string) error {
	tr := tar.NewReader(r)
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("context cancelled: %w", err)
		}

		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("tar reader: %w", err)
		}

		if err := extractEntry(ctx, tr, hdr, root); err != nil {
			return err
		}
	}
}

// extractLayers downloads each layer blob in order and extracts it into rootfs.
func extractLayers(ctx context.Context, client *http.Client, ref imageReference, digests []string, rootfs string) error {
	for _, digest := range digests {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("context cancelled: %w", err)
		}

		err := extractLayerBlob(ctx, client, ref, digest, rootfs)
		if err != nil {
			return fmt.Errorf("extract layer: %w", err)
		}
	}
	return nil
}

// extractEntry handles a single tar entry, dispatching to the appropriate
// handler based on type and checking for OCI whiteouts first.
func extractEntry(ctx context.Context, tr *tar.Reader, hdr *tar.Header, root string) error {
	name := filepath.Clean(hdr.Name)
	path := filepath.Join(root, name)
	if !strings.HasPrefix(path, filepath.Clean(root)+string(os.PathSeparator)) {
		return fmt.Errorf("tar entry %q escapes root", hdr.Name)
	}

	base := filepath.Base(name)

	if base == ".wh..wh..opq" {
		return clearDir(filepath.Dir(path))
	}

	whName, isWhiteout := strings.CutPrefix(base, ".wh.")
	if isWhiteout {
		whPath := filepath.Join(filepath.Dir(path), whName)
		if err := os.RemoveAll(whPath); err != nil {
			return fmt.Errorf("remove whiteout %q: %w", whPath, err)
		}
		return nil
	}

	switch hdr.Typeflag {
	case tar.TypeDir:
		return extractDir(path, hdr)
	case tar.TypeReg:
		return extractFile(ctx, tr, path, hdr)
	case tar.TypeSymlink:
		return extractSymlink(path, hdr, root)
	case tar.TypeLink:
		return extractHardLink(path, hdr, root)
	default:
		return nil
	}
}

// extractDir creates a directory at the given path.
func extractDir(path string, hdr *tar.Header) error {
	if err := os.MkdirAll(path, os.FileMode(hdr.Mode)); err != nil { //nolint:gosec // mode comes from image layer
		return fmt.Errorf("mkdir %q: %w", path, err)
	}
	return nil
}

// extractFile creates a regular file at path and copies content from the tar reader.
func extractFile(ctx context.Context, tr *tar.Reader, path string, hdr *tar.Header) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("mkdir parent %q: %w", path, err)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode)) //nolint:gosec // path validated above
	if err != nil {
		return fmt.Errorf("create file %q: %w", path, err)
	}

	if _, err := io.Copy(f, tr); err != nil {
		_ = f.Close() //nolint:errcheck // best-effort close on write error

		return fmt.Errorf("write file %q: %w", path, err)
	}

	ctxErr := ctx.Err()
	if ctxErr != nil {
		_ = f.Close() //nolint:errcheck // best-effort close on cancel
		return fmt.Errorf("context cancelled: %w", ctxErr)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("close file %q: %w", path, err)
	}

	return nil
}

// extractSymlink creates a symbolic link. Absolute targets and targets
// resolving outside root are rejected to keep links inside the rootfs.
func extractSymlink(path string, hdr *tar.Header, root string) error {
	if hdr.Linkname == "" {
		return fmt.Errorf("symlink %q has empty target", hdr.Name)
	}
	if filepath.IsAbs(hdr.Linkname) {
		return fmt.Errorf("symlink target %q escapes root", hdr.Linkname)
	}
	target := filepath.Join(filepath.Dir(path), hdr.Linkname) //nolint:gosec // target is validated below to stay inside root
	cleanRoot := filepath.Clean(root)
	if target != cleanRoot && !strings.HasPrefix(target, cleanRoot+string(os.PathSeparator)) {
		return fmt.Errorf("symlink target %q escapes root", hdr.Linkname)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("mkdir parent for symlink %q: %w", path, err)
	}

	if err := os.Symlink(hdr.Linkname, path); err != nil {
		return fmt.Errorf("symlink %q -> %q: %w", path, hdr.Linkname, err)
	}
	return nil
}

// extractHardLink creates a hard link. It may fail if the target has not
// been extracted yet (cross-layer links), so the error is best-effort.
func extractHardLink(path string, hdr *tar.Header, root string) error {
	linkPath := filepath.Join(root, filepath.Clean(hdr.Linkname))
	if !strings.HasPrefix(linkPath, filepath.Clean(root)+string(os.PathSeparator)) {
		return fmt.Errorf("hardlink target %q escapes root", hdr.Linkname)
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("mkdir parent for hardlink %q: %w", path, err)
	}

	//nolint:errcheck // may fail if target not yet extracted (cross-layer)
	_ = os.Link(linkPath, path)

	return nil
}

// clearDir removes all children of dir, keeping the directory itself.
func clearDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return fmt.Errorf("read dir %q: %w", dir, err)
	}

	for _, entry := range entries {
		if err := os.RemoveAll(filepath.Join(dir, entry.Name())); err != nil {
			return fmt.Errorf("remove %q: %w", entry.Name(), err)
		}
	}

	return nil
}
