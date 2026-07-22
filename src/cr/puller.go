// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cr

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	"github.com/podomy/concord/src/or"
)

// ImagePuller pulls OCI container images from the local zot registry
// at localhost:8444 and extracts them into rootfs directories.
type ImagePuller struct {
	registry string
}

// NewImagePuller creates an ImagePuller targeting the local zot instance.
func NewImagePuller() *ImagePuller {
	return &ImagePuller{registry: "localhost:" + strconv.Itoa(or.Port)}
}

// PullResult contains the extracted rootfs path and the image configuration
// for use by the bundle builder.
type PullResult struct {
	RootFS string
	Config *v1.ConfigFile
}

// Pull fetches the image, resolves the platform, and extracts layers
// into the rootfs directory under bundleDir.
func (p *ImagePuller) Pull(ctx context.Context, ref, bundleDir string) (*PullResult, error) {
	rootfs := filepath.Join(bundleDir, "rootfs")

	if _, err := os.Stat(rootfs); err == nil {
		return nil, fmt.Errorf("rootfs already exists at %s", rootfs)
	}

	tag, err := name.ParseReference(ref)
	if err != nil {
		return nil, fmt.Errorf("parse image reference: %w", err)
	}

	desc, err := remote.Get(tag, remote.WithAuth(authn.Anonymous))
	if err != nil {
		return nil, fmt.Errorf("remote get: %w", err)
	}

	img, err := desc.Image()
	if err != nil {
		return nil, fmt.Errorf("image descriptor: %w", err)
	}

	cfg, err := img.ConfigFile()
	if err != nil {
		return nil, fmt.Errorf("image config file: %w", err)
	}

	if err = os.MkdirAll(rootfs, 0o700); err != nil {
		return nil, fmt.Errorf("create rootfs: %w", err)
	}

	layers, err := img.Layers()
	if err != nil {
		return nil, fmt.Errorf("get image layers: %w", err)
	}

	if err := extractLayers(ctx, layers, rootfs); err != nil {
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

		if err := extractEntry(tr, hdr, root); err != nil {
			return err
		}
	}
}

// extractLayers extracts all image layers into rootfs.
func extractLayers(ctx context.Context, layers []v1.Layer, rootfs string) error {
	for _, layer := range layers {
		rc, err := layer.Uncompressed()
		if err != nil {
			return fmt.Errorf("layer uncompressed: %w", err)
		}

		extractErr := extractTar(ctx, rc, rootfs)
		if closeErr := rc.Close(); closeErr != nil && extractErr == nil {
			return fmt.Errorf("close layer reader: %w", closeErr)
		}
		if extractErr != nil {
			return fmt.Errorf("extract layer: %w", extractErr)
		}
	}
	return nil
}

// extractEntry handles a single tar entry, dispatching to the appropriate
// handler based on type and checking for OCI whiteouts first.
func extractEntry(tr *tar.Reader, hdr *tar.Header, root string) error {
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
		return extractFile(tr, path, hdr)
	case tar.TypeSymlink:
		return extractSymlink(path, hdr)
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
func extractFile(tr *tar.Reader, path string, hdr *tar.Header) error {
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

	if err := f.Close(); err != nil {
		return fmt.Errorf("close file %q: %w", path, err)
	}

	return nil
}

// extractSymlink creates a symbolic link.
func extractSymlink(path string, hdr *tar.Header) error {
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
