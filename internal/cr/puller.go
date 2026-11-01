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
		err := ctx.Err()
		if err != nil {
			return fmt.Errorf("context cancelled: %w", err)
		}

		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("tar reader: %w", err)
		}

		err = extractEntry(ctx, tr, hdr, root)
		if err != nil {
			return err
		}
	}
}

// extractLayers downloads each layer blob in order and extracts it into rootfs.
func extractLayers(ctx context.Context, client *http.Client, ref imageReference, digests []string, rootfs string) error {
	for _, digest := range digests {
		err := ctx.Err()
		if err != nil {
			return fmt.Errorf("context cancelled: %w", err)
		}

		err = extractLayerBlob(ctx, client, ref, digest, rootfs)
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
		return clearDir(filepath.Dir(path), root)
	}

	handled, err := tryWhiteout(hdr, path, root)
	if err != nil {
		return err
	}
	if handled {
		return nil
	}

	switch hdr.Typeflag {
	case tar.TypeDir:
		return extractDir(path, hdr, root)
	case tar.TypeReg:
		return extractFile(ctx, tr, path, hdr, root)
	case tar.TypeSymlink:
		return extractSymlink(path, hdr, root)
	case tar.TypeLink:
		return extractHardLink(path, hdr, root)
	default:
		return nil
	}
}

// tryWhiteout removes the whiteout target when hdr names one.
// It reports whether hdr was a whiteout entry.
func tryWhiteout(hdr *tar.Header, path, root string) (bool, error) {
	base := filepath.Base(filepath.Clean(hdr.Name))
	whName, isWhiteout := strings.CutPrefix(base, ".wh.")
	if !isWhiteout {
		return false, nil
	}
	if whName == "" {
		return true, nil
	}
	if whName == "." || whName == ".." {
		return true, fmt.Errorf("tar entry %q has invalid whiteout", hdr.Name)
	}
	err := rejectSymlinkPrefix(filepath.Dir(path), root)
	if err != nil {
		return true, err
	}
	whPath := filepath.Join(filepath.Dir(path), whName)
	cleanRoot := filepath.Clean(root)
	if whPath != cleanRoot && !strings.HasPrefix(whPath, cleanRoot+string(os.PathSeparator)) {
		return true, fmt.Errorf("tar entry %q escapes root", hdr.Name)
	}
	err = os.RemoveAll(whPath)
	if err != nil {
		return true, fmt.Errorf("remove whiteout %q: %w", whPath, err)
	}
	return true, nil
}

// safeMode strips setuid, setgid and sticky bits from a tar mode, keeping rwx bits.
func safeMode(m int64) os.FileMode {
	return os.FileMode(m & 0o777)
}

// rejectSymlinkPrefix fails when any existing component of path under root is a symlink.
// Missing prefixes are safe because deeper entries cannot exist without them.
func rejectSymlinkPrefix(path, root string) error {
	cleanRoot := filepath.Clean(root)
	cleanPath := filepath.Clean(path)
	if cleanPath != cleanRoot && !strings.HasPrefix(cleanPath, cleanRoot+string(os.PathSeparator)) {
		return fmt.Errorf("tar entry %q escapes root", cleanPath)
	}
	rel, err := filepath.Rel(cleanRoot, cleanPath)
	if err != nil {
		return fmt.Errorf("resolve path %q: %w", cleanPath, err)
	}
	if rel == "." {
		return nil
	}
	cur := cleanRoot
	for part := range strings.SplitSeq(rel, string(os.PathSeparator)) {
		if part == "." || part == "" {
			continue
		}
		cur = filepath.Join(cur, part)
		fi, err := os.Lstat(cur)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return fmt.Errorf("lstat %q: %w", cur, err)
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("tar entry %q traverses symlink at %q", cleanPath, cur)
		}
	}
	return nil
}

// extractDir creates a directory without following symlinks and drops special mode bits.
// Setuid, setgid and sticky bits are removed, keeping rwx bits.
func extractDir(path string, hdr *tar.Header, root string) error {
	err := rejectSymlinkPrefix(path, root)
	if err != nil {
		return err
	}
	err = os.MkdirAll(path, safeMode(hdr.Mode))
	if err != nil {
		return fmt.Errorf("mkdir %q: %w", path, err)
	}
	return nil
}

// extractFile creates a regular file at path without traversing symlinks.
// Parent components must not contain symlinks, an existing symlink at path
// is rejected, and setuid, setgid and sticky bits are dropped from the mode.
func extractFile(ctx context.Context, tr *tar.Reader, path string, hdr *tar.Header, root string) error {
	err := rejectSymlinkPrefix(filepath.Dir(path), root)
	if err != nil {
		return err
	}
	err = os.MkdirAll(filepath.Dir(path), 0o700)
	if err != nil {
		return fmt.Errorf("mkdir parent %q: %w", path, err)
	}
	fi, err := os.Lstat(path)
	if err == nil {
		if fi.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("tar entry %q traverses symlink at %q", path, path)
		}
	} else {
		if !os.IsNotExist(err) {
			return fmt.Errorf("lstat %q: %w", path, err)
		}
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, safeMode(hdr.Mode)) //nolint:gosec // path validated above
	if err != nil {
		return fmt.Errorf("create file %q: %w", path, err)
	}

	_, err = io.Copy(f, tr)
	if err != nil {
		_ = f.Close() //nolint:errcheck // best-effort close on write error

		return fmt.Errorf("write file %q: %w", path, err)
	}

	ctxErr := ctx.Err()
	if ctxErr != nil {
		_ = f.Close() //nolint:errcheck // best-effort close on cancel
		return fmt.Errorf("context cancelled: %w", ctxErr)
	}

	err = f.Close()
	if err != nil {
		return fmt.Errorf("close file %q: %w", path, err)
	}

	return nil
}

// extractSymlink creates a symbolic link. Absolute targets and targets
// resolving outside root are rejected to keep links inside the rootfs.
// Parent components must not traverse symlinks.
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
	err := rejectSymlinkPrefix(filepath.Dir(path), root)
	if err != nil {
		return err
	}
	err = os.MkdirAll(filepath.Dir(path), 0o700)
	if err != nil {
		return fmt.Errorf("mkdir parent for symlink %q: %w", path, err)
	}

	err = os.Symlink(hdr.Linkname, path)
	if err != nil {
		return fmt.Errorf("symlink %q -> %q: %w", path, hdr.Linkname, err)
	}
	return nil
}

// extractHardLink creates a hard link with the target resolved relative to rootfs.
// This follows OCI semantics where the hardlink source is root-relative, unlike
// symlinks which resolve relative to the link parent. Escape, missing target
// and link failures all fail the layer instead of skipping silently.
func extractHardLink(path string, hdr *tar.Header, root string) error {
	linkPath := filepath.Join(root, filepath.Clean(hdr.Linkname))
	cleanRoot := filepath.Clean(root)
	if linkPath != cleanRoot && !strings.HasPrefix(linkPath, cleanRoot+string(os.PathSeparator)) {
		return fmt.Errorf("hardlink target %q escapes root", hdr.Linkname)
	}

	err := rejectSymlinkPrefix(filepath.Dir(path), root)
	if err != nil {
		return err
	}
	err = os.MkdirAll(filepath.Dir(path), 0o700)
	if err != nil {
		return fmt.Errorf("mkdir parent for hardlink %q: %w", path, err)
	}
	err = rejectSymlinkPrefix(filepath.Dir(linkPath), root)
	if err != nil {
		return err
	}
	_, err = os.Lstat(linkPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("hardlink target %q not found", hdr.Linkname)
		}
		return fmt.Errorf("lstat hardlink target %q: %w", linkPath, err)
	}

	err = os.Link(linkPath, path)
	if err != nil {
		return fmt.Errorf("link %q to %q: %w", path, hdr.Linkname, err)
	}

	return nil
}

// clearDir removes all children of dir without following symlinks, keeping the directory itself.
func clearDir(dir, root string) error {
	err := rejectSymlinkPrefix(dir, root)
	if err != nil {
		return err
	}
	fi, err := os.Lstat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("lstat %q: %w", dir, err)
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("tar entry %q traverses symlink at %q", dir, dir)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return fmt.Errorf("read dir %q: %w", dir, err)
	}

	for _, entry := range entries {
		err = os.RemoveAll(filepath.Join(dir, entry.Name()))
		if err != nil {
			return fmt.Errorf("remove %q: %w", entry.Name(), err)
		}
	}

	return nil
}
