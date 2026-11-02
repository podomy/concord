// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package registry

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"net/http"
	"os"
	"testing"
	"time"
)

// auditFakeConn is a no-op net.Conn for hijack abort assertions.
type auditFakeConn struct{}

func (auditFakeConn) Read(_ []byte) (int, error)         { return 0, io.EOF }
func (auditFakeConn) Write(b []byte) (int, error)        { return len(b), nil }
func (auditFakeConn) Close() error                       { return nil }
func (auditFakeConn) LocalAddr() net.Addr                { return nil }
func (auditFakeConn) RemoteAddr() net.Addr               { return nil }
func (auditFakeConn) SetDeadline(_ time.Time) error      { return nil }
func (auditFakeConn) SetReadDeadline(_ time.Time) error  { return nil }
func (auditFakeConn) SetWriteDeadline(_ time.Time) error { return nil }

// auditHijackWriter records headers and body and reports Hijack.
type auditHijackWriter struct {
	header   http.Header
	body     bytes.Buffer
	status   int
	hijacked bool
}

func newAuditHijackWriter() *auditHijackWriter {
	return &auditHijackWriter{header: make(http.Header)}
}

func (w *auditHijackWriter) Header() http.Header { return w.header }

func (w *auditHijackWriter) Write(b []byte) (int, error) { return w.body.Write(b) } //nolint:wrapcheck // test writer passthrough

func (w *auditHijackWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *auditHijackWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	w.hijacked = true
	rw := bufio.NewReadWriter(bufio.NewReader(&w.body), bufio.NewWriter(&w.body))
	return auditFakeConn{}, rw, nil
}

// TestAuditUploadBound503 covers the concurrent upload semaphore.
// A full semaphore fails fast with 503; a drained one accepts 201.
func TestAuditUploadBound503(t *testing.T) {
	reg := newTestRegistry(t)
	repo := "testrepo/audit-bound"
	data := []byte("bound bytes")
	digest := digestOf(data)
	target := "/v2/" + repo + "/blobs/uploads?digest=" + digest

	for range maxConcurrentUploads {
		uploadSlots <- struct{}{}
	}
	rec := serve(t, reg, http.MethodPost, target, data)
	if rec.Code != http.StatusServiceUnavailable {
		for range maxConcurrentUploads {
			<-uploadSlots
		}
		t.Fatalf("status = %d, want 503, body = %s", rec.Code, rec.Body.String())
	}
	for range maxConcurrentUploads {
		<-uploadSlots
	}
	rec = serve(t, reg, http.MethodPost, target, data)
	requireCode(t, rec, http.StatusCreated)
}

// TestAuditLargeServeHappyPath covers the single-pass large serve path.
func TestAuditLargeServeHappyPath(t *testing.T) {
	reg := newTestRegistry(t)
	repo := "testrepo/audit-large"
	const size = (10 << 20) + 1024
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i * 31) % 251)
	}
	digest := digestOf(data)
	rec := serve(t, reg, http.MethodPost, "/v2/"+repo+"/blobs/uploads?digest="+digest, data)
	requireCode(t, rec, http.StatusCreated)
	rec = serve(t, reg, http.MethodGet, "/v2/"+repo+"/blobs/"+digest, nil)
	requireCode(t, rec, http.StatusOK)
	if !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatal("large serve bytes differ from upload")
	}
}

// auditWriteTemp writes data to a temp file in dir and returns its name.
func auditWriteTemp(t *testing.T, dir string, data []byte) string {
	t.Helper()
	tmp, err := os.CreateTemp(dir, ".tmp-audit-*")
	if err != nil {
		t.Fatal(err)
	}
	_, err = tmp.Write(data)
	if err != nil {
		t.Fatal(err)
	}
	err = tmp.Close()
	if err != nil {
		t.Fatal(err)
	}
	return tmp.Name()
}

// auditRequireNoRaw asserts no raw file exists for digest.
func auditRequireNoRaw(t *testing.T, dir, digest string) string {
	t.Helper()
	dest, ok := rawBlobPath(dir, digest)
	if !ok {
		t.Fatal("raw blob path rejected valid digest")
	}
	if _, statErr := os.Stat(dest); !os.IsNotExist(statErr) {
		t.Fatalf("corrupt install left %s behind", dest)
	}
	return dest
}

// TestAuditInstallRawRehash covers post-rename re-hash behavior.
// A temp file with wrong bytes must fail install and leave no dest.
func TestAuditInstallRawRehash(t *testing.T) {
	reg := newTestRegistry(t)
	dir := reg.storeDirFor()
	err := os.MkdirAll(dir, 0o700)
	if err != nil {
		t.Fatal(err)
	}
	good := []byte("audit rehash good")
	digest := digestOf(good)

	badName := auditWriteTemp(t, dir, []byte("audit rehash corrupt"))
	err = reg.installRaw(badName, digest)
	if err == nil {
		t.Fatal("installRaw with corrupt bytes succeeded, want hash mismatch")
	}
	dest := auditRequireNoRaw(t, dir, digest)

	goodName := auditWriteTemp(t, dir, good)
	err = reg.installRaw(goodName, digest)
	if err != nil {
		t.Fatalf("installRaw good bytes: %v", err)
	}
	got, err := os.ReadFile(dest) //nolint:gosec // test reads file it installed
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, good) {
		t.Fatal("installed bytes differ from source")
	}
}

// TestAuditServeRawLargeAbortOnMismatch covers truncation on hash mismatch.
// Headers commit 200, then the handler must abort the connection.
func TestAuditServeRawLargeAbortOnMismatch(t *testing.T) {
	reg := newTestRegistry(t)
	repo := "testrepo/audit-abort"
	const size = (10 << 20) + 1024
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i * 7) % 251)
	}
	digest := digestOf(data)
	rec := serve(t, reg, http.MethodPost, "/v2/"+repo+"/blobs/uploads?digest="+digest, data)
	requireCode(t, rec, http.StatusCreated)
	dest, ok := rawBlobPath(reg.storeDirFor(), digest)
	if !ok {
		t.Fatal("raw blob path rejected valid digest")
	}
	f, err := os.OpenFile(dest, os.O_WRONLY, 0o600) //nolint:gosec // test corrupts file it created
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteAt([]byte{data[0] ^ 0xff}, 0)
	if err != nil {
		_ = f.Close() //nolint:errcheck // best-effort close on write error
		t.Fatal(err)
	}
	err = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	w := newAuditHijackWriter()
	reg.serveRawLarge(w, digest, dest)
	if w.status != http.StatusOK {
		t.Fatalf("status = %d, want 200 committed before abort", w.status)
	}
	if !w.hijacked {
		t.Fatal("serveRawLarge did not abort connection on digest mismatch")
	}
}
