// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package transport

import (
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/noise"

	"github.com/podomy/concord/internal/certs"
)

// noisePrologue is mixed into every handshake hash. Both sides must present
// the identical value or the handshake fails, which binds each session to this
// protocol and blocks cross-protocol splice attacks.
const noisePrologue = "concord-noise-transport-v1"

// toNoiseStatic converts a transport key to the fixed-size noise key. It
// rejects halves of the wrong length here so a truncated key never reaches
// the handshake.
func toNoiseStatic(self StaticKey) (noise.StaticKey, error) {
	if len(self.Private) != 32 {
		return noise.StaticKey{}, fmt.Errorf("noise private key must be 32 bytes, got %d", len(self.Private))
	}
	if len(self.Public) != 32 {
		return noise.StaticKey{}, fmt.Errorf("noise public key must be 32 bytes, got %d", len(self.Public))
	}
	var out noise.StaticKey
	copy(out.Private[:], self.Private)
	copy(out.Public[:], self.Public)
	return out, nil
}

// handshakeTimeout bounds one Noise handshake over TCP.
const handshakeTimeout = 10 * time.Second

// handshakeFrameMax is the largest handshake message accepted. IK messages
// carry two public keys plus a ~280-byte identity parcel; 2 KiB leaves wide
// margin.
const handshakeFrameMax = 2048

// frameLenBytes is the width of the length prefix on every frame.
const frameLenBytes = 4

// Peer is the expected responder: node ID plus static public key, both learned
// from gossip. The CA signature arrives inside the session, never from gossip.
type Peer struct {
	ID  uuid.UUID
	Pub []byte
}

// Verifier checks a presented Noise parcel: node ID, rotation generation,
// static public key, CA signature. A nil error admits the peer. Revocation
// plugs in here later: same signature, deny revoked keys first.
type Verifier func(id uuid.UUID, generation uint64, pub, sig []byte) error

// CAVerifier admits exactly the keys bearing the fleet CA signature. caCert
// is used only as storage for the CA public key; nothing else in the
// certificate matters.
func CAVerifier(caCert *x509.Certificate) Verifier {
	return func(id uuid.UUID, generation uint64, pub, sig []byte) error {
		err := certs.VerifyNodeKey(caCert, id, generation, pub, sig)
		if err != nil {
			return fmt.Errorf("peer %s parcel: %w", id, err)
		}

		return nil
	}
}

// parcelIDLen and parcelGenLen are the fixed widths opening every parcel:
// 16-byte node ID, 8-byte big-endian generation, then the CA signature.
const (
	parcelIDLen  = 16
	parcelGenLen = 8
)

// EncodeParcel serializes one side's parcel for the session: node ID,
// generation, CA signature. Runtime calls it once at boot with the signed
// parcel; both handshake payloads use this encoding.
func EncodeParcel(id uuid.UUID, generation uint64, sig []byte) []byte {
	out := make([]byte, 0, parcelIDLen+parcelGenLen+len(sig))
	out = append(out, id[:]...)
	out = binary.BigEndian.AppendUint64(out, generation)
	out = append(out, sig...)

	return out
}

// decodeParcel parses a session parcel. The signature is the variable-length
// tail, so CA key size changes need no format change.
func decodeParcel(raw []byte) (uuid.UUID, uint64, []byte, error) {
	if len(raw) < parcelIDLen+parcelGenLen {
		return uuid.UUID{}, 0, nil, fmt.Errorf("parcel must hold id and generation, got %d bytes", len(raw))
	}

	id, err := uuid.FromBytes(raw[:parcelIDLen])
	if err != nil {
		return uuid.UUID{}, 0, nil, fmt.Errorf("parcel node id: %w", err)
	}

	return id, binary.BigEndian.Uint64(raw[parcelIDLen : parcelIDLen+parcelGenLen]), raw[parcelIDLen+parcelGenLen:], nil
}

// writeFrame writes one length-prefixed frame: 4 big-endian length bytes
// followed by exactly that many payload bytes. Payloads here are handshake
// messages and Noise ciphertexts, all far below 4 GiB, so the length conversion
// cannot overflow.
func writeFrame(w io.Writer, payload []byte) error {
	var prefix [frameLenBytes]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(payload))) //nolint:gosec // payloads bounded by MaxMsgLen, see above

	_, err := w.Write(prefix[:])
	if err != nil {
		return fmt.Errorf("write frame length: %w", err)
	}

	_, err = w.Write(payload)
	if err != nil {
		return fmt.Errorf("write frame payload: %w", err)
	}

	return nil
}

// readFrame reads one length-prefixed frame, rejecting lengths above maxLen
// before allocating.
func readFrame(r io.Reader, maxLen uint32) ([]byte, error) {
	var prefix [frameLenBytes]byte
	_, err := io.ReadFull(r, prefix[:])
	if err != nil {
		return nil, fmt.Errorf("read frame length: %w", err)
	}

	length := binary.BigEndian.Uint32(prefix[:])
	if length > maxLen {
		return nil, fmt.Errorf("frame length %d exceeds max %d", length, maxLen)
	}

	payload := make([]byte, length)
	_, err = io.ReadFull(r, payload)
	if err != nil {
		return nil, fmt.Errorf("read frame payload: %w", err)
	}

	return payload, nil
}

// dialHandshake opens a TCP connection to addr and runs the IK initiator
// handshake for self against the peer's known static key. It returns the
// encrypted channel plus the responder's parcel, which the caller must verify
// before sending application bytes.
func dialHandshake(
	dialer *net.Dialer,
	addr string,
	self StaticKey,
	selfParcel []byte,
	expectPub []byte,
) (*noiseConn, []byte, error) {
	raw, err := dialer.Dial("tcp", addr)
	if err != nil {
		return nil, nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	err = raw.SetDeadline(time.Now().Add(handshakeTimeout))
	if err != nil {
		_ = raw.Close() //nolint:errcheck // best-effort close on failed handshake
		return nil, nil, fmt.Errorf("set handshake deadline: %w", err)
	}

	conn, parcel, err := initiatorHandshake(raw, self, selfParcel, expectPub)
	if err != nil {
		_ = raw.Close() //nolint:errcheck // best-effort close on failed handshake
		return nil, nil, err
	}

	err = raw.SetDeadline(time.Time{})
	if err != nil {
		_ = raw.Close() //nolint:errcheck // best-effort close on failed handshake
		return nil, nil, fmt.Errorf("clear handshake deadline: %w", err)
	}

	return conn, parcel, nil
}

// initiatorHandshake runs the IK initiator side over raw. selfParcel is our
// encoded parcel sent as the first-message payload; expectPub is the
// responder's pre-known static key. It returns the channel plus the
// responder's encoded parcel. The caller sets a deadline on raw first.
func initiatorHandshake(raw net.Conn, self StaticKey, selfParcel, expectPub []byte) (*noiseConn, []byte, error) {
	selfKey, err := toNoiseStatic(self)
	if err != nil {
		return nil, nil, err
	}
	var peerStatic [32]byte
	if len(expectPub) != 32 {
		return nil, nil, fmt.Errorf("peer public key must be 32 bytes, got %d", len(expectPub))
	}
	copy(peerStatic[:], expectPub)

	hs, err := noise.NewIKInitiator([]byte(noisePrologue), selfKey, peerStatic)
	if err != nil {
		return nil, nil, fmt.Errorf("noise initiator: %w", err)
	}

	msg, err := hs.WriteRequest(selfParcel)
	if err != nil {
		return nil, nil, fmt.Errorf("noise write request: %w", err)
	}

	err = writeFrame(raw, msg)
	if err != nil {
		return nil, nil, err
	}

	reply, err := readFrame(raw, handshakeFrameMax)
	if err != nil {
		return nil, nil, err
	}

	parcel, send, recv, err := hs.ReadResponse(reply)
	if err != nil {
		return nil, nil, fmt.Errorf("noise read response: %w", err)
	}

	conn := &noiseConn{conn: raw, send: send, recv: recv}

	return conn, parcel, nil
}

// serveHandshake runs the IK responder side over an accepted connection. Our
// encoded parcel goes out as the reply payload; the initiator's parcel
// arrives as the request payload and is checked with verify before serving. It
// returns the encrypted channel, or an error after which the caller must close
// raw. The caller sets a deadline on raw first.
func serveHandshake(raw net.Conn, static StaticKey, selfParcel []byte, verify Verifier) (*noiseConn, error) {
	selfKey, err := toNoiseStatic(static)
	if err != nil {
		return nil, err
	}

	hs, err := noise.NewIKResponder([]byte(noisePrologue), selfKey)
	if err != nil {
		return nil, fmt.Errorf("noise responder: %w", err)
	}

	msg, err := readFrame(raw, handshakeFrameMax)
	if err != nil {
		return nil, err
	}

	payload, peerStatic, err := hs.ReadRequest(msg)
	if err != nil {
		return nil, fmt.Errorf("noise read request: %w", err)
	}

	peerID, generation, sig, err := decodeParcel(payload)
	if err != nil {
		return nil, err
	}

	// The presented static key must be the membership-bound key for the
	// claimed node ID: same bytes the CA signed, not a substitute. Checked
	// before any reply bytes go out.
	err = verify(peerID, generation, peerStatic[:], sig)
	if err != nil {
		return nil, err
	}

	reply, send, recv, err := hs.WriteResponse(selfParcel)
	if err != nil {
		return nil, fmt.Errorf("noise write response: %w", err)
	}

	err = writeFrame(raw, reply)
	if err != nil {
		return nil, err
	}

	conn := &noiseConn{conn: raw, send: send, recv: recv}

	return conn, nil
}

// LoadCACert parses the fleet CA certificate from caFile. Only the RSA public
// key inside is ever used; no X.509 verification runs against it. Runtime uses
// it to build the transport verifier; tests use it directly.
func LoadCACert(caFile string) (*x509.Certificate, error) {
	// #nosec G304: caFile is local runtime configuration, not user input.
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read ca: %w", err)
	}

	block, _ := pem.Decode(caPEM)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("ca file holds no CERTIFICATE PEM") //nolint:perfsprint // plain sentinel, no wrap target
	}

	ca, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse ca certificate: %w", err)
	}

	return ca, nil
}

// maxTransportFrame caps one decrypted transport read: the largest Noise
// message plus its authentication tag plus the length prefix.
const maxTransportFrame = noise.MaxMsgLen + 16 + frameLenBytes

// noiseConn is a net.Conn carrying length-prefixed Noise transport messages.
// Reads and writes may proceed concurrently; each direction holds its own
// Transport and mutex.
type noiseConn struct {
	conn net.Conn
	send *noise.Transport
	recv *noise.Transport

	sendMu sync.Mutex
	recvMu sync.Mutex

	// pending holds decrypted bytes not yet consumed by Read.
	pending []byte
}

// Read returns decrypted stream bytes, reading and decrypting one frame when
// the buffer is empty.
func (c *noiseConn) Read(p []byte) (int, error) {
	c.recvMu.Lock()
	defer c.recvMu.Unlock()

	if len(c.pending) == 0 {
		frame, err := readFrame(c.conn, maxTransportFrame)
		if err != nil {
			return 0, err
		}

		plain, err := c.recv.Decrypt(nil, nil, frame)
		if err != nil {
			return 0, fmt.Errorf("noise decrypt: %w", err)
		}
		c.pending = plain
	}

	n := copy(p, c.pending)
	c.pending = c.pending[n:]

	return n, nil
}

// Write encrypts p in MaxMsgLen chunks and writes one frame per chunk.
func (c *noiseConn) Write(p []byte) (int, error) {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()

	written := 0
	for len(p) > 0 {
		chunk := min(len(p), noise.MaxMsgLen)

		ciphertext, err := c.send.Encrypt(nil, nil, p[:chunk])
		if err != nil {
			return written, fmt.Errorf("noise encrypt: %w", err)
		}

		err = writeFrame(c.conn, ciphertext)
		if err != nil {
			return written, err
		}

		p = p[chunk:]
		written += chunk
	}

	return written, nil
}

// Close closes the underlying connection.
func (c *noiseConn) Close() error {
	err := c.conn.Close()
	if err != nil {
		return fmt.Errorf("close noise conn: %w", err)
	}

	return nil
}

// LocalAddr returns the underlying local address.
func (c *noiseConn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// RemoteAddr returns the underlying remote address.
func (c *noiseConn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// SetDeadline delegates to the underlying connection.
func (c *noiseConn) SetDeadline(t time.Time) error {
	err := c.conn.SetDeadline(t)
	if err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	return nil
}

// SetReadDeadline delegates to the underlying connection.
func (c *noiseConn) SetReadDeadline(t time.Time) error {
	err := c.conn.SetReadDeadline(t)
	if err != nil {
		return fmt.Errorf("set read deadline: %w", err)
	}

	return nil
}

// SetWriteDeadline delegates to the underlying connection.
func (c *noiseConn) SetWriteDeadline(t time.Time) error {
	err := c.conn.SetWriteDeadline(t)
	if err != nil {
		return fmt.Errorf("set write deadline: %w", err)
	}

	return nil
}
