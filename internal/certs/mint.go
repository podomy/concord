// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package certs

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
)

// MintNode loads the CA from the default paths and writes a new node.crt and
// node.key for nodeID. Requires ca.crt and ca.key already on disk.
//
// Node cert SANs always include the node ID and localhost as DNS names, plus
// loopback IPs. If advertiseAddress is valid, that IP is added as an IP SAN.
// Overwrites existing node cert/key only.
func MintNode(nodeID uuid.UUID, advertiseAddress netip.Addr) error {
	paths, err := DefaultPaths()
	if err != nil {
		return fmt.Errorf("default paths: %w", err)
	}

	caCert, caKey, err := loadCA(paths)
	if err != nil {
		return err
	}

	nodeDER, nodeKey, err := createNode(nodeID, caCert, caKey, advertiseAddress)
	if err != nil {
		return err
	}

	if err = writePEM(paths.Cert, "CERTIFICATE", nodeDER); err != nil {
		return fmt.Errorf("write pem: %w", err)
	}
	if err = writePEM(paths.Key, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(nodeKey)); err != nil {
		return fmt.Errorf("write pem: %w", err)
	}
	return nil
}

// createNode generates this node's RSA key and a certificate signed by the CA.
//
// SANs name this node only: node ID, localhost, loopback, optional advertise.
func createNode(nodeID uuid.UUID, caCert *x509.Certificate, caKey *rsa.PrivateKey, advertiseAddress netip.Addr) ([]byte, *rsa.PrivateKey, error) {
	nodeKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, fmt.Errorf("rsa generate node key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		return nil, nil, fmt.Errorf("serial: %w", err)
	}

	ips := []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback}
	if advertiseAddress.IsValid() {
		ips = append(ips, advertiseAddress.AsSlice())
	}

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "Concord Node: " + nodeID.String()},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{nodeID.String(), "localhost"},
		IPAddresses:  ips,
	}

	nodeDER, err := x509.CreateCertificate(rand.Reader, template, caCert, &nodeKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, fmt.Errorf("create node certificate: %w", err)
	}
	return nodeDER, nodeKey, nil
}

// writePEM writes der as a single PEM block of the given type to path.
// Files are created or truncated with mode 0600 (owner read/write only).
func writePEM(path, blockType string, der []byte) (err error) {
	f, err := os.OpenFile(filepath.Clean(path), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			if err != nil {
				err = fmt.Errorf("close %s: %w; original error: %w", path, closeErr, err)
				return
			}
			err = fmt.Errorf("close %s: %w", path, closeErr)
		}
	}()
	if err = pem.Encode(f, &pem.Block{Type: blockType, Bytes: der}); err != nil {
		return fmt.Errorf("encode %s: %w", path, err)
	}
	return nil
}
