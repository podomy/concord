// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package transport

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// loadTLSConfig loads this node's cert/key and the CA trust pool for the HTTPS server.
func loadTLSConfig(caFile, certFile, keyFile string) (*tls.Config, error) {
	cert, pool, err := loadCertAndPool(caFile, certFile, keyFile)
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		NextProtos:   []string{"h2"},
		ClientCAs:    pool,
		RootCAs:      pool,
		Certificates: []tls.Certificate{cert},
	}, nil
}

// loadClientTLSConfig loads material for outbound mTLS dials.
//
// Peer certs are verified against the CA only (no hostname/IP check). Nodes dial
// by memberlist IP while cert SANs carry node id / advertise IP; requiring the
// dial string to match SAN would break LAN peers without a matching IP SAN.
func loadClientTLSConfig(caFile, certFile, keyFile string) (*tls.Config, error) {
	cert, pool, err := loadCertAndPool(caFile, certFile, keyFile)
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		MinVersion:             tls.VersionTLS13,
		NextProtos:             []string{"h2"},
		RootCAs:                pool,
		Certificates:           []tls.Certificate{cert},
		InsecureSkipVerify:     true, //nolint:gosec // hostname skipped; VerifyConnection enforces CA
		SessionTicketsDisabled: true, // avoid resume bypassing custom verify
		VerifyConnection: func(cs tls.ConnectionState) error {
			return verifyPeerCertAgainstPool(cs.PeerCertificates, pool)
		},
	}, nil
}

func loadCertAndPool(caFile, certFile, keyFile string) (tls.Certificate, *x509.CertPool, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("load node cert/key: %w", err)
	}

	// #nosec G304: ca path is local runtime configuration, not user-controlled input.
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("read ca: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return tls.Certificate{}, nil, fmt.Errorf("no CA certs in %s", caFile)
	}
	return cert, pool, nil
}

func verifyPeerCertAgainstPool(peerCerts []*x509.Certificate, roots *x509.CertPool) error {
	if len(peerCerts) == 0 {
		return fmt.Errorf("peer certificate missing") //nolint:perfsprint // plain sentinel
	}
	leaf := peerCerts[0]
	intermediates := x509.NewCertPool()
	for _, c := range peerCerts[1:] {
		intermediates.AddCert(c)
	}
	opts := x509.VerifyOptions{
		Roots:         roots,
		Intermediates: intermediates,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	if _, err := leaf.Verify(opts); err != nil {
		return fmt.Errorf("peer certificate verify: %w", err)
	}
	return nil
}
