// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package transport

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// loadTLSConfig loads this node's cert/key and the CA trust pool.
// Used for both HTTPS server and client (same identity both ways).
func loadTLSConfig(caFile, certFile, keyFile string) (*tls.Config, error) {
	// Node identity (server + client).
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load node cert/key: %w", err)
	}

	// Certificate authority. Paths come from certs.Ensure (local config dir).
	// #nosec G304: ca path is local runtime configuration, not user-controlled input.
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read ca: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("no CA certs in %s", caFile)
	}

	return &tls.Config{
		MinVersion:   tls.VersionTLS13,               // TLS 1.3 only
		ClientAuth:   tls.RequireAndVerifyClientCert, // mTLS
		NextProtos:   []string{"h2"},                 // ALPN: HTTP/2 only
		ClientCAs:    pool,
		RootCAs:      pool,
		Certificates: []tls.Certificate{cert},
	}, nil
}
