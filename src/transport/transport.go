// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package transport

import (
	"crypto/tls"
	"net/http"
	"time"
)

const (
	Port = "8443"
)

func InitTransport() (*http.Server, error) {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /v1/sync", postSync)

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS13,               // TLS 1.3 only
		ClientAuth: tls.RequireAndVerifyClientCert, // mTLS
		NextProtos: []string{"h2"},                 // ALPN: HTTP/2 only
	}

	srv := &http.Server{
		Addr: ":" + Port,
		// middleware
		Handler:           chain(mux, requireHTTP2),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		TLSConfig:         tlsConfig,
	}

	return srv, nil
}
