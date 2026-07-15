// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package transport

import (
	"net/http"
	"slices"
)

func requireHTTP2(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 2 {
			http.Error(w, "HTTP/2 required", http.StatusHTTPVersionNotSupported)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func chain(h http.Handler, mws ...func(http.Handler) http.Handler) http.Handler {
	// apply in reverse so chain(h, A, B, C) => A(B(C(h)))
	for _, mw := range slices.Backward(mws) {
		h = mw(h)
	}
	return h
}
