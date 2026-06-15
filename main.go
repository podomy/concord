// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import "fmt"

func main() {
	_, err := fmt.Println("Hello!")
	if err != nil {
		panic(err)
	}
}
