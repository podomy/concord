// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package journalreader

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// Blank and whitespace-only lines are skipped, not errors: hand edits
// leave empty lines, and the committed prefix still reads back.
func TestJSONLReaderSkipsBlankLines(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "journal.jsonl")
	line1 := `{"a":1}`
	line2 := `{"b":2}`
	content := line1 + "\n\n   \n\t\n" + line2 + "\n\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write journal: %v", err)
	}

	reader, err := OpenJSONLReaderPath(path)
	if err != nil {
		t.Fatalf("open journal reader: %v", err)
	}
	t.Cleanup(func() {
		if err := reader.Close(); err != nil {
			t.Fatalf("close journal reader: %v", err)
		}
	})

	ctx := context.Background()
	mustReadOffset(t, reader, ctx, 0)
	off2 := int64(len(line1) + 8) // line1\n + blank\n + spaces\n + tab\n
	mustReadOffset(t, reader, ctx, off2)
	mustReadEOF(t, reader, ctx)
}
