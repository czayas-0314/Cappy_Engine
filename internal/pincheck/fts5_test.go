package pincheck

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "go.mau.fi/util/dbutil/litestream" // registers the "sqlite3-fk-wal" driver used by mautrix
)

// TestFTS5Available proves that the cgo SQLite build used by mautrix has FTS5
// when built with -tags sqlite_fts5. It writes only synthetic text.
func TestFTS5Available(t *testing.T) {
	db, err := sql.Open("sqlite3-fk-wal", "file:"+filepath.Join(t.TempDir(), "fts.db")+"?_txlock=immediate")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err = db.ExecContext(ctx, `CREATE VIRTUAL TABLE t USING fts5(body, tokenize='unicode61 remove_diacritics 2')`); err != nil {
		t.Fatalf("fts5 unavailable (build with -tags sqlite_fts5): %v", err)
	}
	if _, err = db.ExecContext(ctx, `INSERT INTO t(body) VALUES ('synthetic café sample'), ('other row')`); err != nil {
		t.Fatal(err)
	}
	var n int
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM t WHERE t MATCH '"cafe"'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 match, got %d", n)
	}
}
