package inbox

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// makeDB creates a SQLite file with the given statements, using the plain driver (no WAL), as another program would.
func makeDB(t *testing.T, path string, stmts ...string) {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, s := range stmts {
		if _, err = db.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
}

func TestForeignDatabasesAreRefusedUntouched(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name  string
		setup func(t *testing.T, path string)
		want  error
	}{
		{"foreign tables", func(t *testing.T, path string) {
			makeDB(t, path, `CREATE TABLE foreign_thing (x INTEGER)`, `INSERT INTO foreign_thing VALUES (1)`, `PRAGMA user_version=77`)
		}, ErrForeignOwner},
		{"foreign view only", func(t *testing.T, path string) {
			makeDB(t, path, `CREATE VIEW v AS SELECT 1`)
		}, ErrForeignOwner},
		{"another application id", func(t *testing.T, path string) {
			makeDB(t, path, `PRAGMA application_id=1234`)
		}, ErrForeignOwner},
		{"engine_meta of another owner", func(t *testing.T, path string) {
			makeDB(t, path, `CREATE TABLE engine_meta (key TEXT PRIMARY KEY, value BLOB NOT NULL)`,
				`INSERT INTO engine_meta VALUES ('owner', 'someone-else'), ('schema_version', '1')`)
		}, ErrForeignOwner},
		{"engine_meta with other columns", func(t *testing.T, path string) {
			makeDB(t, path, `CREATE TABLE engine_meta (a INTEGER)`)
		}, ErrForeignOwner},
		{"not a database", func(t *testing.T, path string) {
			if err := os.WriteFile(path, bytes.Repeat([]byte("not sqlite at all\n"), 400), 0o600); err != nil {
				t.Fatal(err)
			}
		}, ErrForeignOwner},
		{"newer engine", func(t *testing.T, path string) {
			makeDB(t, path, `CREATE TABLE engine_meta (key TEXT PRIMARY KEY, value BLOB NOT NULL)`,
				`INSERT INTO engine_meta VALUES ('owner', 'cappy-engine'), ('schema_version', '`+strconv.Itoa(SchemaVersion+1)+`')`)
		}, ErrNewerSchema},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "inbox.db")
			tc.setup(t, path)
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			db, err := Open(ctx, path)
			if err == nil {
				_ = db.Close()
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("expected %v, got %v", tc.want, err)
			}
			after, _ := os.ReadFile(path)
			if tc.want == ErrForeignOwner && !bytes.Equal(before, after) {
				t.Fatal("a foreign file was modified")
			}
		})
	}
}

func TestEmptyFileIsAdoptedAndTagged(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "inbox.db")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	var appID int64
	if err = db.db.QueryRow(`PRAGMA application_id`).Scan(&appID); err != nil || appID != ApplicationID {
		t.Fatalf("application_id = %d (%v)", appID, err)
	}
	_ = db.Close()
	// A first start that stopped after creating engine_meta (but before the schema) is still ours to finish.
	path2 := filepath.Join(t.TempDir(), "inbox.db")
	makeDB(t, path2, `CREATE TABLE engine_meta (key TEXT PRIMARY KEY, value BLOB NOT NULL)`)
	if db, err = Open(ctx, path2); err != nil {
		t.Fatalf("half-created inbox refused: %v", err)
	}
	_ = db.Close()
	// Reopening our own file works.
	if db, err = Open(ctx, path); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
}

// TestUpgradeFromV1 opens a schema v1 file written by the first spike build: it gets the application ID, the edit
// links, and the stored content of messages deleted under v1 is blanked.
func TestUpgradeFromV1(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "inbox.db")
	makeDB(t, path, schemaV1,
		`INSERT INTO engine_meta VALUES ('owner', 'cappy-engine'), ('schema_version', '1')`,
		`INSERT INTO accounts (account_id, network, login_id, state, created_at) VALUES ('acc_1', 'whatsapp', 'l1', 'UNKNOWN', 1)`,
		`INSERT INTO chats (room_id, account_id, network, portal_id, portal_receiver, kind) VALUES ('!r:cappy.local', 'acc_1', 'whatsapp', 'p', 'l1', 'dm')`,
		`INSERT INTO messages (event_id, room_id, from_me, ts, kind, redacted_ts, source) VALUES ('$gone', '!r:cappy.local', 0, 1, 'message', 5, 'live')`,
		`INSERT INTO messages (event_id, room_id, from_me, ts, kind, body, source) VALUES ('$kept', '!r:cappy.local', 0, 2, 'message', 'synthetic kept', 'live')`,
		`INSERT INTO raw_events VALUES ('$gone', '!r:cappy.local', 'm.room.message', '@a:cappy.local', 1, '{"msgtype":"m.text","body":"CANARY-GONE"}')`,
		`INSERT INTO raw_events VALUES ('$edit', '!r:cappy.local', 'm.room.message', '@a:cappy.local', 2, '{"msgtype":"m.text","body":"* CANARY-EDIT","m.new_content":{"body":"CANARY-EDIT"},"m.relates_to":{"rel_type":"m.replace","event_id":"$gone"}}')`,
		`INSERT INTO raw_events VALUES ('$kept', '!r:cappy.local', 'm.room.message', '@a:cappy.local', 3, '{"msgtype":"m.text","body":"synthetic kept"}')`,
		`INSERT INTO raw_events VALUES ('$broken', '!r:cappy.local', 'm.room.message', '@a:cappy.local', 4, 'not json')`,
	)
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version string
	if err = db.db.QueryRow(`SELECT value FROM engine_meta WHERE key='schema_version'`).Scan(&version); err != nil || version != strconv.Itoa(SchemaVersion) {
		t.Fatalf("schema_version = %q (%v)", version, err)
	}
	var replaces sql.NullString
	if err = db.db.QueryRow(`SELECT replaces FROM raw_events WHERE event_id='$edit'`).Scan(&replaces); err != nil || replaces.String != "$gone" {
		t.Fatalf("edit link not filled in: %q (%v)", replaces.String, err)
	}
	assertNoCanary(t, db)
	if raw, err := db.RawEvent(ctx, "$kept"); err != nil || !strings.Contains(raw.Content, "synthetic kept") {
		t.Fatal("content of a message that was not deleted was lost")
	}
}

// assertNoCanary checks that no stored raw event still holds text marked CANARY.
func assertNoCanary(t *testing.T, db *DB) {
	t.Helper()
	var n int
	if err := db.db.QueryRow(`SELECT count(*) FROM raw_events WHERE content LIKE '%CANARY%'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("%d raw events still hold deleted text", n)
	}
}

func TestRedactBlanksRawEvents(t *testing.T) {
	db := openTest(t)
	ctx := context.Background()
	_, alice := seed(t, db)
	mustTx(t, db, func(tx *Tx) error {
		if err := tx.PutRawEvent("$m1", roomA, "m.room.message", "@alice:cappy.local", 1, `{"msgtype":"m.text","body":"CANARY original"}`, ""); err != nil {
			return err
		}
		if _, err := tx.PutMessage(NewMessage{EventID: "$m1", RoomID: roomA, SenderContactID: alice, TS: 1, Kind: "message", Body: "CANARY original", Source: SourceLive}); err != nil {
			return err
		}
		if err := tx.PutRawEvent("$e1", roomA, "m.room.message", "@alice:cappy.local", 2, `{"m.new_content":{"body":"CANARY edited"},"m.relates_to":{"rel_type":"m.replace","event_id":"$m1"}}`, "$m1"); err != nil {
			return err
		}
		if err := tx.EditMessage("$m1", "CANARY edited", "", 2); err != nil {
			return err
		}
		if err := tx.PutRawEvent("$r1", roomA, "m.reaction", "@alice:cappy.local", 3, `{"m.relates_to":{"rel_type":"m.annotation","event_id":"$m1","key":"CANARY-KEY"}}`, ""); err != nil {
			return err
		}
		return tx.PutReaction(NewReaction{EventID: "$r1", TargetEventID: "$m1", SenderContactID: alice, Key: "CANARY-KEY", TS: 3})
	})
	// Removing the reaction drops its stored event.
	mustTx(t, db, func(tx *Tx) error { return tx.Redact("$r1", 4, "unsend") })
	if _, err := db.RawEvent(ctx, "$r1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("removed reaction's raw event kept: %v", err)
	}
	// Deleting the message blanks its stored event and its edit; the rows stay so GetEvent can report the redaction.
	mustTx(t, db, func(tx *Tx) error { return tx.Redact("$m1", 5, "disappear") })
	raw, err := db.RawEvent(ctx, "$m1")
	if err != nil || raw.RedactedTS != 5 || raw.Content != blankContent {
		t.Fatalf("deleted message's raw event: %+v (%v)", raw, err)
	}
	// An edit that arrives after the deletion is stored without its text.
	mustTx(t, db, func(tx *Tx) error {
		return tx.PutRawEvent("$e2", roomA, "m.room.message", "@alice:cappy.local", 6, `{"m.new_content":{"body":"CANARY late edit"},"m.relates_to":{"rel_type":"m.replace","event_id":"$m1"}}`, "$m1")
	})
	assertNoCanary(t, db)
	var plan string
	rows, err := db.db.Query(`EXPLAIN QUERY PLAN UPDATE raw_events SET content='{}' WHERE event_id=? OR replaces=?`, "$x", "$x")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id, parent, unused int
		var detail string
		_ = rows.Scan(&id, &parent, &unused, &detail)
		plan += detail + "\n"
	}
	_ = rows.Close()
	if strings.Contains(plan, "SCAN raw_events") {
		t.Fatalf("redaction scans raw_events:\n%s", plan)
	}
}
