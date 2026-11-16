// Package inbox is Cappy Engine's own SQLite store (inbox.db): accounts, contacts, chats, messages with FTS5 search,
// reactions, receipts, room state and raw events. It is written only by the local Matrix connector (localmx) and the
// engine. mautrix's own tables live in a separate file (bridge.db), so the two write locks never block each other.
//
// It uses mattn/go-sqlite3 (cgo) through the "sqlite3-fk-wal" driver that go.mau.fi/util registers, so inbox.db and
// bridge.db share one SQLite build. FTS5 needs the build tag sqlite_fts5.
package inbox

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/mattn/go-sqlite3"
	_ "go.mau.fi/util/dbutil/litestream" // registers the "sqlite3-fk-wal" driver (mattn/go-sqlite3)
)

var (
	ErrNotFound     = errors.New("not found")
	ErrNewerSchema  = errors.New("inbox.db was written by a newer engine")
	ErrForeignOwner = errors.New("inbox.db belongs to another program")
)

// ApplicationID is stored in the SQLite file header of inbox.db (PRAGMA application_id). It is "CAPY" in ASCII.
const ApplicationID = 0x43415059

type DB struct {
	db         *sql.DB
	path       string
	installKey []byte
}

// Open opens (creating if needed) and migrates inbox.db. The file is created with mode 0600. A file that is not Cappy
// Engine's (another program's SQLite database, or not SQLite at all) is refused with ErrForeignOwner before anything
// is written to it.
func Open(ctx context.Context, path string) (*DB, error) {
	if f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600); err != nil {
		return nil, err
	} else {
		_ = f.Close()
	}
	if err := checkOwner(ctx, path); err != nil {
		return nil, err
	}
	// secure_delete overwrites deleted and replaced content (eg the text of a deleted message) instead of leaving it in
	// free space in the file.
	sqlDB, err := sql.Open("sqlite3-fk-wal", "file:"+path+"?_txlock=immediate&_cache_size=-4096&_secure_delete=on")
	if err != nil {
		return nil, err
	}
	sqlDB.SetMaxOpenConns(4)
	sqlDB.SetMaxIdleConns(2)
	sqlDB.SetConnMaxIdleTime(5 * time.Minute)
	d := &DB{db: sqlDB, path: path}
	if err = d.migrate(ctx); err != nil {
		_ = sqlDB.Close()
		return nil, err
	}
	if err = d.loadInstallKey(ctx); err != nil {
		_ = sqlDB.Close()
		return nil, err
	}
	return d, nil
}

// checkOwner refuses a file that another program owns. It runs on its own query-only connection, because the main
// pool's connect hook already switches the file to WAL mode, which would change a foreign database.
//
// The file is Cappy Engine's if its engine_meta table names this owner. Otherwise it is accepted only if it is empty
// (new, or a first start that stopped before the schema was created): it has no tables, views, indexes or triggers
// except engine_meta.
func checkOwner(ctx context.Context, path string) error {
	db, err := sql.Open("sqlite3", "file:"+path+"?mode=rw&_query_only=1&_busy_timeout=5000")
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	var appID int64
	if err = db.QueryRowContext(ctx, `PRAGMA application_id`).Scan(&appID); err != nil {
		return foreignIfNotADB(err)
	}
	if appID != 0 && appID != ApplicationID {
		return ErrForeignOwner
	}
	var others, hasMeta int
	if err = db.QueryRowContext(ctx, `SELECT
			count(*) FILTER (WHERE name <> 'engine_meta' AND name NOT LIKE 'sqlite\_%' ESCAPE '\'),
			count(*) FILTER (WHERE type='table' AND name='engine_meta')
		FROM sqlite_master`).Scan(&others, &hasMeta); err != nil {
		return foreignIfNotADB(err)
	}
	owner := ""
	if hasMeta > 0 {
		var value []byte
		err = db.QueryRowContext(ctx, `SELECT value FROM engine_meta WHERE key='owner'`).Scan(&value)
		var sqlErr sqlite3.Error
		switch {
		case err == nil:
			owner = string(value)
		case errors.Is(err, sql.ErrNoRows):
		case errors.As(err, &sqlErr) && sqlErr.Code == sqlite3.ErrError:
			// An engine_meta table without key/value columns is someone else's.
			return ErrForeignOwner
		default:
			return err
		}
	}
	if owner != "" && owner != Owner {
		return ErrForeignOwner
	}
	if owner == "" && others > 0 {
		return ErrForeignOwner
	}
	return nil
}

func foreignIfNotADB(err error) error {
	var sqlErr sqlite3.Error
	if errors.As(err, &sqlErr) && sqlErr.Code == sqlite3.ErrNotADB {
		return ErrForeignOwner
	}
	return err
}

func (d *DB) migrate(ctx context.Context) error {
	if _, err := d.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS engine_meta (key TEXT PRIMARY KEY, value BLOB NOT NULL)`); err != nil {
		return err
	}
	var owner string
	err := d.db.QueryRowContext(ctx, `SELECT value FROM engine_meta WHERE key='owner'`).Scan(&owner)
	if err == nil && owner != Owner {
		return ErrForeignOwner
	} else if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var versionStr string
	version := 0
	err = d.db.QueryRowContext(ctx, `SELECT value FROM engine_meta WHERE key='schema_version'`).Scan(&versionStr)
	if err == nil {
		if version, err = strconv.Atoi(versionStr); err != nil {
			return fmt.Errorf("bad schema_version in inbox.db: %w", err)
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if version > SchemaVersion {
		return ErrNewerSchema
	}
	setAppID := fmt.Sprintf(`PRAGMA application_id = %d`, ApplicationID)
	if version == SchemaVersion {
		// Files written before the application ID existed get it now.
		var appID int64
		if err = d.db.QueryRowContext(ctx, `PRAGMA application_id`).Scan(&appID); err != nil {
			return err
		}
		if appID != ApplicationID {
			_, err = d.db.ExecContext(ctx, setAppID)
		}
		return err
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if version < 1 {
		if _, err = tx.ExecContext(ctx, schemaV1); err != nil {
			return fmt.Errorf("failed to create inbox schema: %w", err)
		}
	}
	if version < 2 {
		if _, err = tx.ExecContext(ctx, schemaV2); err != nil {
			return fmt.Errorf("failed to upgrade inbox schema to v2: %w", err)
		}
	}
	if _, err = tx.ExecContext(ctx, setAppID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO engine_meta (key, value) VALUES ('owner', ?), ('schema_version', ?)
		ON CONFLICT (key) DO UPDATE SET value=excluded.value`, Owner, strconv.Itoa(SchemaVersion)); err != nil {
		return err
	}
	return tx.Commit()
}

func (d *DB) loadInstallKey(ctx context.Context) error {
	var key []byte
	err := d.db.QueryRowContext(ctx, `SELECT value FROM engine_meta WHERE key='install_key'`).Scan(&key)
	if errors.Is(err, sql.ErrNoRows) {
		key = make([]byte, 32)
		if _, err = rand.Read(key); err != nil {
			return err
		}
		_, err = d.db.ExecContext(ctx, `INSERT INTO engine_meta (key, value) VALUES ('install_key', ?)`, key)
	}
	if err != nil {
		return err
	}
	if len(key) != 32 {
		return errors.New("install key in inbox.db is corrupt")
	}
	d.installKey = key
	return nil
}

// InstallKey is 32 random bytes created on first start. Room and message IDs are HMACs under it, so they are stable
// for this install but cannot be brute-forced from phone numbers.
func (d *DB) InstallKey() []byte {
	return d.installKey
}

func (d *DB) Path() string {
	return d.path
}

// Close checkpoints the WAL into the main file and closes the database.
func (d *DB) Close() error {
	_, _ = d.db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`)
	return d.db.Close()
}

// MessageChange is a change to an existing message, for a message.updated notification.
type MessageChange struct {
	EventID string
	Change  string // edit | delete | reactions | status | receipts
}

// Changes collects what a transaction changed, so notifications can be sent after it commits.
type Changes struct {
	// Live messages, for message.new (in commit order).
	NewMessages []string
	Updated     []MessageChange
	// Chats and the reason they changed (created, info, activity, read, backfill, deleted), first reason wins
	// except that "created" and "deleted" override.
	Chats      map[string]string
	ChatOrder  []string
	Accounts   []string
	seenUpdate map[MessageChange]bool
}

func (c *Changes) Empty() bool {
	return len(c.NewMessages) == 0 && len(c.Updated) == 0 && len(c.Chats) == 0 && len(c.Accounts) == 0
}

func (c *Changes) chat(roomID, reason string) {
	if c.Chats == nil {
		c.Chats = make(map[string]string)
	}
	prev, ok := c.Chats[roomID]
	if !ok {
		c.ChatOrder = append(c.ChatOrder, roomID)
		c.Chats[roomID] = reason
		return
	}
	if reason == "created" || reason == "deleted" || (prev != "created" && prev != "deleted" && reason == "backfill") {
		c.Chats[roomID] = reason
	}
}

func (c *Changes) update(eventID, change string) {
	mc := MessageChange{EventID: eventID, Change: change}
	if c.seenUpdate == nil {
		c.seenUpdate = make(map[MessageChange]bool)
	}
	if c.seenUpdate[mc] {
		return
	}
	c.seenUpdate[mc] = true
	c.Updated = append(c.Updated, mc)
}

func (c *Changes) account(accountID string) {
	for _, a := range c.Accounts {
		if a == accountID {
			return
		}
	}
	c.Accounts = append(c.Accounts, accountID)
}

// Tx is one write transaction plus the changes it made.
type Tx struct {
	ctx context.Context
	tx  *sql.Tx
	Ch  *Changes
}

// Tx runs fn in one IMMEDIATE transaction and returns what it changed. Nothing is recorded if fn fails.
func (d *DB) Tx(ctx context.Context, fn func(tx *Tx) error) (*Changes, error) {
	sqlTx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	tx := &Tx{ctx: ctx, tx: sqlTx, Ch: &Changes{}}
	if err = fn(tx); err != nil {
		_ = sqlTx.Rollback()
		return nil, err
	}
	if err = sqlTx.Commit(); err != nil {
		return nil, err
	}
	return tx.Ch, nil
}

func (tx *Tx) exec(query string, args ...any) (sql.Result, error) {
	return tx.tx.ExecContext(tx.ctx, query, args...)
}

func (tx *Tx) queryRow(query string, args ...any) *sql.Row {
	return tx.tx.QueryRowContext(tx.ctx, query, args...)
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullInt(i int64) any {
	if i == 0 {
		return nil
	}
	return i
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func nowMS() int64 {
	return time.Now().UnixMilli()
}
