package inbox

// SchemaVersion is the inbox.db schema this build writes. A database with a newer version is refused (no downgrade).
const SchemaVersion = 2

// Owner marks inbox.db as Cappy Engine's, so another program's database is never migrated by mistake.
const Owner = "cappy-engine"

const schemaV1 = `
CREATE TABLE IF NOT EXISTS engine_meta (key TEXT PRIMARY KEY, value BLOB NOT NULL);

CREATE TABLE accounts (
	account_id    TEXT PRIMARY KEY,
	network       TEXT NOT NULL,
	login_id      TEXT NOT NULL,
	name          TEXT,
	state         TEXT NOT NULL,
	state_error   TEXT,
	state_message TEXT,
	user_action   TEXT,
	state_ts      INTEGER,
	created_at    INTEGER NOT NULL,
	logged_out_at INTEGER,
	UNIQUE (network, login_id)
);

CREATE TABLE contacts (
	contact_id  INTEGER PRIMARY KEY,
	network     TEXT NOT NULL,
	remote_id   TEXT NOT NULL,
	mxid        TEXT NOT NULL UNIQUE,
	name        TEXT,
	avatar_sha  TEXT,
	identifiers TEXT,
	is_bot      INTEGER NOT NULL DEFAULT 0,
	UNIQUE (network, remote_id)
);

CREATE TABLE chats (
	room_id         TEXT PRIMARY KEY,
	account_id      TEXT REFERENCES accounts (account_id),
	network         TEXT NOT NULL,
	portal_id       TEXT NOT NULL,
	portal_receiver TEXT NOT NULL,
	kind            TEXT NOT NULL,
	title           TEXT,
	topic           TEXT,
	avatar_sha      TEXT,
	dm_contact_id   INTEGER REFERENCES contacts (contact_id),
	parent_room_id  TEXT,
	message_request INTEGER NOT NULL DEFAULT 0,
	last_activity   INTEGER NOT NULL DEFAULT 0,
	last_event_id   TEXT,
	last_read_ts    INTEGER NOT NULL DEFAULT 0,
	unread_count    INTEGER NOT NULL DEFAULT 0,
	marked_unread   INTEGER NOT NULL DEFAULT 0,
	muted_until     INTEGER,
	pinned          INTEGER NOT NULL DEFAULT 0,
	archived        INTEGER NOT NULL DEFAULT 0,
	disappear_type  TEXT,
	disappear_ms    INTEGER,
	deleted         INTEGER NOT NULL DEFAULT 0,
	UNIQUE (network, portal_id, portal_receiver)
);
CREATE INDEX chats_list ON chats (account_id, deleted, archived, last_activity DESC, room_id DESC);
CREATE INDEX chats_list_all ON chats (deleted, archived, last_activity DESC, room_id DESC);

CREATE TABLE members (
	room_id    TEXT NOT NULL,
	contact_id INTEGER NOT NULL DEFAULT 0,
	is_owner   INTEGER NOT NULL DEFAULT 0,
	membership TEXT NOT NULL,
	PRIMARY KEY (room_id, contact_id, is_owner)
);

CREATE TABLE room_state (
	room_id   TEXT NOT NULL,
	type      TEXT NOT NULL,
	state_key TEXT NOT NULL,
	sender    TEXT NOT NULL,
	content   TEXT NOT NULL,
	PRIMARY KEY (room_id, type, state_key)
);

CREATE TABLE messages (
	id                INTEGER PRIMARY KEY,
	event_id          TEXT NOT NULL UNIQUE,
	room_id           TEXT NOT NULL REFERENCES chats (room_id),
	sender_contact_id INTEGER REFERENCES contacts (contact_id),
	from_me           INTEGER NOT NULL,
	ts                INTEGER NOT NULL,
	kind              TEXT NOT NULL,
	msgtype           TEXT,
	body              TEXT,
	formatted_body    TEXT,
	update_json       TEXT,
	reply_to          TEXT,
	thread_root       TEXT,
	remote_id         TEXT,
	part_id           TEXT,
	edited_ts         INTEGER,
	redacted_ts       INTEGER,
	redact_mode       TEXT,
	attachment        TEXT,
	link_previews     TEXT,
	status            TEXT,
	status_error      TEXT,
	client_txn        TEXT UNIQUE,
	source            TEXT NOT NULL
);
CREATE INDEX messages_room_ts ON messages (room_id, ts, id);
CREATE INDEX messages_ts ON messages (ts, id);

CREATE VIRTUAL TABLE messages_fts USING fts5(
	body, content='messages', content_rowid='id', tokenize='unicode61 remove_diacritics 2'
);
CREATE TRIGGER messages_fts_ai AFTER INSERT ON messages WHEN new.body IS NOT NULL BEGIN
	INSERT INTO messages_fts (rowid, body) VALUES (new.id, new.body);
END;
CREATE TRIGGER messages_fts_ad AFTER DELETE ON messages WHEN old.body IS NOT NULL BEGIN
	INSERT INTO messages_fts (messages_fts, rowid, body) VALUES ('delete', old.id, old.body);
END;
CREATE TRIGGER messages_fts_au AFTER UPDATE OF body ON messages BEGIN
	INSERT INTO messages_fts (messages_fts, rowid, body) SELECT 'delete', old.id, old.body WHERE old.body IS NOT NULL;
	INSERT INTO messages_fts (rowid, body) SELECT new.id, new.body WHERE new.body IS NOT NULL;
END;

CREATE TABLE reactions (
	event_id          TEXT PRIMARY KEY,
	target_event_id   TEXT NOT NULL,
	room_id           TEXT NOT NULL,
	sender_contact_id INTEGER,
	from_me           INTEGER NOT NULL,
	key               TEXT NOT NULL,
	ts                INTEGER NOT NULL
);
CREATE INDEX reactions_target ON reactions (target_event_id);

CREATE TABLE receipts (
	room_id    TEXT NOT NULL,
	contact_id INTEGER NOT NULL,
	event_id   TEXT NOT NULL,
	ts         INTEGER NOT NULL,
	PRIMARY KEY (room_id, contact_id)
);

CREATE TABLE raw_events (
	event_id TEXT PRIMARY KEY,
	room_id  TEXT NOT NULL,
	type     TEXT NOT NULL,
	sender   TEXT NOT NULL,
	ts       INTEGER NOT NULL,
	content  TEXT NOT NULL
);
`

// schemaV2 links each stored edit to the message it replaces, so deleting a message can also blank the text its edits
// carried. Content already stored for deleted messages (and their edits) is blanked as part of the upgrade.
const schemaV2 = `
ALTER TABLE raw_events ADD COLUMN replaces TEXT;
UPDATE raw_events SET replaces=(CASE WHEN json_valid(content) THEN
		CASE WHEN json_extract(content, '$."m.relates_to".rel_type')='m.replace'
			THEN json_extract(content, '$."m.relates_to".event_id') END END)
	WHERE type IN ('m.room.message', 'm.sticker');
CREATE INDEX raw_events_replaces ON raw_events (replaces) WHERE replaces IS NOT NULL;
UPDATE raw_events SET content='{}'
	WHERE event_id IN (SELECT event_id FROM messages WHERE redacted_ts IS NOT NULL)
	   OR replaces IN (SELECT event_id FROM messages WHERE redacted_ts IS NOT NULL);
`
