package inbox

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

// ---- accounts ----

// EnsureAccount returns the account for a network login, creating it (state UNKNOWN) if needed.
func (tx *Tx) EnsureAccount(network, loginID string) (string, error) {
	if network == "" || loginID == "" {
		return "", errors.New("account needs a network and a login ID")
	}
	var accountID string
	err := tx.queryRow(`SELECT account_id FROM accounts WHERE network=? AND login_id=?`, network, loginID).Scan(&accountID)
	if err == nil {
		return accountID, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	accountID = NewAccountID()
	if _, err = tx.exec(`INSERT INTO accounts (account_id, network, login_id, state, created_at) VALUES (?, ?, ?, 'UNKNOWN', ?)`,
		accountID, network, loginID, nowMS()); err != nil {
		return "", err
	}
	tx.Ch.account(accountID)
	return accountID, nil
}

type AccountState struct {
	State      string
	Error      string
	Message    string
	UserAction string
	TS         int64
}

func (tx *Tx) SetAccountState(accountID string, st AccountState) error {
	if st.TS == 0 {
		st.TS = nowMS()
	}
	res, err := tx.exec(`UPDATE accounts SET state=?, state_error=?, state_message=?, user_action=?, state_ts=? WHERE account_id=?`,
		st.State, nullStr(st.Error), nullStr(st.Message), nullStr(st.UserAction), st.TS, accountID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	tx.Ch.account(accountID)
	return nil
}

func (tx *Tx) SetAccountName(accountID, name string) error {
	_, err := tx.exec(`UPDATE accounts SET name=? WHERE account_id=? AND name IS NOT ?`, nullStr(name), accountID, nullStr(name))
	return err
}

// SetAccountLoggedIn clears a previous logout (the same login came back).
func (tx *Tx) SetAccountLoggedIn(accountID string) error {
	_, err := tx.exec(`UPDATE accounts SET logged_out_at=NULL WHERE account_id=?`, accountID)
	tx.Ch.account(accountID)
	return err
}

func (tx *Tx) SetAccountLoggedOut(accountID string, ts int64) error {
	_, err := tx.exec(`UPDATE accounts SET logged_out_at=?, state='LOGGED_OUT', state_ts=? WHERE account_id=?`, ts, ts, accountID)
	tx.Ch.account(accountID)
	return err
}

// DeleteAccountData removes an account's chats and everything in them (accounts.logout with purgeLocal).
func (tx *Tx) DeleteAccountData(accountID string) error {
	rooms := `SELECT room_id FROM chats WHERE account_id=?`
	for _, q := range []string{
		`DELETE FROM reactions WHERE room_id IN (` + rooms + `)`,
		`DELETE FROM receipts WHERE room_id IN (` + rooms + `)`,
		`DELETE FROM raw_events WHERE room_id IN (` + rooms + `)`,
		`DELETE FROM room_state WHERE room_id IN (` + rooms + `)`,
		`DELETE FROM members WHERE room_id IN (` + rooms + `)`,
		`DELETE FROM messages WHERE room_id IN (` + rooms + `)`,
		`DELETE FROM chats WHERE account_id=?`,
	} {
		if _, err := tx.exec(q, accountID); err != nil {
			return err
		}
	}
	return nil
}

// ---- contacts ----

// EnsureContact returns the contact row for a ghost, creating it if needed.
func (tx *Tx) EnsureContact(network, remoteID, mxid string) (int64, error) {
	var id int64
	err := tx.queryRow(`SELECT contact_id FROM contacts WHERE mxid=?`, mxid).Scan(&id)
	if err == nil {
		return id, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	err = tx.queryRow(`INSERT INTO contacts (network, remote_id, mxid) VALUES (?, ?, ?)
		ON CONFLICT (network, remote_id) DO UPDATE SET mxid=excluded.mxid RETURNING contact_id`, network, remoteID, mxid).Scan(&id)
	return id, err
}

func (tx *Tx) SetContactName(contactID int64, name string) error {
	_, err := tx.exec(`UPDATE contacts SET name=? WHERE contact_id=?`, nullStr(name), contactID)
	return err
}

func (tx *Tx) SetContactAvatar(contactID int64, sha string) error {
	_, err := tx.exec(`UPDATE contacts SET avatar_sha=? WHERE contact_id=?`, nullStr(sha), contactID)
	return err
}

func (tx *Tx) SetContactExtra(contactID int64, identifiers []string, isBot bool) error {
	var ids any
	if len(identifiers) > 0 {
		data, _ := json.Marshal(identifiers)
		ids = string(data)
	}
	_, err := tx.exec(`UPDATE contacts SET identifiers=?, is_bot=? WHERE contact_id=?`, ids, boolInt(isBot), contactID)
	return err
}

// ---- chats ----

type NewChat struct {
	RoomID         string
	AccountID      string
	Network        string
	PortalID       string
	PortalReceiver string
	Kind           string // dm | group_dm | group | space | system
	MessageRequest bool
}

// UpsertChat creates a chat or revives a deleted one.
func (tx *Tx) UpsertChat(c NewChat) error {
	_, err := tx.exec(`INSERT INTO chats (room_id, account_id, network, portal_id, portal_receiver, kind, message_request)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (room_id) DO UPDATE SET deleted=0, kind=excluded.kind, account_id=excluded.account_id,
			message_request=excluded.message_request`,
		c.RoomID, nullStr(c.AccountID), c.Network, c.PortalID, c.PortalReceiver, c.Kind, boolInt(c.MessageRequest))
	if err != nil {
		return err
	}
	tx.Ch.chat(c.RoomID, "created")
	return nil
}

type ChatRow struct {
	RoomID    string
	AccountID string
	Network   string
	Kind      string
	Deleted   bool
}

func (tx *Tx) ChatByRoom(roomID string) (*ChatRow, error) {
	var c ChatRow
	var account sql.NullString
	err := tx.queryRow(`SELECT room_id, account_id, network, kind, deleted FROM chats WHERE room_id=?`, roomID).
		Scan(&c.RoomID, &account, &c.Network, &c.Kind, &c.Deleted)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}
	c.AccountID = account.String
	return &c, nil
}

var chatColumns = map[string]bool{
	"title": true, "topic": true, "avatar_sha": true, "dm_contact_id": true, "parent_room_id": true,
	"message_request": true, "marked_unread": true, "muted_until": true, "pinned": true, "archived": true,
	"disappear_type": true, "disappear_ms": true, "deleted": true,
}

// SetChatField sets one derived chat column and records the change with reason.
func (tx *Tx) SetChatField(roomID, column string, value any, reason string) error {
	if !chatColumns[column] {
		return fmt.Errorf("unknown chat column %q", column)
	}
	res, err := tx.exec(`UPDATE chats SET `+column+`=? WHERE room_id=? AND `+column+` IS NOT ?`, value, roomID, value)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n > 0 {
		tx.Ch.chat(roomID, reason)
	}
	return nil
}

// SetLastRead marks everything up to ts as read (it never moves backwards) and clears "marked unread".
func (tx *Tx) SetLastRead(roomID string, ts int64) error {
	if _, err := tx.exec(`UPDATE chats SET last_read_ts=max(last_read_ts, ?), marked_unread=0 WHERE room_id=?`, ts, roomID); err != nil {
		return err
	}
	if err := tx.recomputeUnread(roomID); err != nil {
		return err
	}
	tx.Ch.chat(roomID, "read")
	return nil
}

func (tx *Tx) recomputeUnread(roomID string) error {
	_, err := tx.exec(`UPDATE chats SET unread_count=(
			SELECT count(*) FROM messages m WHERE m.room_id=chats.room_id AND m.from_me=0 AND m.kind='message'
				AND m.redacted_ts IS NULL AND m.ts > chats.last_read_ts)
		WHERE room_id=?`, roomID)
	return err
}

// ---- room state and membership ----

type StateEntry struct {
	Sender  string
	Content string
}

func (tx *Tx) SetRoomState(roomID, typ, stateKey, sender, content string) error {
	_, err := tx.exec(`INSERT INTO room_state (room_id, type, state_key, sender, content) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (room_id, type, state_key) DO UPDATE SET sender=excluded.sender, content=excluded.content`,
		roomID, typ, stateKey, sender, content)
	return err
}

// SetMembership records a member's membership and returns the previous one ("" if none). contactID 0 with isOwner
// means the owner.
func (tx *Tx) SetMembership(roomID string, contactID int64, isOwner bool, membership string) (string, error) {
	var prev string
	err := tx.queryRow(`SELECT membership FROM members WHERE room_id=? AND contact_id=? AND is_owner=?`, roomID, contactID, boolInt(isOwner)).Scan(&prev)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	_, err = tx.exec(`INSERT INTO members (room_id, contact_id, is_owner, membership) VALUES (?, ?, ?, ?)
		ON CONFLICT (room_id, contact_id, is_owner) DO UPDATE SET membership=excluded.membership`,
		roomID, contactID, boolInt(isOwner), membership)
	return prev, err
}

func (tx *Tx) ContactName(contactID int64) (string, error) {
	var name sql.NullString
	err := tx.queryRow(`SELECT name FROM contacts WHERE contact_id=?`, contactID).Scan(&name)
	return name.String, err
}

// ---- events ----

// PutRawEvent stores an event's content for GetEvent. replaces is the event ID an edit replaces ("" otherwise). An
// edit of a message that was already deleted is stored without its content.
func (tx *Tx) PutRawEvent(eventID, roomID, typ, sender string, ts int64, content, replaces string) error {
	if replaces != "" {
		var redacted int
		if err := tx.queryRow(`SELECT count(*) FROM messages WHERE event_id=? AND redacted_ts IS NOT NULL`, replaces).Scan(&redacted); err != nil {
			return err
		}
		if redacted > 0 {
			content = blankContent
		}
	}
	_, err := tx.exec(`INSERT INTO raw_events (event_id, room_id, type, sender, ts, content, replaces) VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (event_id) DO NOTHING`, eventID, roomID, typ, sender, ts, content, nullStr(replaces))
	return err
}

// blankContent replaces the stored content of a deleted event.
const blankContent = "{}"

// Sources of a message row.
const (
	SourceLive     = "live"
	SourceBackfill = "backfill"
	SourceLocal    = "local"
)

type NewMessage struct {
	EventID         string
	RoomID          string
	SenderContactID int64
	FromMe          bool
	TS              int64
	Kind            string // message | notice | update
	MsgType         string
	Body            string
	FormattedBody   string
	UpdateJSON      string
	ReplyTo         string
	ThreadRoot      string
	RemoteID        string
	PartID          string
	Attachment      *StoredAttachment
	LinkPreviews    string
	Status          string
	ClientTxn       string
	Source          string
}

// PutMessage stores a message. It is idempotent: a message whose event ID is already stored is left as it is and
// inserted is false.
func (tx *Tx) PutMessage(m NewMessage) (inserted bool, err error) {
	var attachment any
	if m.Attachment != nil {
		data, err := json.Marshal(m.Attachment)
		if err != nil {
			return false, err
		}
		attachment = string(data)
	}
	var body any
	if m.Body != "" || m.Kind == "message" && m.Attachment == nil {
		body = m.Body
	}
	res, err := tx.exec(`INSERT INTO messages (event_id, room_id, sender_contact_id, from_me, ts, kind, msgtype, body,
			formatted_body, update_json, reply_to, thread_root, remote_id, part_id, attachment, link_previews, status,
			client_txn, source)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (event_id) DO NOTHING`,
		m.EventID, m.RoomID, nullInt(m.SenderContactID), boolInt(m.FromMe), m.TS, m.Kind, nullStr(m.MsgType), body,
		nullStr(m.FormattedBody), nullStr(m.UpdateJSON), nullStr(m.ReplyTo), nullStr(m.ThreadRoot), nullStr(m.RemoteID),
		nullStr(m.PartID), attachment, nullStr(m.LinkPreviews), nullStr(m.Status), nullStr(m.ClientTxn), m.Source)
	if err != nil {
		return false, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return false, nil
	}
	if _, err = tx.exec(`UPDATE chats SET last_activity=?, last_event_id=? WHERE room_id=? AND last_activity <= ?`,
		m.TS, m.EventID, m.RoomID, m.TS); err != nil {
		return false, err
	}
	if !m.FromMe && m.Kind == "message" {
		if err = tx.recomputeUnread(m.RoomID); err != nil {
			return false, err
		}
	}
	switch m.Source {
	case SourceLive:
		tx.Ch.NewMessages = append(tx.Ch.NewMessages, m.EventID)
		tx.Ch.chat(m.RoomID, "activity")
	case SourceBackfill:
		tx.Ch.chat(m.RoomID, "backfill")
	default:
		tx.Ch.chat(m.RoomID, "activity")
	}
	return true, nil
}

// EditMessage replaces a message's text (an edit from the network).
func (tx *Tx) EditMessage(eventID, body, formatted string, ts int64) error {
	res, err := tx.exec(`UPDATE messages SET body=?, formatted_body=?, edited_ts=? WHERE event_id=? AND redacted_ts IS NULL`,
		body, nullStr(formatted), ts, eventID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	tx.Ch.update(eventID, "edit")
	return nil
}

// Redact removes a reaction, or blanks a message (which also drops it from search). mode is unsend, disappear or
// viewed. A target that isn't stored is ErrNotFound.
func (tx *Tx) Redact(eventID string, ts int64, mode string) error {
	var target string
	err := tx.queryRow(`DELETE FROM reactions WHERE event_id=? RETURNING target_event_id`, eventID).Scan(&target)
	if err == nil {
		if _, err = tx.exec(`DELETE FROM raw_events WHERE event_id=?`, eventID); err != nil {
			return err
		}
		tx.Ch.update(target, "reactions")
		return nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var roomID string
	err = tx.queryRow(`UPDATE messages SET body=NULL, formatted_body=NULL, attachment=NULL, link_previews=NULL,
			redacted_ts=?, redact_mode=? WHERE event_id=? AND redacted_ts IS NULL RETURNING room_id`, ts, mode, eventID).Scan(&roomID)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	// The stored original event and its edits still hold the text; keep only the rows (GetEvent reports them as
	// redacted).
	if _, err = tx.exec(`UPDATE raw_events SET content=? WHERE event_id=? OR replaces=?`, blankContent, eventID, eventID); err != nil {
		return err
	}
	if err = tx.recomputeUnread(roomID); err != nil {
		return err
	}
	tx.Ch.update(eventID, "delete")
	tx.Ch.chat(roomID, "info")
	return nil
}

type NewReaction struct {
	EventID         string
	TargetEventID   string
	SenderContactID int64
	FromMe          bool
	Key             string
	TS              int64
}

// PutReaction stores a reaction. A reaction to a message that isn't stored is ErrNotFound.
func (tx *Tx) PutReaction(r NewReaction) error {
	var roomID string
	err := tx.queryRow(`SELECT room_id FROM messages WHERE event_id=?`, r.TargetEventID).Scan(&roomID)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	res, err := tx.exec(`INSERT INTO reactions (event_id, target_event_id, room_id, sender_contact_id, from_me, key, ts)
		VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (event_id) DO NOTHING`,
		r.EventID, r.TargetEventID, roomID, nullInt(r.SenderContactID), boolInt(r.FromMe), r.Key, r.TS)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n > 0 {
		tx.Ch.update(r.TargetEventID, "reactions")
	}
	return nil
}

var statusRank = map[string]int{"pending": 0, "failed": 1, "sent": 2, "delivered": 3, "read": 4}

// SetMessageStatus updates an outgoing message's delivery status. It never moves a status backwards (eg a late "sent"
// after "read"). A message that isn't stored, or isn't the owner's, is ErrNotFound.
func (tx *Tx) SetMessageStatus(eventID, status, statusErr string) error {
	var current sql.NullString
	err := tx.queryRow(`SELECT status FROM messages WHERE event_id=? AND from_me=1`, eventID).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	if current.Valid && statusRank[status] <= statusRank[current.String] && !(status == "failed" && current.String == "pending") {
		return nil
	}
	if _, err = tx.exec(`UPDATE messages SET status=?, status_error=? WHERE event_id=?`, status, nullStr(statusErr), eventID); err != nil {
		return err
	}
	tx.Ch.update(eventID, "status")
	return nil
}

// MessageRef is where a stored message is.
type MessageRef struct {
	RoomID string
	TS     int64
	FromMe bool
}

func (tx *Tx) MessageRef(eventID string) (*MessageRef, error) {
	var ref MessageRef
	err := tx.queryRow(`SELECT room_id, ts, from_me FROM messages WHERE event_id=?`, eventID).Scan(&ref.RoomID, &ref.TS, &ref.FromMe)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &ref, err
}

// SetReceipt records another person's read receipt; the owner's sent messages up to it become "read".
func (tx *Tx) SetReceipt(roomID string, contactID int64, eventID string, targetTS, ts int64) error {
	if _, err := tx.exec(`INSERT INTO receipts (room_id, contact_id, event_id, ts) VALUES (?, ?, ?, ?)
		ON CONFLICT (room_id, contact_id) DO UPDATE SET event_id=excluded.event_id, ts=excluded.ts`,
		roomID, contactID, eventID, ts); err != nil {
		return err
	}
	rows, err := tx.tx.QueryContext(tx.ctx, `UPDATE messages SET status='read' WHERE room_id=? AND from_me=1 AND ts<=?
		AND status IN ('sent', 'delivered') RETURNING event_id`, roomID, targetTS)
	if err != nil {
		return err
	}
	defer rows.Close()
	for n := 0; rows.Next(); n++ {
		var updated string
		if err = rows.Scan(&updated); err != nil {
			return err
		}
		if n < 50 {
			tx.Ch.update(updated, "receipts")
		}
	}
	return rows.Err()
}

// FailPending marks outgoing messages left "pending" by an earlier process: "sent" if sent(eventID) says the bridge
// stored it, otherwise failed with "interrupted".
func (tx *Tx) FailPending(sent func(eventID string) bool) (int, error) {
	rows, err := tx.tx.QueryContext(tx.ctx, `SELECT event_id FROM messages WHERE from_me=1 AND source='local' AND status='pending'`)
	if err != nil {
		return 0, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	for _, id := range ids {
		status, statusErr := "failed", "interrupted"
		if sent(id) {
			status, statusErr = "sent", ""
		}
		if _, err = tx.exec(`UPDATE messages SET status=?, status_error=? WHERE event_id=?`, status, nullStr(statusErr), id); err != nil {
			return 0, err
		}
	}
	return len(ids), nil
}
