package inbox

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// ---- accounts ----

const accountColumns = `account_id, network, login_id, name, state, state_error, state_message, user_action, state_ts,
	created_at, logged_out_at`

func scanAccount(row interface{ Scan(...any) error }) (*Account, error) {
	var a Account
	var name, stErr, stMsg, action sql.NullString
	var stTS, outAt sql.NullInt64
	if err := row.Scan(&a.Account, &a.Network, &a.LoginID, &name, &a.State, &stErr, &stMsg, &action, &stTS, &a.CreatedAt, &outAt); err != nil {
		return nil, err
	}
	a.Name = strPtr(name)
	a.Error = strPtr(stErr)
	a.Message = strPtr(stMsg)
	a.UserAction = strPtr(action)
	a.StateTs = intPtr(stTS)
	a.LoggedOutAt = intPtr(outAt)
	return &a, nil
}

func (d *DB) ListAccounts(ctx context.Context) ([]*Account, error) {
	rows, err := d.db.QueryContext(ctx, `SELECT `+accountColumns+` FROM accounts ORDER BY created_at, account_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Account
	for rows.Next() {
		a, err := scanAccount(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (d *DB) Account(ctx context.Context, accountID string) (*Account, error) {
	a, err := scanAccount(d.db.QueryRowContext(ctx, `SELECT `+accountColumns+` FROM accounts WHERE account_id=?`, accountID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return a, err
}

func (d *DB) AccountByLogin(ctx context.Context, network, loginID string) (*Account, error) {
	a, err := scanAccount(d.db.QueryRowContext(ctx, `SELECT `+accountColumns+` FROM accounts WHERE network=? AND login_id=?`, network, loginID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return a, err
}

// ---- room state and raw events (for localmx reads) ----

func (d *DB) RoomState(ctx context.Context, roomID, typ string) (map[string]StateEntry, error) {
	rows, err := d.db.QueryContext(ctx, `SELECT state_key, sender, content FROM room_state WHERE room_id=? AND type=?`, roomID, typ)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]StateEntry)
	for rows.Next() {
		var key string
		var e StateEntry
		if err = rows.Scan(&key, &e.Sender, &e.Content); err != nil {
			return nil, err
		}
		out[key] = e
	}
	return out, rows.Err()
}

type RawEvent struct {
	EventID    string
	RoomID     string
	Type       string
	Sender     string
	TS         int64
	Content    string
	RedactedTS int64
}

func (d *DB) RawEvent(ctx context.Context, eventID string) (*RawEvent, error) {
	var e RawEvent
	var redacted sql.NullInt64
	err := d.db.QueryRowContext(ctx, `SELECT r.event_id, r.room_id, r.type, r.sender, r.ts, r.content, m.redacted_ts
		FROM raw_events r LEFT JOIN messages m ON m.event_id=r.event_id WHERE r.event_id=?`, eventID).
		Scan(&e.EventID, &e.RoomID, &e.Type, &e.Sender, &e.TS, &e.Content, &redacted)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}
	e.RedactedTS = redacted.Int64
	return &e, nil
}

// ChatRoom returns the stored row for a chat.
func (d *DB) ChatRow(ctx context.Context, roomID string) (*ChatRow, error) {
	var c ChatRow
	var account sql.NullString
	err := d.db.QueryRowContext(ctx, `SELECT room_id, account_id, network, kind, deleted FROM chats WHERE room_id=?`, roomID).
		Scan(&c.RoomID, &account, &c.Network, &c.Kind, &c.Deleted)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}
	c.AccountID = account.String
	return &c, nil
}

// ---- chats ----

const chatSelect = `SELECT c.room_id, c.account_id, c.network, c.kind, c.title, c.topic, c.avatar_sha, c.message_request,
	c.last_activity, c.unread_count, c.marked_unread, c.muted_until, c.pinned, c.archived, c.deleted,
	dc.contact_id, dc.network, dc.name, dc.avatar_sha, dc.is_bot,
	lm.event_id, lm.from_me, lm.sender_contact_id, lm.ts, lm.kind, lm.msgtype, lm.body, lm.attachment
FROM chats c
LEFT JOIN contacts dc ON dc.contact_id = c.dm_contact_id
LEFT JOIN messages lm ON lm.event_id = c.last_event_id`

func scanChat(row interface{ Scan(...any) error }) (*Chat, error) {
	var c Chat
	var account, title, topic, avatar sql.NullString
	var lastActivity int64
	var muted sql.NullInt64
	var dcID sql.NullInt64
	var dcNetwork, dcName, dcAvatar sql.NullString
	var dcBot sql.NullBool
	var lmEvent, lmKind, lmType, lmBody, lmAttachment sql.NullString
	var lmFromMe sql.NullBool
	var lmSender, lmTS sql.NullInt64
	err := row.Scan(&c.RoomID, &account, &c.Network, &c.Kind, &title, &topic, &avatar, &c.MessageRequest,
		&lastActivity, &c.Unread, &c.MarkedUnread, &muted, &c.Pinned, &c.Archived, &c.Deleted,
		&dcID, &dcNetwork, &dcName, &dcAvatar, &dcBot,
		&lmEvent, &lmFromMe, &lmSender, &lmTS, &lmKind, &lmType, &lmBody, &lmAttachment)
	if err != nil {
		return nil, err
	}
	c.Chat = ChatIDFromRoom(c.RoomID)
	c.Account = strPtr(account)
	c.Title = strPtr(title)
	c.Topic = strPtr(topic)
	c.Avatar = mediaRef(avatar.String, "", 0)
	if lastActivity > 0 {
		c.LastActivity = &lastActivity
	}
	c.MutedUntil = intPtr(muted)
	if dcID.Valid {
		c.DMContact = &Contact{Contact: ContactIDString(dcID.Int64), Network: dcNetwork.String, Name: strPtr(dcName),
			Avatar: mediaRef(dcAvatar.String, "", 0), IsBot: dcBot.Bool}
	}
	if lmEvent.Valid {
		lm := &LastMessage{Message: MessageIDFromEvent(lmEvent.String), FromMe: lmFromMe.Bool, Ts: lmTS.Int64, Kind: lmKind.String,
			Msgtype: strPtr(lmType)}
		if lmSender.Valid {
			s := ContactIDString(lmSender.Int64)
			lm.Sender = &s
		}
		lm.Preview = preview(lmBody.String, lmAttachment.String, lmType.String)
		c.LastMessage = lm
	}
	return &c, nil
}

func preview(body, attachment, msgtype string) string {
	if body == "" && attachment != "" {
		var a StoredAttachment
		if json.Unmarshal([]byte(attachment), &a) == nil {
			if a.Caption != "" {
				body = a.Caption
			} else {
				body = a.Name
			}
		}
	}
	body = strings.Join(strings.Fields(body), " ")
	if utf8.RuneCountInString(body) > 140 {
		runes := []rune(body)
		body = string(runes[:140])
	}
	_ = msgtype
	return body
}

// Chat returns one chat (including deleted and hidden ones).
func (d *DB) Chat(ctx context.Context, roomID string) (*Chat, error) {
	c, err := scanChat(d.db.QueryRowContext(ctx, chatSelect+` WHERE c.room_id=?`, roomID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return c, err
}

func (d *DB) ChatSummary(ctx context.Context, roomID string) (*ChatSummary, error) {
	var s ChatSummary
	var last int64
	err := d.db.QueryRowContext(ctx, `SELECT unread_count, last_activity FROM chats WHERE room_id=?`, roomID).Scan(&s.Unread, &last)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}
	s.Chat = ChatIDFromRoom(roomID)
	if last > 0 {
		s.LastActivity = &last
	}
	return &s, nil
}

type chatCursor struct {
	Activity int64  `json:"a"`
	Room     string `json:"r"`
}

var ErrBadCursor = errors.New("invalid cursor")

func encodeCursor(v any) string {
	data, _ := json.Marshal(v)
	return base64.RawURLEncoding.EncodeToString(data)
}

func decodeCursor(s string, v any) error {
	data, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || json.Unmarshal(data, v) != nil {
		return ErrBadCursor
	}
	return nil
}

type ChatQuery struct {
	AccountID string // "" = all accounts
	Archived  bool
	Cursor    string
	Limit     int
}

// ListChats lists visible chats newest activity first (then chat descending), excluding deleted, system and space
// chats.
func (d *DB) ListChats(ctx context.Context, q ChatQuery) ([]*Chat, string, error) {
	where := []string{"c.deleted=0", "c.kind NOT IN ('system', 'space')", "c.archived=?"}
	args := []any{boolInt(q.Archived)}
	if q.AccountID != "" {
		where = append(where, "c.account_id=?")
		args = append(args, q.AccountID)
	}
	if q.Cursor != "" {
		var cur chatCursor
		if err := decodeCursor(q.Cursor, &cur); err != nil {
			return nil, "", err
		}
		where = append(where, "(c.last_activity < ? OR (c.last_activity = ? AND c.room_id < ?))")
		args = append(args, cur.Activity, cur.Activity, cur.Room)
	}
	args = append(args, q.Limit+1)
	rows, err := d.db.QueryContext(ctx, chatSelect+` WHERE `+strings.Join(where, " AND ")+
		` ORDER BY c.last_activity DESC, c.room_id DESC LIMIT ?`, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var out []*Chat
	for rows.Next() {
		c, err := scanChat(rows)
		if err != nil {
			return nil, "", err
		}
		out = append(out, c)
	}
	if err = rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(out) > q.Limit {
		out = out[:q.Limit]
		last := out[len(out)-1]
		var act int64
		if last.LastActivity != nil {
			act = *last.LastActivity
		}
		next = encodeCursor(chatCursor{Activity: act, Room: last.RoomID})
	}
	return out, next, nil
}

// ---- messages ----

const messageSelect = `SELECT m.id, m.event_id, m.room_id, c.account_id, m.from_me, m.ts, m.edited_ts, m.redacted_ts, m.kind,
	m.msgtype, m.body, m.formatted_body, m.update_json, m.reply_to, m.thread_root, m.attachment, m.status, m.status_error,
	s.contact_id, s.network, s.name, s.avatar_sha, s.is_bot`

const messageFrom = ` FROM messages m JOIN chats c ON c.room_id = m.room_id LEFT JOIN contacts s ON s.contact_id = m.sender_contact_id`

func scanMessage(row interface{ Scan(...any) error }, extra ...any) (*Message, error) {
	var m Message
	var account, msgtype, body, html, update, replyTo, threadRoot, attachment, status, statusErr sql.NullString
	var edited, redacted sql.NullInt64
	var sID sql.NullInt64
	var sNetwork, sName, sAvatar sql.NullString
	var sBot sql.NullBool
	dest := []any{&m.rowID, &m.EventID, &m.RoomID, &account, &m.FromMe, &m.Ts, &edited, &redacted, &m.Kind,
		&msgtype, &body, &html, &update, &replyTo, &threadRoot, &attachment, &status, &statusErr,
		&sID, &sNetwork, &sName, &sAvatar, &sBot}
	if err := row.Scan(append(dest, extra...)...); err != nil {
		return nil, err
	}
	m.Message = MessageIDFromEvent(m.EventID)
	m.Chat = ChatIDFromRoom(m.RoomID)
	m.Account = strPtr(account)
	m.EditedTs = intPtr(edited)
	m.Deleted = redacted.Valid
	m.Msgtype = strPtr(msgtype)
	m.Text = strPtr(body)
	m.HTML = strPtr(html)
	if update.Valid {
		m.Update = json.RawMessage(update.String)
	} else {
		m.Update = json.RawMessage("null")
	}
	if replyTo.Valid {
		r := MessageIDFromEvent(replyTo.String)
		m.ReplyTo = &r
	}
	if threadRoot.Valid {
		r := MessageIDFromEvent(threadRoot.String)
		m.ThreadRoot = &r
	}
	if attachment.Valid {
		var a StoredAttachment
		if json.Unmarshal([]byte(attachment.String), &a) == nil {
			m.Attachment = a.view()
		}
	}
	m.Status = strPtr(status)
	m.StatusError = strPtr(statusErr)
	if sID.Valid {
		m.Sender = &Contact{Contact: ContactIDString(sID.Int64), Network: sNetwork.String, Name: strPtr(sName),
			Avatar: mediaRef(sAvatar.String, "", 0), IsBot: sBot.Bool}
	}
	m.Reactions = []Reaction{}
	return &m, nil
}

func (d *DB) attachReactions(ctx context.Context, msgs []*Message) error {
	if len(msgs) == 0 {
		return nil
	}
	byEvent := make(map[string]*Message, len(msgs))
	args := make([]any, 0, len(msgs))
	for _, m := range msgs {
		byEvent[m.EventID] = m
		args = append(args, m.EventID)
	}
	rows, err := d.db.QueryContext(ctx, `SELECT target_event_id, key, count(*), max(from_me) FROM reactions
		WHERE target_event_id IN (?`+strings.Repeat(",?", len(args)-1)+`) GROUP BY target_event_id, key ORDER BY min(ts)`, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var target string
		var r Reaction
		if err = rows.Scan(&target, &r.Key, &r.Count, &r.FromMe); err != nil {
			return err
		}
		if m := byEvent[target]; m != nil {
			m.Reactions = append(m.Reactions, r)
		}
	}
	return rows.Err()
}

// Message returns one message by event ID.
func (d *DB) Message(ctx context.Context, eventID string) (*Message, error) {
	m, err := scanMessage(d.db.QueryRowContext(ctx, messageSelect+messageFrom+` WHERE m.event_id=?`, eventID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}
	return m, d.attachReactions(ctx, []*Message{m})
}

// MessageByClientTxn finds an outgoing message by the host's transaction ID.
func (d *DB) MessageByClientTxn(ctx context.Context, txn string) (*Message, error) {
	m, err := scanMessage(d.db.QueryRowContext(ctx, messageSelect+messageFrom+` WHERE m.client_txn=?`, txn))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}
	return m, d.attachReactions(ctx, []*Message{m})
}

// ListMessages returns up to limit messages of a chat before the given one (or the newest), oldest first.
func (d *DB) ListMessages(ctx context.Context, roomID, beforeEventID string, limit int) ([]*Message, bool, error) {
	where := "m.room_id=?"
	args := []any{roomID}
	if beforeEventID != "" {
		var ts, rowID int64
		err := d.db.QueryRowContext(ctx, `SELECT ts, id FROM messages WHERE event_id=? AND room_id=?`, beforeEventID, roomID).Scan(&ts, &rowID)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, ErrBadCursor
		} else if err != nil {
			return nil, false, err
		}
		where += " AND (m.ts < ? OR (m.ts = ? AND m.id < ?))"
		args = append(args, ts, ts, rowID)
	}
	args = append(args, limit+1)
	rows, err := d.db.QueryContext(ctx, messageSelect+messageFrom+` WHERE `+where+` ORDER BY m.ts DESC, m.id DESC LIMIT ?`, args...)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	var out []*Message
	for rows.Next() {
		m, err := scanMessage(rows)
		if err != nil {
			return nil, false, err
		}
		out = append(out, m)
	}
	if err = rows.Err(); err != nil {
		return nil, false, err
	}
	hasMore := len(out) > limit
	if hasMore {
		out = out[:limit]
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, hasMore, d.attachReactions(ctx, out)
}

// ---- search ----

type SearchQuery struct {
	Query     string
	AccountID string
	RoomID    string
	Cursor    string
	Limit     int
}

type searchCursor struct {
	TS  int64 `json:"t"`
	Row int64 `json:"i"`
}

// FTSQuery turns free text into a safe FTS5 query: every term is quoted (so FTS operators can't be injected), terms
// are ANDed, and the last one matches as a prefix. It returns "" if the text has no searchable terms.
func FTSQuery(text string) string {
	terms := strings.FieldsFunc(text, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r) && !unicode.Is(unicode.Mn, r)
	})
	if len(terms) > 16 {
		terms = terms[:16]
	}
	for i, t := range terms {
		terms[i] = `"` + t + `"`
	}
	if len(terms) == 0 {
		return ""
	}
	terms[len(terms)-1] += "*"
	return strings.Join(terms, " ")
}

// Search finds messages whose text matches, newest first, with a snippet around the match.
func (d *DB) Search(ctx context.Context, q SearchQuery) ([]SearchResult, string, error) {
	match := FTSQuery(q.Query)
	if match == "" {
		return []SearchResult{}, "", nil
	}
	where := []string{"messages_fts MATCH ?", "m.redacted_ts IS NULL", "c.deleted=0"}
	args := []any{match}
	if q.AccountID != "" {
		where = append(where, "c.account_id=?")
		args = append(args, q.AccountID)
	}
	if q.RoomID != "" {
		where = append(where, "m.room_id=?")
		args = append(args, q.RoomID)
	}
	if q.Cursor != "" {
		var cur searchCursor
		if err := decodeCursor(q.Cursor, &cur); err != nil {
			return nil, "", err
		}
		where = append(where, "(m.ts < ? OR (m.ts = ? AND m.id < ?))")
		args = append(args, cur.TS, cur.TS, cur.Row)
	}
	args = append(args, q.Limit+1)
	query := messageSelect + `, snippet(messages_fts, 0, '[', ']', '…', 12)
		FROM messages_fts JOIN messages m ON m.id = messages_fts.rowid JOIN chats c ON c.room_id = m.room_id
		LEFT JOIN contacts s ON s.contact_id = m.sender_contact_id
		WHERE ` + strings.Join(where, " AND ") + ` ORDER BY m.ts DESC, m.id DESC LIMIT ?`
	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("search: %w", err)
	}
	defer rows.Close()
	var msgs []*Message
	var snippets []string
	for rows.Next() {
		var snippet string
		m, err := scanMessage(rows, &snippet)
		if err != nil {
			return nil, "", err
		}
		msgs = append(msgs, m)
		snippets = append(snippets, snippet)
	}
	if err = rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(msgs) > q.Limit {
		msgs = msgs[:q.Limit]
		last := msgs[len(msgs)-1]
		next = encodeCursor(searchCursor{TS: last.Ts, Row: last.rowID})
	}
	if err = d.attachReactions(ctx, msgs); err != nil {
		return nil, "", err
	}
	out := make([]SearchResult, len(msgs))
	for i, m := range msgs {
		out[i] = SearchResult{Message: *m, Snippet: snippets[i]}
	}
	return out, next, nil
}

// ---- stats ----

func (d *DB) CountAccounts(ctx context.Context, network string) (int, error) {
	var n int
	err := d.db.QueryRowContext(ctx, `SELECT count(*) FROM accounts WHERE network=? AND logged_out_at IS NULL`, network).Scan(&n)
	return n, err
}

func strPtr(s sql.NullString) *string {
	if !s.Valid {
		return nil
	}
	return &s.String
}

func intPtr(i sql.NullInt64) *int64 {
	if !i.Valid {
		return nil
	}
	return &i.Int64
}
