package inbox

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func openTest(t *testing.T) *DB {
	t.Helper()
	db, err := Open(context.Background(), filepath.Join(t.TempDir(), "inbox.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func mustTx(t *testing.T, db *DB, fn func(tx *Tx) error) *Changes {
	t.Helper()
	ch, err := db.Tx(context.Background(), fn)
	if err != nil {
		t.Fatal(err)
	}
	return ch
}

const (
	roomA = "!AAAAAAAAAAAAAAAAAAAAAAAAAA:cappy.local"
	roomB = "!BBBBBBBBBBBBBBBBBBBBBBBBBB:cappy.local"
	roomC = "!CCCCCCCCCCCCCCCCCCCCCCCCCC:cappy.local"
)

// seed makes one account with three chats and a contact.
func seed(t *testing.T, db *DB) (accountID string, alice int64) {
	t.Helper()
	mustTx(t, db, func(tx *Tx) error {
		var err error
		if accountID, err = tx.EnsureAccount("whatsapp", "login-1"); err != nil {
			return err
		}
		if alice, err = tx.EnsureContact("whatsapp", "alice", "@whatsapp_alice:cappy.local"); err != nil {
			return err
		}
		if err = tx.SetContactName(alice, "Alice"); err != nil {
			return err
		}
		for i, room := range []string{roomA, roomB, roomC} {
			kind := "dm"
			if i == 2 {
				kind = "group"
			}
			if err = tx.UpsertChat(NewChat{RoomID: room, AccountID: accountID, Network: "whatsapp", PortalID: room, PortalReceiver: "login-1", Kind: kind}); err != nil {
				return err
			}
		}
		return tx.SetChatField(roomA, "dm_contact_id", alice, "info")
	})
	return
}

func put(t *testing.T, db *DB, m NewMessage) *Changes {
	t.Helper()
	return mustTx(t, db, func(tx *Tx) error {
		_, err := tx.PutMessage(m)
		return err
	})
}

func TestOpenTwiceKeepsInstallKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.db")
	db, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	key := append([]byte(nil), db.InstallKey()...)
	_ = db.Close()
	db, err = Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if string(key) != string(db.InstallKey()) || len(key) != 32 {
		t.Fatal("install key changed between opens")
	}
}

func TestInsertListAndIdempotence(t *testing.T) {
	db := openTest(t)
	ctx := context.Background()
	_, alice := seed(t, db)
	ch := put(t, db, NewMessage{EventID: "$m1", RoomID: roomA, SenderContactID: alice, TS: 1000, Kind: "message", MsgType: "m.text", Body: "synthetic hello", Source: SourceLive})
	if len(ch.NewMessages) != 1 || ch.Chats[roomA] != "activity" {
		t.Fatalf("unexpected changes: %+v", ch)
	}
	// The same event again (eg live and then backfill) is stored once and is not "new".
	ch = put(t, db, NewMessage{EventID: "$m1", RoomID: roomA, SenderContactID: alice, TS: 1000, Kind: "message", MsgType: "m.text", Body: "synthetic hello", Source: SourceBackfill})
	if len(ch.NewMessages) != 0 || len(ch.Chats) != 0 {
		t.Fatalf("duplicate insert recorded changes: %+v", ch)
	}
	put(t, db, NewMessage{EventID: "$m2", RoomID: roomA, FromMe: true, TS: 2000, Kind: "message", MsgType: "m.text", Body: "synthetic reply", Source: SourceLive})
	put(t, db, NewMessage{EventID: "$m3", RoomID: roomA, SenderContactID: alice, TS: 3000, Kind: "message", MsgType: "m.text", Body: "third", Source: SourceBackfill})

	msgs, more, err := db.ListMessages(ctx, roomA, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 3 || more || msgs[0].EventID != "$m1" || msgs[2].EventID != "$m3" {
		t.Fatalf("expected 3 messages oldest first, got %d (more=%v)", len(msgs), more)
	}
	if msgs[0].Sender == nil || *msgs[0].Sender.Name != "Alice" || msgs[1].FromMe != true || msgs[1].Sender != nil {
		t.Fatal("sender mapping is wrong")
	}
	if msgs[0].Message != "msg_m1" || msgs[0].Chat != ChatIDFromRoom(roomA) {
		t.Fatal("host IDs are wrong")
	}
	// Paging backwards.
	page, more, err := db.ListMessages(ctx, roomA, "$m3", 1)
	if err != nil || len(page) != 1 || page[0].EventID != "$m2" || !more {
		t.Fatalf("paging failed: %v %d %v", err, len(page), more)
	}
	if _, _, err = db.ListMessages(ctx, roomA, "$nope", 1); !errors.Is(err, ErrBadCursor) {
		t.Fatalf("expected ErrBadCursor, got %v", err)
	}
	chat, err := db.Chat(ctx, roomA)
	if err != nil {
		t.Fatal(err)
	}
	if chat.Unread != 2 || chat.LastMessage == nil || chat.LastMessage.Message != "msg_m3" || chat.DMContact == nil {
		t.Fatalf("chat summary wrong: unread=%d last=%+v", chat.Unread, chat.LastMessage)
	}
	// Reading up to the second message leaves one unread.
	mustTx(t, db, func(tx *Tx) error { return tx.SetLastRead(roomA, 2000) })
	if chat, _ = db.Chat(ctx, roomA); chat.Unread != 1 {
		t.Fatalf("expected 1 unread after read, got %d", chat.Unread)
	}
}

func TestChatOrderingAndCursor(t *testing.T) {
	db := openTest(t)
	ctx := context.Background()
	accountID, alice := seed(t, db)
	put(t, db, NewMessage{EventID: "$a", RoomID: roomA, SenderContactID: alice, TS: 100, Kind: "message", Body: "a", Source: SourceLive})
	put(t, db, NewMessage{EventID: "$b", RoomID: roomB, SenderContactID: alice, TS: 300, Kind: "message", Body: "b", Source: SourceLive})
	put(t, db, NewMessage{EventID: "$c", RoomID: roomC, SenderContactID: alice, TS: 200, Kind: "message", Body: "c", Source: SourceLive})
	// An older backfilled message does not move the chat's activity backwards.
	put(t, db, NewMessage{EventID: "$old", RoomID: roomB, SenderContactID: alice, TS: 50, Kind: "message", Body: "old", Source: SourceBackfill})

	chats, next, err := db.ListChats(ctx, ChatQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(chats) != 3 || next != "" {
		t.Fatalf("expected 3 chats and no cursor, got %d %q", len(chats), next)
	}
	if chats[0].RoomID != roomB || chats[1].RoomID != roomC || chats[2].RoomID != roomA {
		t.Fatalf("wrong order: %s %s %s", chats[0].Chat, chats[1].Chat, chats[2].Chat)
	}
	if chats[0].LastMessage.Message != "msg_b" {
		t.Fatal("backfilled older message replaced the last message")
	}
	// Paging two at a time.
	page1, cur, err := db.ListChats(ctx, ChatQuery{AccountID: accountID, Limit: 2})
	if err != nil || len(page1) != 2 || cur == "" {
		t.Fatalf("page 1: %v %d %q", err, len(page1), cur)
	}
	page2, cur2, err := db.ListChats(ctx, ChatQuery{AccountID: accountID, Limit: 2, Cursor: cur})
	if err != nil || len(page2) != 1 || cur2 != "" || page2[0].RoomID != roomA {
		t.Fatalf("page 2: %v %d %q", err, len(page2), cur2)
	}
	if _, _, err = db.ListChats(ctx, ChatQuery{Limit: 2, Cursor: "!!!"}); !errors.Is(err, ErrBadCursor) {
		t.Fatalf("expected ErrBadCursor, got %v", err)
	}
	// Archived and deleted chats are hidden from the default list.
	mustTx(t, db, func(tx *Tx) error {
		if err := tx.SetChatField(roomB, "archived", 1, "info"); err != nil {
			return err
		}
		return tx.SetChatField(roomC, "deleted", 1, "deleted")
	})
	chats, _, _ = db.ListChats(ctx, ChatQuery{Limit: 10})
	if len(chats) != 1 || chats[0].RoomID != roomA {
		t.Fatalf("expected only chat A, got %d", len(chats))
	}
	archived, _, _ := db.ListChats(ctx, ChatQuery{Limit: 10, Archived: true})
	if len(archived) != 1 || archived[0].RoomID != roomB {
		t.Fatal("archived list wrong")
	}
	if other, _, _ := db.ListChats(ctx, ChatQuery{AccountID: "acc_OTHER", Limit: 10}); len(other) != 0 {
		t.Fatal("account filter failed")
	}
}

func TestSearch(t *testing.T) {
	db := openTest(t)
	ctx := context.Background()
	_, alice := seed(t, db)
	put(t, db, NewMessage{EventID: "$s1", RoomID: roomA, SenderContactID: alice, TS: 100, Kind: "message", Body: "Lunch at the café tomorrow?", Source: SourceLive})
	put(t, db, NewMessage{EventID: "$s2", RoomID: roomB, SenderContactID: alice, TS: 200, Kind: "message", Body: "cafeteria is closed", Source: SourceLive})
	put(t, db, NewMessage{EventID: "$s3", RoomID: roomB, SenderContactID: alice, TS: 300, Kind: "message", Body: "nothing relevant", Source: SourceLive})

	// Diacritics are folded and the last term is a prefix: "cafe" matches "café" and "cafeteria".
	res, next, err := db.Search(ctx, SearchQuery{Query: "cafe", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 2 || next != "" || res[0].Message.EventID != "$s2" || res[1].Message.EventID != "$s1" {
		t.Fatalf("expected 2 results newest first, got %d", len(res))
	}
	if res[1].Snippet == "" || !containsBracket(res[1].Snippet) {
		t.Fatalf("snippet missing match markers: %q", res[1].Snippet)
	}
	// Chat filter and paging.
	if res, _, _ = db.Search(ctx, SearchQuery{Query: "cafe", RoomID: roomA, Limit: 10}); len(res) != 1 {
		t.Fatal("chat filter failed")
	}
	page, cur, err := db.Search(ctx, SearchQuery{Query: "cafe", Limit: 1})
	if err != nil || len(page) != 1 || cur == "" {
		t.Fatalf("search page 1: %v", err)
	}
	page, cur, err = db.Search(ctx, SearchQuery{Query: "cafe", Limit: 1, Cursor: cur})
	if err != nil || len(page) != 1 || cur != "" || page[0].Message.EventID != "$s1" {
		t.Fatalf("search page 2: %v", err)
	}
	// FTS operators in user input are inert.
	for _, q := range []string{`cafe OR nothing`, `"unbalanced`, `body:cafe`, `NEAR(cafe nothing)`, `*`, `-`, `)(`} {
		if _, _, err = db.Search(ctx, SearchQuery{Query: q, Limit: 10}); err != nil {
			t.Fatalf("query %q failed: %v", q, err)
		}
	}
	if res, _, _ = db.Search(ctx, SearchQuery{Query: "cafe OR nothing", Limit: 10}); len(res) != 0 {
		t.Fatal("OR was interpreted as an operator")
	}
	// Edits are searchable, redactions are removed from the index.
	mustTx(t, db, func(tx *Tx) error { return tx.EditMessage("$s3", "now about espresso", "", 400) })
	if res, _, _ = db.Search(ctx, SearchQuery{Query: "espresso", Limit: 10}); len(res) != 1 {
		t.Fatal("edited text not searchable")
	}
	ch := mustTx(t, db, func(tx *Tx) error { return tx.Redact("$s1", 500, "unsend") })
	if len(ch.Updated) != 1 || ch.Updated[0].Change != "delete" {
		t.Fatalf("redaction change not recorded: %+v", ch.Updated)
	}
	if res, _, _ = db.Search(ctx, SearchQuery{Query: "lunch", Limit: 10}); len(res) != 0 {
		t.Fatal("redacted message still searchable")
	}
	m, _ := db.Message(ctx, "$s1")
	if !m.Deleted || m.Text != nil {
		t.Fatal("redacted message still has text")
	}
}

func containsBracket(s string) bool {
	for _, r := range s {
		if r == '[' {
			return true
		}
	}
	return false
}

func TestReactionsStatusesAndReceipts(t *testing.T) {
	db := openTest(t)
	ctx := context.Background()
	_, alice := seed(t, db)
	put(t, db, NewMessage{EventID: "$out", RoomID: roomA, FromMe: true, TS: 100, Kind: "message", Body: "x", Status: "pending", ClientTxn: "txn-1", Source: SourceLocal})
	mustTx(t, db, func(tx *Tx) error {
		if err := tx.PutReaction(NewReaction{EventID: "$r1", TargetEventID: "$out", SenderContactID: alice, Key: "👍", TS: 110}); err != nil {
			return err
		}
		return tx.PutReaction(NewReaction{EventID: "$r1", TargetEventID: "$out", SenderContactID: alice, Key: "👍", TS: 110})
	})
	if err := func() error {
		_, err := db.Tx(ctx, func(tx *Tx) error {
			return tx.PutReaction(NewReaction{EventID: "$r2", TargetEventID: "$missing", Key: "x", TS: 1})
		})
		return err
	}(); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for a reaction to a missing message, got %v", err)
	}
	m, _ := db.MessageByClientTxn(ctx, "txn-1")
	if len(m.Reactions) != 1 || m.Reactions[0].Count != 1 || *m.Status != "pending" {
		t.Fatalf("reactions/status wrong: %+v", m.Reactions)
	}
	mustTx(t, db, func(tx *Tx) error { return tx.SetMessageStatus("$out", "sent", "") })
	mustTx(t, db, func(tx *Tx) error {
		return tx.SetReceipt(roomA, alice, "$out", 100, 150)
	})
	if m, _ = db.Message(ctx, "$out"); *m.Status != "read" {
		t.Fatalf("expected read, got %s", *m.Status)
	}
	// A late "sent" never moves the status backwards.
	mustTx(t, db, func(tx *Tx) error { return tx.SetMessageStatus("$out", "sent", "") })
	if m, _ = db.Message(ctx, "$out"); *m.Status != "read" {
		t.Fatal("status moved backwards")
	}
	// Removing the reaction.
	ch := mustTx(t, db, func(tx *Tx) error { return tx.Redact("$r1", 200, "unsend") })
	if len(ch.Updated) != 1 || ch.Updated[0].Change != "reactions" {
		t.Fatalf("reaction removal not recorded: %+v", ch.Updated)
	}
	if m, _ = db.Message(ctx, "$out"); len(m.Reactions) != 0 {
		t.Fatal("reaction not removed")
	}
}

func TestIDMaps(t *testing.T) {
	if got := RoomFromChatID(ChatIDFromRoom(roomA)); got != roomA {
		t.Fatalf("room round trip: %q", got)
	}
	if got := EventFromMessageID(MessageIDFromEvent("$mABC")); got != "$mABC" {
		t.Fatalf("event round trip: %q", got)
	}
	for _, bad := range []string{"", "cht_", "cht_a b", "cht_x:y", "msg_", "room"} {
		if RoomFromChatID(bad) != "" || EventFromMessageID(bad) != "" {
			t.Fatalf("accepted malformed ID %q", bad)
		}
	}
	if FTSQuery(`  `) != "" || FTSQuery(`a "b`) != `"a" "b"*` {
		t.Fatalf("FTSQuery: %q", FTSQuery(`a "b`))
	}
}
