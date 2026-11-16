package localmx

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"cappy.local/engine/internal/inbox"
	"cappy.local/engine/internal/media"
)

func newTestConnector(t *testing.T, network string) (*Connector, *inbox.DB) {
	t.Helper()
	dir := t.TempDir()
	ib, err := inbox.Open(context.Background(), filepath.Join(dir, "inbox.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ib.Close() })
	ms, err := media.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	return New(network, ib, ms, nil, zerolog.Nop()), ib
}

func TestIDs(t *testing.T) {
	wa, _ := newTestConnector(t, "whatsapp")
	tg, _ := newTestConnector(t, "telegram")
	key := networkid.PortalKey{ID: "15550000001@s.whatsapp.net", Receiver: "15550000002"}
	room := wa.RoomIDFor(key)
	if room != wa.RoomIDFor(key) {
		t.Fatal("room IDs are not deterministic")
	}
	if strings.Contains(string(room), "1555") || !strings.HasPrefix(string(room), "!") || len(room) != 1+26+len(":cappy.local") {
		t.Fatalf("room ID leaks the portal ID or has the wrong shape: %s", room)
	}
	if inbox.RoomFromChatID(inbox.ChatIDFromRoom(string(room))) != string(room) {
		t.Fatal("room ID does not map to a chat ID and back")
	}
	if tg.RoomIDFor(key) == room {
		t.Fatal("different networks/installs produced the same room ID")
	}
	msgID := wa.MessageEventID(key, "MSG1", "")
	if msgID != wa.MessageEventID(key, "MSG1", "") || msgID == wa.MessageEventID(key, "MSG1", "1") || !strings.HasPrefix(string(msgID), "$m") {
		t.Fatalf("bad message event ID %s", msgID)
	}
	if inbox.EventFromMessageID(inbox.MessageIDFromEvent(string(msgID))) != string(msgID) {
		t.Fatal("event ID does not map to a message ID and back")
	}
	for _, remote := range []networkid.UserID{"15550000003", "lid:12345@lid", "Ünïcødé user", "a:b/c"} {
		mxid := wa.FormatGhostMXID(remote)
		back, ok := wa.ParseGhostMXID(mxid)
		if !ok || back != remote {
			t.Fatalf("ghost round trip failed for %q via %s", remote, mxid)
		}
		if _, ok = tg.ParseGhostMXID(mxid); ok {
			t.Fatal("telegram parsed a whatsapp ghost")
		}
	}
	if _, ok := wa.ParseGhostMXID(OwnerMXID); ok {
		t.Fatal("owner parsed as a ghost")
	}
	if BotMXID("whatsapp") == wa.FormatGhostMXID("bot") {
		t.Fatal("bot and ghost IDs collide")
	}
}

// TestRoomAndTimeline drives the connector directly: room creation from bridgev2's create request, a ghost's message,
// GetEvent for it (quoted replies), an owner reaction and a redaction.
func TestRoomAndTimeline(t *testing.T) {
	c, ib := newTestConnector(t, "whatsapp")
	ctx := context.Background()
	key := networkid.PortalKey{ID: "chat1", Receiver: "login1"}
	roomID := c.RoomIDFor(key)
	bridgeInfo := &event.BridgeEventContent{Channel: event.BridgeInfoSection{ID: "chat1", Receiver: "login1"}, BeeperRoomTypeV2: "dm"}
	stateKey := "cappy-whatsapp"
	created, err := c.BotIntent().CreateRoom(ctx, &mautrix.ReqCreateRoom{
		BeeperLocalRoomID: roomID,
		Name:              "Synthetic chat",
		IsDirect:          true,
		InitialState:      []*event.Event{{Type: event.StateBridge, StateKey: &stateKey, Content: event.Content{Parsed: bridgeInfo}}},
	})
	if err != nil || created != roomID {
		t.Fatalf("CreateRoom: %v", err)
	}
	ghost := c.GhostIntent("alice")
	if err = ghost.EnsureJoined(ctx, roomID); err != nil {
		t.Fatal(err)
	}
	resp, err := ghost.SendMessage(ctx, roomID, event.EventMessage, &event.Content{Parsed: &event.MessageEventContent{MsgType: event.MsgText, Body: "synthetic text"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	evt, err := ghost.GetEvent(ctx, roomID, resp.EventID)
	if err != nil || evt.Content.AsMessage().Body != "synthetic text" || evt.Sender != ghost.GetMXID() {
		t.Fatalf("GetEvent: %v", err)
	}
	if _, err = c.owner.SendMessage(ctx, roomID, event.EventReaction, &event.Content{Parsed: &event.ReactionEventContent{
		RelatesTo: event.RelatesTo{Type: event.RelAnnotation, EventID: resp.EventID, Key: "❤️"}}}, nil); err != nil {
		t.Fatal(err)
	}
	chat, err := ib.Chat(ctx, string(roomID))
	if err != nil || chat.Kind != "dm" || chat.Title == nil || *chat.Title != "Synthetic chat" || chat.DMContact == nil || chat.Account == nil || chat.Unread != 1 {
		t.Fatalf("chat not derived correctly: %+v", chat)
	}
	msg, _ := ib.Message(ctx, string(resp.EventID))
	if len(msg.Reactions) != 1 || !msg.Reactions[0].FromMe || msg.Reactions[0].Key != "❤" {
		t.Fatalf("reaction not stored (variation selector removed): %+v", msg.Reactions)
	}
	if _, err = ghost.SendMessage(ctx, roomID, event.EventRedaction, &event.Content{Parsed: &event.RedactionEventContent{Redacts: resp.EventID}}, nil); err != nil {
		t.Fatal(err)
	}
	if evt, err = ghost.GetEvent(ctx, roomID, resp.EventID); err != nil || evt.Unsigned.RedactedBecause == nil {
		t.Fatal("redacted event not marked as redacted")
	}
	if _, err = ghost.GetEvent(ctx, roomID, "$nope"); err == nil {
		t.Fatal("expected M_NOT_FOUND for an unknown event")
	}
	members, err := c.GetMembers(ctx, roomID)
	if err != nil || members[ghost.GetMXID()] == nil {
		t.Fatal("membership not stored")
	}
	// The owner reading on another device clears the unread count.
	if err = c.owner.MarkRead(ctx, roomID, resp.EventID, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if chat, _ = ib.Chat(ctx, string(roomID)); chat.Unread != 0 {
		t.Fatalf("expected 0 unread after the owner read the chat, got %d", chat.Unread)
	}
}

// TestTombstoneHidesMergedRoom covers bridgev2's portal merge (eg a WhatsApp phone-number DM merged into its LID
// chat): the source room gets a tombstone, then DeleteRoom with puppetsOnly. The old chat must stop being listed.
func TestTombstoneHidesMergedRoom(t *testing.T) {
	c, ib := newTestConnector(t, "whatsapp")
	ctx := context.Background()
	rooms := make([]id.RoomID, 2)
	for i, portalID := range []string{"15550000001@s.whatsapp.net", "123456@lid"} {
		key := networkid.PortalKey{ID: networkid.PortalID(portalID), Receiver: "login1"}
		stateKey := "cappy-whatsapp"
		info := &event.BridgeEventContent{Channel: event.BridgeInfoSection{ID: portalID, Receiver: "login1"}, BeeperRoomTypeV2: "dm"}
		roomID, err := c.BotIntent().CreateRoom(ctx, &mautrix.ReqCreateRoom{
			BeeperLocalRoomID: c.RoomIDFor(key),
			IsDirect:          true,
			InitialState:      []*event.Event{{Type: event.StateBridge, StateKey: &stateKey, Content: event.Content{Parsed: info}}},
		})
		if err != nil {
			t.Fatal(err)
		}
		rooms[i] = roomID
	}
	if _, err := c.BotIntent().SendState(ctx, rooms[0], event.StateTombstone, "", &event.Content{Parsed: &event.TombstoneEventContent{
		Body: "This room has been merged", ReplacementRoom: rooms[1]}}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := c.BotIntent().DeleteRoom(ctx, rooms[0], true); err != nil {
		t.Fatal(err)
	}
	old, err := ib.ChatRow(ctx, string(rooms[0]))
	if err != nil || !old.Deleted {
		t.Fatalf("merged room still listed: %+v (%v)", old, err)
	}
	if kept, err := ib.ChatRow(ctx, string(rooms[1])); err != nil || kept.Deleted {
		t.Fatalf("replacement room hidden: %+v (%v)", kept, err)
	}
	chats, _, err := ib.ListChats(ctx, inbox.ChatQuery{Limit: 10})
	if err != nil || len(chats) != 1 {
		t.Fatalf("expected only the replacement chat to be listed, got %d (%v)", len(chats), err)
	}
}
