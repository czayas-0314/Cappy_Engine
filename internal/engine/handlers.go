package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"cappy.local/engine/internal/inbox"
	"cappy.local/engine/internal/localmx"
	"cappy.local/engine/internal/rpc"
)

const (
	maxChatLimit    = 200
	maxMessageLimit = 200
	maxSearchLimit  = 100
	maxTextRunes    = 65536
)

func clampLimit(limit, def, max int) (int, error) {
	if limit == 0 {
		return def, nil
	}
	if limit < 0 || limit > max {
		return 0, rpc.InvalidParams("limit must be between 1 and %d", max)
	}
	return limit, nil
}

// decorateAccount fills in whether the account's login is currently loaded.
func (e *Engine) decorateAccount(acc *inbox.Account) {
	acc.LoggedIn = false
	if acc.LoggedOutAt != nil {
		return
	}
	if n := e.networkMap[acc.Network]; n != nil && n.br != nil {
		acc.LoggedIn = n.br.GetCachedUserLoginByID(networkid.UserLoginID(acc.LoginID)) != nil
	}
}

func (e *Engine) handleAccountsList(ctx context.Context, params json.RawMessage) (any, error) {
	var p struct{}
	if err := rpc.Decode(params, &p); err != nil {
		return nil, err
	}
	accounts, err := e.inbox.ListAccounts(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*inbox.Account, 0, len(accounts))
	for _, a := range accounts {
		e.decorateAccount(a)
		out = append(out, a)
	}
	return map[string]any{"accounts": out}, nil
}

type logoutParams struct {
	Account    string `json:"account"`
	Remote     bool   `json:"remote"`
	PurgeLocal bool   `json:"purgeLocal"`
}

func (e *Engine) handleAccountsLogout(ctx context.Context, params json.RawMessage) (any, error) {
	var p logoutParams
	if err := rpc.Decode(params, &p); err != nil {
		return nil, err
	}
	acc, err := e.inbox.Account(ctx, p.Account)
	if errors.Is(err, inbox.ErrNotFound) {
		return nil, rpc.NewError(CodeAccountNotFound, "account_not_found", "no such account")
	} else if err != nil {
		return nil, err
	}
	n := e.networkMap[acc.Network]
	if n != nil && n.br != nil {
		if login := n.br.GetCachedUserLoginByID(networkid.UserLoginID(acc.LoginID)); login != nil {
			// Remote logout unlinks the device on the network; local only forgets the session here.
			login.Delete(ctx, status.BridgeState{StateEvent: status.StateLoggedOut}, bridgev2.DeleteOpts{LogoutRemote: p.Remote})
		} else if !p.PurgeLocal {
			return nil, rpc.NewError(CodeNotLoggedIn, "not_logged_in", "the account is not logged in")
		}
	}
	// The inbox chats are only purged once bridgev2 has forgotten their rooms. Otherwise a later login of the same
	// account would find portals pointing at rooms that no longer exist and drop every event for them.
	purge := p.PurgeLocal
	var cleanupErr error
	if purge && n != nil && n.br != nil {
		if cleanupErr = e.deleteAccountPortals(ctx, n, networkid.UserLoginID(acc.LoginID)); cleanupErr != nil {
			e.log.Err(cleanupErr).Str("network", n.spec.ID).Msg("Failed to delete the account's portals; keeping its chats")
			purge = false
		}
	}
	ch, err := e.inbox.Tx(ctx, func(tx *inbox.Tx) error {
		if err := tx.SetAccountLoggedOut(acc.Account, time.Now().UnixMilli()); err != nil {
			return err
		}
		if purge {
			return tx.DeleteAccountData(acc.Account)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	e.notifier.Changed(ctx, ch)
	if cleanupErr != nil {
		return nil, errors.New("logged out, but the local data could not be purged")
	}
	return map[string]bool{"ok": true}, nil
}

// deleteAccountPortals deletes the bridgev2 portals (rows, messages and rooms) of one login. bridgev2's own cleanup on
// logout is disabled (CleanupOnLogout), so this is the purgeLocal part on the bridge side.
func (e *Engine) deleteAccountPortals(ctx context.Context, n *network, loginID networkid.UserLoginID) error {
	rows, err := n.br.DB.Portal.GetAllWithMXID(ctx)
	if err != nil {
		return err
	}
	var portals []*bridgev2.Portal
	for _, row := range rows {
		if row.Receiver != loginID {
			continue
		}
		portal, err := n.br.GetExistingPortalByKey(ctx, row.PortalKey)
		if err != nil {
			return err
		} else if portal != nil {
			portals = append(portals, portal)
		}
	}
	failed := 0
	bridgev2.DeleteManyPortals(ctx, portals, func(_ *bridgev2.Portal, roomFailed bool, _ error) {
		// Only a failed row delete matters: the room itself is deleted from the inbox right after.
		if !roomFailed {
			failed++
		}
	})
	if failed > 0 {
		return fmt.Errorf("%d portals could not be deleted", failed)
	}
	return nil
}

type chatsListParams struct {
	Account  *string `json:"account"`
	Cursor   *string `json:"cursor"`
	Limit    int     `json:"limit"`
	Archived bool    `json:"archived"`
}

func (e *Engine) handleChatsList(ctx context.Context, params json.RawMessage) (any, error) {
	var p chatsListParams
	if err := rpc.Decode(params, &p); err != nil {
		return nil, err
	}
	limit, err := clampLimit(p.Limit, 50, maxChatLimit)
	if err != nil {
		return nil, err
	}
	q := inbox.ChatQuery{Limit: limit, Archived: p.Archived}
	if p.Account != nil {
		if _, err = e.inbox.Account(ctx, *p.Account); err != nil {
			return nil, rpc.NewError(CodeAccountNotFound, "account_not_found", "no such account")
		}
		q.AccountID = *p.Account
	}
	if p.Cursor != nil {
		q.Cursor = *p.Cursor
	}
	chats, next, err := e.inbox.ListChats(ctx, q)
	if errors.Is(err, inbox.ErrBadCursor) {
		return nil, rpc.NewError(CodeInvalidCursor, "invalid_cursor", "invalid cursor")
	} else if err != nil {
		return nil, err
	}
	if chats == nil {
		chats = []*inbox.Chat{}
	}
	var nextCursor any
	if next != "" {
		nextCursor = next
	}
	return map[string]any{"chats": chats, "nextCursor": nextCursor}, nil
}

func (e *Engine) chatRoom(ctx context.Context, chatID string) (*inbox.ChatRow, *rpc.Error) {
	roomID := inbox.RoomFromChatID(chatID)
	if roomID == "" {
		return nil, rpc.NewError(CodeChatNotFound, "chat_not_found", "no such chat")
	}
	row, err := e.inbox.ChatRow(ctx, roomID)
	if err != nil || row.Kind == "system" {
		return nil, rpc.NewError(CodeChatNotFound, "chat_not_found", "no such chat")
	}
	return row, nil
}

type messagesListParams struct {
	Chat   string  `json:"chat"`
	Before *string `json:"before"`
	Limit  int     `json:"limit"`
}

func (e *Engine) handleMessagesList(ctx context.Context, params json.RawMessage) (any, error) {
	var p messagesListParams
	if err := rpc.Decode(params, &p); err != nil {
		return nil, err
	}
	limit, err := clampLimit(p.Limit, 50, maxMessageLimit)
	if err != nil {
		return nil, err
	}
	row, rerr := e.chatRoom(ctx, p.Chat)
	if rerr != nil {
		return nil, rerr
	}
	before := ""
	if p.Before != nil {
		if before = inbox.EventFromMessageID(*p.Before); before == "" {
			return nil, rpc.NewError(CodeInvalidCursor, "invalid_cursor", "invalid message ID")
		}
	}
	msgs, hasMore, err := e.inbox.ListMessages(ctx, row.RoomID, before, limit)
	if errors.Is(err, inbox.ErrBadCursor) {
		return nil, rpc.NewError(CodeInvalidCursor, "invalid_cursor", "unknown message")
	} else if err != nil {
		return nil, err
	}
	if msgs == nil {
		msgs = []*inbox.Message{}
	}
	return map[string]any{"messages": msgs, "hasMore": hasMore}, nil
}

type searchParams struct {
	Query   string  `json:"query"`
	Account *string `json:"account"`
	Chat    *string `json:"chat"`
	Limit   int     `json:"limit"`
	Cursor  *string `json:"cursor"`
}

func (e *Engine) handleSearch(ctx context.Context, params json.RawMessage) (any, error) {
	var p searchParams
	if err := rpc.Decode(params, &p); err != nil {
		return nil, err
	}
	if strings.TrimSpace(p.Query) == "" {
		return nil, rpc.InvalidParams("query must not be empty")
	}
	if len(p.Query) > 1024 {
		return nil, rpc.NewError(CodeTooLarge, "too_large", "query too long")
	}
	limit, err := clampLimit(p.Limit, 50, maxSearchLimit)
	if err != nil {
		return nil, err
	}
	q := inbox.SearchQuery{Query: p.Query, Limit: limit}
	if p.Account != nil {
		q.AccountID = *p.Account
	}
	if p.Chat != nil {
		row, rerr := e.chatRoom(ctx, *p.Chat)
		if rerr != nil {
			return nil, rerr
		}
		q.RoomID = row.RoomID
	}
	if p.Cursor != nil {
		q.Cursor = *p.Cursor
	}
	results, next, err := e.inbox.Search(ctx, q)
	if errors.Is(err, inbox.ErrBadCursor) {
		return nil, rpc.NewError(CodeInvalidCursor, "invalid_cursor", "invalid cursor")
	} else if err != nil {
		return nil, err
	}
	var nextCursor any
	if next != "" {
		nextCursor = next
	}
	return map[string]any{"results": results, "nextCursor": nextCursor}, nil
}

type sendParams struct {
	Chat      string  `json:"chat"`
	Text      string  `json:"text"`
	ReplyTo   *string `json:"replyTo"`
	ClientTxn string  `json:"clientTxn"`
}

// handleMessagesSend stores the message locally as "pending", answers right away, and hands the event to bridgev2 in
// the background. bridgev2 picks the chat's login (split portals) and reports the outcome through SendMessageStatus,
// which becomes message.updated{change:"status"}.
func (e *Engine) handleMessagesSend(ctx context.Context, params json.RawMessage) (any, error) {
	var p sendParams
	if err := rpc.Decode(params, &p); err != nil {
		return nil, err
	}
	if !utf8.ValidString(p.Text) || strings.ContainsRune(p.Text, 0) {
		return nil, rpc.InvalidParams("text must be valid UTF-8 without NUL characters")
	}
	if runes := utf8.RuneCountInString(p.Text); runes == 0 || strings.TrimSpace(p.Text) == "" {
		return nil, rpc.InvalidParams("text must not be empty")
	} else if runes > maxTextRunes {
		return nil, rpc.NewError(CodeTooLarge, "too_large", "text is longer than 65536 characters")
	}
	if len(p.ClientTxn) > 128 {
		return nil, rpc.InvalidParams("clientTxn is too long")
	}
	if p.ClientTxn != "" {
		if existing, err := e.inbox.MessageByClientTxn(ctx, p.ClientTxn); err == nil {
			return map[string]any{"message": existing}, nil
		}
	}
	row, rerr := e.chatRoom(ctx, p.Chat)
	if rerr != nil {
		return nil, rerr
	}
	if row.Deleted {
		return nil, rpc.NewError(CodeChatNotFound, "chat_not_found", "the chat was deleted")
	}
	n := e.networkMap[row.Network]
	if n == nil {
		return nil, rpc.NewError(CodeUnknownNetwork, "unknown_network", "unknown network")
	}
	if rerr = n.usable(); rerr != nil {
		return nil, rerr
	}
	acc, err := e.inbox.Account(ctx, row.AccountID)
	if err != nil {
		return nil, rpc.NewError(CodeNotLoggedIn, "not_logged_in", "the chat's account is not logged in")
	}
	if n.br.GetCachedUserLoginByID(networkid.UserLoginID(acc.LoginID)) == nil {
		return nil, rpc.NewError(CodeNotLoggedIn, "not_logged_in", "the chat's account is not logged in")
	}
	replyTo := ""
	if p.ReplyTo != nil {
		replyTo = inbox.EventFromMessageID(*p.ReplyTo)
		target, err := e.inbox.Message(ctx, replyTo)
		if replyTo == "" || err != nil || target.RoomID != row.RoomID {
			return nil, rpc.InvalidParams("replyTo is not a message in this chat")
		}
	}
	// Registered before the message is stored, so a send refused during shutdown leaves nothing pending.
	if !e.beginSend() {
		return nil, rpc.NewError(CodeNotReady, "not_ready", "the engine is stopping")
	}
	queued := false
	defer func() {
		if !queued {
			e.sendWG.Done()
		}
	}()
	eventID := id.EventID("$c" + inbox.NewULID())
	now := time.Now().UnixMilli()
	ch, err := e.inbox.Tx(ctx, func(tx *inbox.Tx) error {
		_, err := tx.PutMessage(inbox.NewMessage{
			EventID: string(eventID), RoomID: row.RoomID, FromMe: true, TS: now, Kind: "message",
			MsgType: string(event.MsgText), Body: p.Text, ReplyTo: replyTo, Status: "pending",
			ClientTxn: p.ClientTxn, Source: inbox.SourceLocal,
		})
		return err
	})
	if err != nil {
		if p.ClientTxn != "" {
			// A concurrent send with the same clientTxn won the race.
			if existing, lerr := e.inbox.MessageByClientTxn(ctx, p.ClientTxn); lerr == nil {
				return map[string]any{"message": existing}, nil
			}
		}
		return nil, err
	}
	msg, err := e.inbox.Message(ctx, string(eventID))
	if err != nil {
		return nil, err
	}
	e.notifier.Changed(ctx, ch)

	content := &event.MessageEventContent{MsgType: event.MsgText, Body: p.Text}
	if replyTo != "" {
		content.RelatesTo = (&event.RelatesTo{}).SetReplyTo(id.EventID(replyTo))
	}
	evt := &event.Event{
		ID:        eventID,
		Type:      event.EventMessage,
		Sender:    localmx.OwnerMXID,
		RoomID:    id.RoomID(row.RoomID),
		Timestamp: now,
		Content:   event.Content{Parsed: content},
	}
	queued = true
	go func() {
		defer e.sendWG.Done()
		sendCtx := e.log.With().Str("network", row.Network).Logger().WithContext(e.bgCtx)
		res := n.br.QueueMatrixEvent(sendCtx, evt)
		if res.Error != nil || (!res.Success && !res.Queued && !res.Ignored) {
			n.mx.SendMessageStatus(sendCtx, &bridgev2.MessageStatus{Status: event.MessageStatusFail, ErrorReason: event.MessageStatusGenericError},
				&bridgev2.MessageStatusEventInfo{RoomID: evt.RoomID, SourceEventID: evt.ID, EventType: evt.Type, Sender: evt.Sender})
		}
	}()
	return map[string]any{"message": msg}, nil
}

type markReadParams struct {
	Chat    string  `json:"chat"`
	Through *string `json:"through"`
}

// handleChatsMarkRead marks a chat read up to a message (or its newest message), clears the local
// unread count and hands a read receipt to the network, which sends it like the official app does.
func (e *Engine) handleChatsMarkRead(ctx context.Context, params json.RawMessage) (any, error) {
	var p markReadParams
	if err := rpc.Decode(params, &p); err != nil {
		return nil, err
	}
	row, rerr := e.chatRoom(ctx, p.Chat)
	if rerr != nil {
		return nil, rerr
	}
	var target *inbox.Message
	if p.Through != nil {
		eventID := inbox.EventFromMessageID(*p.Through)
		msg, err := e.inbox.Message(ctx, eventID)
		if eventID == "" || err != nil || msg.RoomID != row.RoomID {
			return nil, rpc.InvalidParams("through is not a message in this chat")
		}
		target = msg
	} else {
		newest, _, err := e.inbox.ListMessages(ctx, row.RoomID, "", 1)
		if err != nil {
			return nil, err
		}
		if len(newest) > 0 {
			target = newest[len(newest)-1]
		}
	}
	if target != nil {
		ch, err := e.inbox.Tx(ctx, func(tx *inbox.Tx) error { return tx.SetLastRead(row.RoomID, target.Ts) })
		if err != nil {
			return nil, err
		}
		e.notifier.Changed(ctx, ch)
		e.sendReadReceipt(row, target)
	}
	summary, err := e.inbox.ChatSummary(ctx, row.RoomID)
	if err != nil {
		return nil, err
	}
	return map[string]any{"unread": summary.Unread}, nil
}

// sendReadReceipt queues the receipt for the chat's network in the background. The local read state
// is already saved, so a network that is offline or not logged in only misses the receipt.
func (e *Engine) sendReadReceipt(row *inbox.ChatRow, target *inbox.Message) {
	n := e.networkMap[row.Network]
	if n == nil || n.usable() != nil || target.FromMe || !e.beginSend() {
		return
	}
	evt := &event.Event{
		Type:    event.EphemeralEventReceipt,
		Sender:  localmx.OwnerMXID,
		RoomID:  id.RoomID(row.RoomID),
		Mautrix: event.MautrixInfo{EventSource: event.SourceEphemeral},
		Content: event.Content{Parsed: &event.ReceiptEventContent{
			id.EventID(target.EventID): {event.ReceiptTypeRead: {localmx.OwnerMXID: {Timestamp: time.Now()}}},
		}},
	}
	go func() {
		defer e.sendWG.Done()
		ctx := e.log.With().Str("network", row.Network).Logger().WithContext(e.bgCtx)
		if res := n.br.QueueMatrixEvent(ctx, evt); res.Error != nil {
			e.log.Warn().Str("network", row.Network).Msg("A read receipt could not be queued")
		}
	}()
}
