// send-test: the one owner-approved test message of T8. It finds the single direct chat of the WhatsApp account that
// received an incoming message inside a time window (the window in which `watch` saw message.new go from 0 to 1),
// prints only that chat's opaque ID, its kind and the matched timestamps, and, only with --dry-run=false --yes and the
// --chat ID that the dry run printed, sends it exactly one text message.
//
// Like the rest of cappy-t8 it decodes only opaque IDs, timestamps, the direction (fromMe), the sender's contact ID
// and bot flag, message and chat kinds and statuses. Chat titles, contact names, phone numbers and message text are
// never decoded from the engine's JSON, so they cannot be printed. The test text itself is never printed either.
//
// Copyright (C) 2026 Cappy Comm contributors. AGPL-3.0-or-later; see LICENSE.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	defaultTestText    = "Cappy engine test"
	defaultWindowStart = "2026-11-13T22:26:50-06:00"
	defaultWindowEnd   = "2026-11-13T22:27:35-06:00"

	messagePageLimit = 200 // messages.list maximum
	maxWindow        = time.Hour
	maxTextRunes     = 65536
	sendCallTimeout  = 30 * time.Second
	sendStatusWait   = 60 * time.Second
	tsLayout         = "2006-01-02T15:04:05.000Z07:00"

	// clientTxnSeed is the fixed part of the send's clientTxn; the chat ID is the only other part.
	clientTxnSeed = "cappy-t8 T8 send-test"
)

// sendTestFlags are refused on the other subcommands, so a stray --yes never looks like it did something.
var sendTestFlags = map[string]bool{"text": true, "window-start": true, "window-end": true, "dry-run": true, "yes": true,
	"chat": true}

// engineAPI is the part of the engine client that send-test uses. Tests replace it with a fake engine.
type engineAPI interface {
	call(method string, params any, timeout time.Duration) (json.RawMessage, error)
}

// messageView is a content-free view of a Message: no text, html, attachment or reactions, and of the sender (a
// Contact, or null) only its contact ID and bot flag.
type messageView struct {
	Message string         `json:"message"`
	Chat    string         `json:"chat"`
	Sender  *contactIDView `json:"sender"`
	FromMe  bool           `json:"fromMe"`
	Ts      int64          `json:"ts"`
	Kind    string         `json:"kind"` // message | notice | update
	Status  *string        `json:"status"`
}

// statusNote is a message.updated{change:"status"} for one of the owner's own messages, reduced to ID and status.
type statusNote struct {
	Message string
	Status  string
}

func decodeStatusNote(raw json.RawMessage) (statusNote, bool) {
	var p struct {
		Message messageView `json:"message"`
		Change  string      `json:"change"`
	}
	if json.Unmarshal(raw, &p) != nil || p.Change != "status" || !p.Message.FromMe || p.Message.Message == "" ||
		p.Message.Status == nil {
		return statusNote{}, false
	}
	return statusNote{Message: p.Message.Message, Status: *p.Message.Status}, true
}

func decodeMessagesPage(raw json.RawMessage) ([]messageView, bool, error) {
	var r struct {
		Messages []messageView `json:"messages"`
		HasMore  *bool         `json:"hasMore"`
	}
	if err := json.Unmarshal(raw, &r); err != nil || r.HasMore == nil {
		return nil, false, errors.New("messages.list: unexpected result shape")
	}
	return r.Messages, *r.HasMore, nil
}

func (o *options) setWindow(start, end string) error {
	var err error
	if o.windowStart, err = time.Parse(time.RFC3339, start); err != nil {
		return errors.New("--window-start is not an RFC3339 time")
	}
	if o.windowEnd, err = time.Parse(time.RFC3339, end); err != nil {
		return errors.New("--window-end is not an RFC3339 time")
	}
	if !o.windowEnd.After(o.windowStart) {
		return errors.New("--window-end must be after --window-start")
	}
	if o.windowEnd.Sub(o.windowStart) > maxWindow {
		return fmt.Errorf("the window is longer than %s; narrow it to the minute the message arrived", maxWindow)
	}
	return nil
}

// checkTestText applies the engine's own text rules up front, without ever echoing the text.
func checkTestText(s string) error {
	switch {
	case !utf8.ValidString(s) || strings.ContainsRune(s, 0):
		return errors.New("--text must be valid UTF-8 without NUL characters")
	case strings.TrimSpace(s) == "":
		return errors.New("--text must not be empty")
	case utf8.RuneCountInString(s) > maxTextRunes:
		return fmt.Errorf("--text is longer than %d characters", maxTextRunes)
	}
	return nil
}

// validID reports whether s is an opaque engine ID with the given prefix and nothing but ID characters.
func validID(s, prefix string) bool {
	return strings.HasPrefix(s, prefix) && len(s) > len(prefix) && len(s) <= 64 && token(s) == s
}

// whatsAppLogins returns the logged-in WhatsApp accounts.
func whatsAppLogins(accs []accountView) []accountView {
	var wa []accountView
	for _, a := range accs {
		if a.Network == "whatsapp" && a.LoggedIn {
			wa = append(wa, a)
		}
	}
	return wa
}

// whatsAppReady is the send-test wait condition: at least one logged-in WhatsApp account, and every one of them
// CONNECTED with a state reported in this run.
func whatsAppReady(accs []accountView, since int64) bool {
	wa := whatsAppLogins(accs)
	return len(wa) > 0 && pendingAccounts(wa, since) == 0
}

// pickWhatsAppAccount returns the one logged-in, CONNECTED WhatsApp account, or why there is not exactly one.
func pickWhatsAppAccount(accs []accountView, since int64) (accountView, string) {
	wa := whatsAppLogins(accs)
	switch {
	case len(wa) == 0:
		return accountView{}, "no logged-in WhatsApp account in this data folder"
	case len(wa) > 1:
		return accountView{}, fmt.Sprintf("%d logged-in WhatsApp accounts; exactly 1 is needed", len(wa))
	case pendingAccounts(wa, since) != 0:
		return accountView{}, fmt.Sprintf("the WhatsApp account is not CONNECTED in this run (%s)", accountLabel(wa[0]))
	case !validID(wa[0].Account, "acc_"):
		return accountView{}, "the WhatsApp account has an unexpected ID"
	}
	return wa[0], ""
}

// stableChats lists every chat of the account, repeating the listing until a pass finds no chat that the earlier
// passes missed. chats.list pages by last activity, so a chat that becomes active while the pages are read (offline
// messages arriving right after the connect) can jump ahead of the cursor; a repeated pass catches it.
func stableChats(api engineAPI, account string) ([]chatView, error) {
	seen := map[string]bool{}
	var all []chatView
	for pass := 0; pass < 5; pass++ {
		chats, err := listChats(api, account)
		if err != nil {
			return nil, err
		}
		added := 0
		for _, ch := range chats {
			if ch.Chat == "" {
				return nil, errors.New("chats.list: a chat without an ID")
			}
			if !seen[ch.Chat] {
				seen[ch.Chat] = true
				all = append(all, ch)
				added++
			}
		}
		if pass > 0 && added == 0 {
			return all, nil
		}
	}
	return nil, errors.New("chats.list: the chat list kept changing")
}

// incoming reports whether a message was sent to the owner by a person: not fromMe, not a timeline update (kind
// "update", such as membership or name changes), and from a sender contact that is not a bot. Notices without a
// sender (sender null, such as bridge notices) do not count.
func incoming(m messageView) bool {
	return !m.FromMe && m.Kind != "update" && m.Sender != nil && m.Sender.Contact != "" && !m.Sender.IsBot
}

// windowMsg is an incoming message inside the window: its timestamp (ms) and its sender's contact ID.
type windowMsg struct {
	ts     int64
	sender string
}

// incomingInWindow pages a chat's messages, newest first, and returns the incoming messages inside [startMs, endMs].
// It stops at the first page that reaches back before the window.
func incomingInWindow(api engineAPI, chat string, startMs, endMs int64) ([]windowMsg, error) {
	var before *string
	var hits []windowMsg
	for page := 0; page < 100000; page++ {
		params := map[string]any{"chat": chat, "limit": messagePageLimit}
		if before != nil {
			params["before"] = *before
		}
		raw, err := api.call("messages.list", params, 15*time.Second)
		if err != nil {
			return nil, err
		}
		msgs, hasMore, err := decodeMessagesPage(raw)
		if err != nil {
			return nil, err
		}
		for _, m := range msgs { // oldest first
			if incoming(m) && m.Ts >= startMs && m.Ts <= endMs {
				hits = append(hits, windowMsg{ts: m.Ts, sender: m.Sender.Contact})
			}
		}
		if !hasMore || len(msgs) == 0 || msgs[0].Ts < startMs {
			return hits, nil
		}
		if msgs[0].Message == "" {
			return nil, errors.New("messages.list: a message without an ID")
		}
		oldest := msgs[0].Message
		if before != nil && *before == oldest {
			return nil, errors.New("messages.list: paging did not advance")
		}
		before = &oldest
	}
	return nil, errors.New("messages.list: too many pages")
}

// windowHit is a chat with at least one incoming message inside the window.
type windowHit struct {
	chat chatView
	msgs []windowMsg
}

func scanWindow(api engineAPI, chats []chatView, start, end time.Time) ([]windowHit, error) {
	var hits []windowHit
	for _, ch := range chats {
		msgs, err := incomingInWindow(api, ch.Chat, start.UnixMilli(), end.UnixMilli())
		if err != nil {
			return nil, err
		}
		if len(msgs) > 0 {
			hits = append(hits, windowHit{chat: ch, msgs: msgs})
		}
	}
	return hits, nil
}

// allFrom reports whether there is at least one message and every one was sent by contact.
func allFrom(msgs []windowMsg, contact string) bool {
	for _, m := range msgs {
		if m.sender != contact {
			return false
		}
	}
	return len(msgs) > 0
}

// selectTarget applies the selection rule: exactly one chat with an incoming message in the window, and that chat
// is a one-to-one chat with a person (kind dm, a contact, not a bot) of this account, and every matched message was
// sent by that chat's contact. Groups, group DMs, status broadcasts and newsletters (all listed as kind group or
// group_dm) are refused. The reason holds counts and kinds only.
func selectTarget(hits []windowHit, account string) (windowHit, string) {
	if len(hits) == 0 {
		return windowHit{}, "no chat of the account has an incoming message in the window"
	}
	if len(hits) > 1 {
		return windowHit{}, fmt.Sprintf("%d chats have an incoming message in the window; exactly 1 is needed", len(hits))
	}
	h := hits[0]
	switch {
	case h.chat.Kind != "dm":
		return windowHit{}, fmt.Sprintf("the matching chat is not a direct chat (kind %s)", token(h.chat.Kind))
	case h.chat.DMContact == nil || h.chat.DMContact.Contact == "":
		return windowHit{}, "the matching direct chat has no contact"
	case h.chat.DMContact.IsBot:
		return windowHit{}, "the matching direct chat is with a bot"
	case !allFrom(h.msgs, h.chat.DMContact.Contact):
		return windowHit{}, "a matched message in the direct chat is not from the chat's contact"
	case h.chat.Deleted:
		return windowHit{}, "the matching chat is deleted"
	case h.chat.Account == nil || *h.chat.Account != account:
		return windowHit{}, "the matching chat belongs to another account"
	case !validID(h.chat.Chat, "cht_"):
		return windowHit{}, "the matching chat has an unexpected ID"
	}
	return h, ""
}

// clientTxn is derived from a fixed T8 constant and the chat ID only (not the text, not the window), so any re-run of
// the real send for the same chat returns the first run's message (the engine deduplicates clientTxn) instead of
// sending a second one.
func clientTxn(chat string) string {
	sum := sha256.Sum256([]byte(clientTxnSeed + "\x00" + chat))
	return "t8-send-test-" + hex.EncodeToString(sum[:16])
}

type sendTestEnv struct {
	api        engineAPI
	statuses   <-chan statusNote // status changes of own messages
	armStatus  func()            // starts routing status changes into statuses; called right before messages.send
	exited     <-chan struct{}   // the engine exited
	sigs       <-chan os.Signal
	since      int64 // account states reported before this (ms) are the previous run's
	statusWait time.Duration
	out        io.Writer
}

// sendTest selects the target chat and, for a real send, sends the one message. The engine must already be past
// hello and the account wait; the caller stops the engine afterwards. It never retries.
func sendTest(env sendTestEnv, o options) int {
	p := func(format string, a ...any) { _, _ = fmt.Fprintf(env.out, "send-test: "+format+"\n", a...) }
	refuse := func(format string, a ...any) int {
		p("refused: "+format+"; nothing was sent", a...)
		return 1
	}
	realSend := !o.dryRun && o.yes // both, always; parseArgs refuses --dry-run=false without --yes
	if realSend && !validID(o.chat, "cht_") {
		return refuse("a real send needs --chat with the chat ID that the dry run printed")
	}

	raw, err := env.api.call("accounts.list", nil, 10*time.Second)
	if err != nil {
		return refuse("accounts.list failed: %v", err)
	}
	accs, err := decodeAccounts(raw)
	if err != nil {
		return refuse("%v", err)
	}
	acc, why := pickWhatsAppAccount(accs, env.since)
	if why != "" {
		return refuse("%s", why)
	}
	chats, err := stableChats(env.api, acc.Account)
	if err != nil {
		return refuse("%v", err)
	}
	hits, err := scanWindow(env.api, chats, o.windowStart, o.windowEnd)
	if err != nil {
		return refuse("%v", err)
	}
	p("matched chats: %d", len(hits))
	target, why := selectTarget(hits, acc.Account)
	if why != "" {
		return refuse("%s", why)
	}
	p("chat: %s", target.chat.Chat)
	p("kind: %s", target.chat.Kind)
	loc := o.windowStart.Location()
	for _, m := range target.msgs {
		p("incoming message at: %s", time.UnixMilli(m.ts).In(loc).Format(tsLayout))
	}
	// The real send goes only to the chat the owner saw in the dry run; a dry run given --chat checks it too.
	if (realSend || o.chat != "") && target.chat.Chat != o.chat {
		return refuse("the selected chat is not the one given with --chat")
	}
	if !realSend {
		p("dry run: nothing was sent")
		return 0
	}

	select {
	case s := <-env.sigs:
		return refuse("%s received before the send", s)
	case <-env.exited:
		return refuse("the engine exited before the send")
	default:
	}
	if env.armStatus != nil {
		env.armStatus()
	}
	began := time.Now()
	// Exactly one messages.send. Whatever happens next (error, timeout, failed status), it is not repeated.
	raw, err = env.api.call("messages.send", map[string]any{
		"chat": target.chat.Chat, "text": o.text, "clientTxn": clientTxn(target.chat.Chat),
	}, sendCallTimeout)
	if err != nil {
		p("messages.send failed: %v; not retried", err)
		return 1
	}
	var res struct {
		Message messageView `json:"message"`
	}
	if json.Unmarshal(raw, &res) != nil || !validID(res.Message.Message, "msg_") {
		p("messages.send: unexpected result shape; not retried")
		return 1
	}
	m := res.Message
	status := "?"
	if m.Status != nil {
		status = token(*m.Status)
	}
	p("message: %s status: %s", m.Message, status)
	if m.Chat != target.chat.Chat || !m.FromMe {
		p("the engine returned a message that is not this send's; not retried")
		return 1
	}
	if status != "pending" || m.Ts < began.Add(-5*time.Second).UnixMilli() {
		p("an earlier run already sent the test message to this chat, so nothing new was sent; check the phone; not retried")
		return 1
	}

	timer := time.NewTimer(env.statusWait)
	defer timer.Stop()
	for {
		select {
		case n := <-env.statuses:
			if n.Message != m.Message {
				continue
			}
			switch n.Status {
			case "sent", "delivered", "read":
				p("message: %s status: %s", m.Message, n.Status)
				return 0
			case "failed":
				p("message: %s status: failed; not retried", m.Message)
				return 1
			}
		case <-timer.C:
			p("message: %s status: pending (no final status within %s); not retried", m.Message, env.statusWait)
			return 1
		case s := <-env.sigs:
			p("%s received while waiting for the status of %s; not retried", s, m.Message)
			return 1
		case <-env.exited:
			p("the engine exited while waiting for the status of %s", m.Message)
			return 1
		}
	}
}

// runSendTest waits for the WhatsApp account to be CONNECTED (like status), runs sendTest and stops the engine.
func runSendTest(c *client, o options, sigs <-chan os.Signal) int {
	since := c.since()
	if !waitAccounts(c, o, sigs, func(accs []accountView) bool { return whatsAppReady(accs, since) }) {
		return 1
	}
	code := sendTest(sendTestEnv{
		api:        c,
		statuses:   c.statuses,
		armStatus:  func() { c.wantStatus.Store(true) },
		exited:     c.exited,
		sigs:       sigs,
		since:      since,
		statusWait: sendStatusWait,
		out:        os.Stdout,
	}, o)
	if c.stop() != 0 {
		return 1
	}
	return code
}
