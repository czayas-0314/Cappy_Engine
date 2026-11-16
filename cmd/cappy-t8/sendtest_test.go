package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// A fake engine that answers accounts.list, chats.list, messages.list and messages.send in the DESIGN.md section 13
// shapes. Every chat and message carries canary titles, names, phone numbers and text that must never be printed.

const testAccount = "acc_K3V9QWERTY"

type fakeMsg struct {
	id        string
	fromMe    bool
	ts        int64
	kind      string // "" means message
	sender    string // "" means con_4F, the contact of every direct chat
	noSender  bool   // sender null, like a bridge notice
	botSender bool
}

type fakeChat struct {
	id        string
	kind      string
	account   string // "" means testAccount
	archived  bool
	noContact bool
	bot       bool
	late      bool      // missing from the first chats.list pass, like a chat that jumped ahead of the cursor
	msgs      []fakeMsg // oldest first
}

type fakeEngine struct {
	t          *testing.T
	accounts   string
	chats      []fakeChat
	chatPage   int // chats per chats.list page
	sendErr    error
	sendStatus string // status pushed as a message.updated note after messages.send ("" = none)
	sendReturn string // status in the messages.send result ("" = pending)

	statuses  chan statusNote
	armed     bool
	passes    int // chats.list calls that start a pass (archived=false, no cursor)
	calls     map[string]int
	sendCalls []map[string]any
	byTxn     map[string]map[string]any // messages created by messages.send, by clientTxn, with their final status
	created   int                       // messages.send calls that created (sent) a new message
}

func newFake(t *testing.T, chats ...fakeChat) *fakeEngine {
	return &fakeEngine{t: t, accounts: fakeAccounts, chats: chats, chatPage: 2, statuses: make(chan statusNote, 8),
		calls: map[string]int{}, byTxn: map[string]map[string]any{}}
}

func (f *fakeEngine) env(out *bytes.Buffer) sendTestEnv {
	return sendTestEnv{api: f, statuses: f.statuses, armStatus: func() { f.armed = true }, since: 0,
		statusWait: 200 * time.Millisecond, out: out}
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func canaryContact(bot bool) map[string]any {
	return map[string]any{"contact": "con_4F", "network": "whatsapp", "name": "CANARY-CONTACT Alice +15550002222",
		"avatar": nil, "isBot": bot}
}

func (c fakeChat) wire() map[string]any {
	acc := c.account
	if acc == "" {
		acc = testAccount
	}
	var dm any
	if c.kind == "dm" && !c.noContact {
		dm = canaryContact(c.bot)
	}
	return map[string]any{
		"chat": c.id, "account": acc, "network": "whatsapp", "kind": c.kind,
		"title": "CANARY-TITLE Alice Example +15550002222", "topic": "CANARY-TOPIC holiday plans", "avatar": nil,
		"dmContact": dm, "messageRequest": false, "lastActivity": 1763094000000,
		"lastMessage": map[string]any{"message": "msg_m7LAST", "fromMe": false, "sender": "con_4F", "ts": 1763094000000,
			"kind": "message", "msgtype": "m.text", "preview": "CANARY-BODY meet me at the station, call 0612345678"},
		"unread": 1, "markedUnread": false, "mutedUntil": nil, "pinned": false, "archived": c.archived, "deleted": false,
	}
}

func (m fakeMsg) wire(chat string) map[string]any {
	kind := m.kind
	if kind == "" {
		kind = "message"
	}
	var sender any
	if !m.fromMe && !m.noSender {
		c := canaryContact(m.botSender)
		if m.sender != "" {
			c["contact"] = m.sender
		}
		sender = c
	}
	return map[string]any{
		"message": m.id, "chat": chat, "account": testAccount, "sender": sender, "fromMe": m.fromMe, "ts": m.ts,
		"editedTs": nil, "deleted": false, "kind": kind, "msgtype": "m.text",
		"text": "CANARY-BODY meet me at the station, call 0612345678", "html": "<b>CANARY-HTML Family</b>",
		"update": nil, "replyTo": nil, "threadRoot": nil, "attachment": nil,
		"reactions": []any{}, "status": nil, "statusError": nil,
	}
}

func (f *fakeEngine) call(method string, params any, _ time.Duration) (json.RawMessage, error) {
	f.calls[method]++
	p, _ := params.(map[string]any)
	switch method {
	case "accounts.list":
		return json.RawMessage(f.accounts), nil
	case "chats.list":
		if p["account"] != testAccount {
			f.t.Errorf("chats.list without the account filter: %v", p["account"])
		}
		archived, _ := p["archived"].(bool)
		start := 0
		if cur, ok := p["cursor"].(string); ok {
			start, _ = strconv.Atoi(cur)
		} else if !archived {
			f.passes++
		}
		var list []map[string]any
		for _, c := range f.chats {
			if c.archived == archived && !(c.late && f.passes == 1) {
				list = append(list, c.wire())
			}
		}
		end := min(start+f.chatPage, len(list))
		var next any
		if end < len(list) {
			next = strconv.Itoa(end)
		}
		page := list[start:end]
		if page == nil {
			page = []map[string]any{}
		}
		return mustJSON(map[string]any{"chats": page, "nextCursor": next}), nil
	case "messages.list":
		var chat *fakeChat
		for i := range f.chats {
			if f.chats[i].id == p["chat"] {
				chat = &f.chats[i]
			}
		}
		if chat == nil {
			return nil, &rpcError{Code: 1010, Message: "no such chat"}
		}
		if p["limit"] != messagePageLimit {
			f.t.Errorf("messages.list limit %v", p["limit"])
		}
		end := len(chat.msgs)
		if before, ok := p["before"].(string); ok {
			end = -1
			for i, m := range chat.msgs {
				if m.id == before {
					end = i
				}
			}
			if end < 0 {
				return nil, &rpcError{Code: 1012, Message: "unknown message"}
			}
		}
		start := max(0, end-messagePageLimit)
		msgs := []map[string]any{}
		for _, m := range chat.msgs[start:end] {
			msgs = append(msgs, m.wire(chat.id))
		}
		return mustJSON(map[string]any{"messages": msgs, "hasMore": start > 0}), nil
	case "messages.send":
		f.sendCalls = append(f.sendCalls, p)
		if !f.armed {
			f.t.Error("messages.send before the status notes were armed")
		}
		if f.sendErr != nil {
			return nil, f.sendErr
		}
		txn, _ := p["clientTxn"].(string)
		if first, ok := f.byTxn[txn]; ok && txn != "" {
			// Like the engine: the first message with this clientTxn, unchanged, and nothing is sent.
			return mustJSON(map[string]any{"message": first}), nil
		}
		status := f.sendReturn
		if status == "" {
			status = "pending"
		}
		ts := time.Now().UnixMilli()
		if status != "pending" {
			ts -= 3600_000 // an earlier run's message
		}
		res := fakeMsg{id: "msg_SENT1", fromMe: true, ts: ts}.wire(p["chat"].(string))
		res["text"] = p["text"]
		res["status"] = status
		f.created++
		stored := maps.Clone(res)
		if f.sendStatus != "" {
			f.statuses <- statusNote{Message: "msg_OTHER", Status: "failed"} // another message's status: ignored
			f.statuses <- statusNote{Message: "msg_SENT1", Status: f.sendStatus}
			stored["status"] = f.sendStatus
		}
		f.byTxn[txn] = stored
		return mustJSON(map[string]any{"message": res}), nil
	}
	f.t.Errorf("unexpected call %s", method)
	return nil, &rpcError{Code: -32601, Message: "method not found"}
}

// Window helpers: the defaults are 2026-11-13T22:26:50-06:00 .. 22:27:35-06:00.
var (
	winStart = mustTime(defaultWindowStart).UnixMilli()
	winEnd   = mustTime(defaultWindowEnd).UnixMilli()
)

func mustTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

func in(sec int64) int64 { return winStart + sec*1000 }

var sendTestForbidden = append([]string{defaultTestText, "Cappy engine", "msg_m7", "HTML", "Alice"}, forbidden...)

func checkClean(t *testing.T, out string) {
	t.Helper()
	for _, bad := range sendTestForbidden {
		if strings.Contains(out, bad) {
			t.Errorf("output contains %q:\n%s", bad, out)
		}
	}
}

// realSendArgs asks for the real send to the chat that oneDirectChat (and most tests) put in the window.
var realSendArgs = []string{"--dry-run=false", "--yes", "--chat", "cht_ZQ4MAAAA"}

func opts(t *testing.T, args ...string) options {
	t.Helper()
	o, cmd, ok := parseArgs(append([]string{"send-test"}, args...))
	if !ok || cmd != "send-test" {
		t.Fatalf("parseArgs(%v) refused", args)
	}
	return o
}

func runFake(t *testing.T, f *fakeEngine, o options) (int, string) {
	t.Helper()
	var out bytes.Buffer
	code := sendTest(f.env(&out), o)
	checkClean(t, out.String())
	return code, out.String()
}

func TestSendTestDryRunSelectsTheOneDirectChat(t *testing.T) {
	// The direct chat's in-window message sits behind 250 newer messages, so finding it needs a second page.
	target := fakeChat{id: "cht_ZQ4MAAAA", kind: "dm", msgs: []fakeMsg{
		{id: "msg_m7A0", ts: winStart - 86400_000},
		{id: "msg_m7A1", ts: in(30)},
	}}
	for i := 0; i < 250; i++ {
		target.msgs = append(target.msgs, fakeMsg{id: fmt.Sprintf("msg_m7AN%03d", i), fromMe: i%2 == 0, ts: winEnd + 1000 + int64(i)})
	}
	group := fakeChat{id: "cht_ZQ4MBBBB", kind: "group", msgs: []fakeMsg{
		{id: "msg_m7B0", ts: winStart - 1},
		{id: "msg_m7B1", fromMe: true, ts: in(10)},
		{id: "msg_m7B2", ts: winEnd + 1},
	}}
	ownOnly := fakeChat{id: "cht_ZQ4MCCCC", kind: "dm", msgs: []fakeMsg{
		{id: "msg_m7C1", fromMe: true, ts: in(20)},
		{id: "msg_m7C2", ts: in(21), kind: "update"},
	}}
	archived := fakeChat{id: "cht_ZQ4MDDDD", kind: "dm", archived: true, msgs: []fakeMsg{{id: "msg_m7D1", ts: winStart - 60_000}}}
	empty := fakeChat{id: "cht_ZQ4MEEEE", kind: "group"}
	f := newFake(t, group, ownOnly, target, archived, empty)

	code, out := runFake(t, f, opts(t))
	if code != 0 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	want := "send-test: matched chats: 1\n" +
		"send-test: chat: cht_ZQ4MAAAA\n" +
		"send-test: kind: dm\n" +
		"send-test: incoming message at: 2026-11-13T22:27:20.000-06:00\n" +
		"send-test: dry run: nothing was sent\n"
	if out != want {
		t.Fatalf("output:\n%s\nwant:\n%s", out, want)
	}
	if len(f.sendCalls) != 0 || f.calls["messages.send"] != 0 {
		t.Fatal("a dry run called messages.send")
	}
	if f.calls["messages.list"] != 6 { // one page per chat, two for the target
		t.Errorf("messages.list calls %d, want 6", f.calls["messages.list"])
	}
}

func TestSendTestRefusesWhenNoChatMatches(t *testing.T) {
	f := newFake(t,
		fakeChat{id: "cht_ZQ4MAAAA", kind: "dm", msgs: []fakeMsg{{id: "msg_m7A1", ts: winStart - 1}, {id: "msg_m7A2", fromMe: true, ts: in(5)}}},
		fakeChat{id: "cht_ZQ4MBBBB", kind: "dm", msgs: []fakeMsg{{id: "msg_m7B1", ts: winEnd + 1}}},
	)
	code, out := runFake(t, f, opts(t, realSendArgs...))
	if code == 0 || !strings.Contains(out, "matched chats: 0") || !strings.Contains(out, "refused: no chat") {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	if len(f.sendCalls) != 0 {
		t.Fatal("messages.send was called")
	}
}

func TestSendTestRefusesTwoMatchingChats(t *testing.T) {
	f := newFake(t,
		fakeChat{id: "cht_ZQ4MAAAA", kind: "dm", msgs: []fakeMsg{{id: "msg_m7A1", ts: in(1)}}},
		fakeChat{id: "cht_ZQ4MBBBB", kind: "dm", archived: true, msgs: []fakeMsg{{id: "msg_m7B1", ts: in(40)}}},
	)
	code, out := runFake(t, f, opts(t, realSendArgs...))
	if code == 0 || !strings.Contains(out, "matched chats: 2") || !strings.Contains(out, "refused: 2 chats") {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	if strings.Contains(out, "cht_") {
		t.Errorf("a refusal names a chat:\n%s", out)
	}
	if len(f.sendCalls) != 0 {
		t.Fatal("messages.send was called")
	}
}

func TestSendTestRefusesChatsThatAreNotOneToOne(t *testing.T) {
	for _, tc := range []struct {
		name string
		chat fakeChat
		why  string
	}{
		{"group", fakeChat{kind: "group"}, "not a direct chat (kind group)"},
		{"group dm", fakeChat{kind: "group_dm"}, "not a direct chat (kind group_dm)"},
		{"dm without contact", fakeChat{kind: "dm", noContact: true}, "has no contact"},
		{"bot", fakeChat{kind: "dm", bot: true}, "with a bot"},
		{"other account", fakeChat{kind: "dm", account: "acc_OTHER"}, "another account"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.chat.id = "cht_ZQ4MAAAA"
			tc.chat.msgs = []fakeMsg{{id: "msg_m7A1", ts: in(30)}}
			f := newFake(t, tc.chat, fakeChat{id: "cht_ZQ4MBBBB", kind: "dm", msgs: []fakeMsg{{id: "msg_m7B1", fromMe: true, ts: in(30)}}})
			code, out := runFake(t, f, opts(t, realSendArgs...))
			if code == 0 || !strings.Contains(out, "matched chats: 1") || !strings.Contains(out, tc.why) {
				t.Fatalf("exit %d:\n%s", code, out)
			}
			if strings.Contains(out, "cht_") {
				t.Errorf("a refusal names a chat:\n%s", out)
			}
			if len(f.sendCalls) != 0 {
				t.Fatal("messages.send was called")
			}
		})
	}
}

func TestSendTestIgnoresMessagesOutsideTheWindow(t *testing.T) {
	f := newFake(t,
		fakeChat{id: "cht_ZQ4MAAAA", kind: "dm", msgs: []fakeMsg{{id: "msg_m7A1", ts: winStart - 1}, {id: "msg_m7A2", ts: winEnd + 1}}},
		fakeChat{id: "cht_ZQ4MBBBB", kind: "group", msgs: []fakeMsg{{id: "msg_m7B1", ts: winStart - 3600_000}, {id: "msg_m7B2", ts: winEnd + 3600_000}}},
		fakeChat{id: "cht_ZQ4MCCCC", kind: "dm", msgs: []fakeMsg{{id: "msg_m7C1", ts: winStart}, {id: "msg_m7C2", ts: winEnd}}}, // both edges count
	)
	code, out := runFake(t, f, opts(t))
	if code != 0 || !strings.Contains(out, "chat: cht_ZQ4MCCCC\n") ||
		!strings.Contains(out, "at: 2026-11-13T22:26:50.000-06:00\n") || !strings.Contains(out, "at: 2026-11-13T22:27:35.000-06:00\n") {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	if strings.Contains(out, "AAAA") || strings.Contains(out, "BBBB") {
		t.Fatalf("a chat with messages only outside the window was printed:\n%s", out)
	}
}

func TestSendTestIgnoresOwnMessages(t *testing.T) {
	f := newFake(t,
		fakeChat{id: "cht_ZQ4MAAAA", kind: "dm", msgs: []fakeMsg{{id: "msg_m7A1", fromMe: true, ts: in(25)}, {id: "msg_m7A2", fromMe: true, ts: in(26)}}},
		fakeChat{id: "cht_ZQ4MBBBB", kind: "group", msgs: []fakeMsg{{id: "msg_m7B1", fromMe: true, ts: in(27)}}},
		fakeChat{id: "cht_ZQ4MCCCC", kind: "dm", msgs: []fakeMsg{{id: "msg_m7C1", ts: in(28)}}},
	)
	code, out := runFake(t, f, opts(t))
	if code != 0 || !strings.Contains(out, "matched chats: 1\n") || !strings.Contains(out, "chat: cht_ZQ4MCCCC\n") {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	// With only own messages in the window there is nothing to answer.
	f = newFake(t, fakeChat{id: "cht_ZQ4MAAAA", kind: "dm", msgs: []fakeMsg{{id: "msg_m7A1", fromMe: true, ts: in(25)}}})
	if code, out = runFake(t, f, opts(t)); code == 0 || !strings.Contains(out, "matched chats: 0") {
		t.Fatalf("exit %d:\n%s", code, out)
	}
}

func TestSendTestFindsAChatThatJumpsAheadOfTheCursor(t *testing.T) {
	f := newFake(t,
		fakeChat{id: "cht_ZQ4MAAAA", kind: "group"},
		fakeChat{id: "cht_ZQ4MBBBB", kind: "group"},
		fakeChat{id: "cht_ZQ4MCCCC", kind: "dm", late: true, msgs: []fakeMsg{{id: "msg_m7C1", ts: in(28)}}},
	)
	code, out := runFake(t, f, opts(t))
	if code != 0 || !strings.Contains(out, "chat: cht_ZQ4MCCCC\n") {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	if f.passes < 2 {
		t.Fatalf("%d chats.list passes", f.passes)
	}
}

func oneDirectChat() fakeChat {
	return fakeChat{id: "cht_ZQ4MAAAA", kind: "dm", msgs: []fakeMsg{{id: "msg_m7A1", ts: in(30)}, {id: "msg_m7A2", fromMe: true, ts: winEnd + 5000}}}
}

func TestSendTestRealSendCallsMessagesSendExactlyOnce(t *testing.T) {
	f := newFake(t, fakeChat{id: "cht_ZQ4MBBBB", kind: "group", msgs: []fakeMsg{{id: "msg_m7B1", fromMe: true, ts: in(30)}}}, oneDirectChat())
	f.sendStatus = "sent"
	code, out := runFake(t, f, opts(t, realSendArgs...))
	if code != 0 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	if len(f.sendCalls) != 1 || f.calls["messages.send"] != 1 || f.created != 1 {
		t.Fatalf("messages.send called %d times", len(f.sendCalls))
	}
	p := f.sendCalls[0]
	if p["chat"] != "cht_ZQ4MAAAA" || p["text"] != defaultTestText || len(p) != 3 {
		t.Fatalf("messages.send params %v", p)
	}
	if txn, _ := p["clientTxn"].(string); !strings.HasPrefix(txn, "t8-send-test-") || txn != clientTxn("cht_ZQ4MAAAA") {
		t.Fatalf("clientTxn %v", p["clientTxn"])
	}
	for _, want := range []string{"matched chats: 1\n", "chat: cht_ZQ4MAAAA\n", "message: msg_SENT1 status: pending\n", "message: msg_SENT1 status: sent\n"} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "dry run") || strings.Contains(out, "msg_OTHER") {
		t.Errorf("unexpected output:\n%s", out)
	}
}

func TestSendTestNeverSendsWithoutYes(t *testing.T) {
	if _, _, ok := parseArgs([]string{"send-test", "--dry-run=false"}); ok {
		t.Fatal("--dry-run=false without --yes was accepted")
	}
	if _, _, ok := parseArgs([]string{"--dry-run=false", "send-test"}); ok {
		t.Fatal("--dry-run=false without --yes (before the subcommand) was accepted")
	}
	for _, o := range []options{
		opts(t, "--yes"), // --yes alone is still a dry run
		opts(t),          // the default
		func() options { o := opts(t); o.dryRun = false; return o }(), // no --yes, even if parseArgs were bypassed
	} {
		f := newFake(t, oneDirectChat())
		f.sendStatus = "sent"
		code, out := runFake(t, f, o)
		if code != 0 || !strings.Contains(out, "dry run: nothing was sent") {
			t.Fatalf("exit %d:\n%s", code, out)
		}
		if len(f.sendCalls) != 0 {
			t.Fatal("messages.send was called without --dry-run=false --yes")
		}
	}
}

func TestSendTestNeverRetries(t *testing.T) {
	for _, tc := range []struct {
		name      string
		setup     func(*fakeEngine)
		wantInOut string
	}{
		{"send error", func(f *fakeEngine) {
			e := &rpcError{Code: 1011, Message: "CANARY +15550001111"}
			e.Data.Reason = "not_logged_in"
			f.sendErr = e
		}, "messages.send failed: engine error 1011 (not_logged_in); not retried"},
		{"failed status", func(f *fakeEngine) { f.sendStatus = "failed" }, "status: failed; not retried"},
		{"no status", func(f *fakeEngine) {}, "status: pending (no final status within 200ms); not retried"},
		{"earlier run", func(f *fakeEngine) { f.sendReturn = "sent" }, "an earlier run already sent the test message to this chat"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t, oneDirectChat())
			tc.setup(f)
			code, out := runFake(t, f, opts(t, realSendArgs...))
			if code == 0 || !strings.Contains(out, tc.wantInOut) {
				t.Fatalf("exit %d:\n%s", code, out)
			}
			if len(f.sendCalls) != 1 {
				t.Fatalf("messages.send called %d times, want exactly 1", len(f.sendCalls))
			}
		})
	}
}

func TestSendTestRefusesUnclearAccounts(t *testing.T) {
	two := `{"accounts":[
		{"account":"acc_K3V9QWERTY","network":"whatsapp","name":"CANARY-OWNER +15550001111","loggedIn":true,"state":"CONNECTED","stateTs":1},
		{"account":"acc_SECOND","network":"whatsapp","name":"CANARY-OWNER2","loggedIn":true,"state":"CONNECTED","stateTs":1}]}`
	stale := `{"accounts":[{"account":"acc_K3V9QWERTY","network":"whatsapp","name":"CANARY","loggedIn":true,"state":"CONNECTED","stateTs":5}]}`
	connecting := `{"accounts":[{"account":"acc_K3V9QWERTY","network":"whatsapp","loggedIn":true,"state":"CONNECTING","stateTs":1}]}`
	telegramOnly := `{"accounts":[{"account":"acc_TG","network":"telegram","loggedIn":true,"state":"CONNECTED","stateTs":1}]}`
	for name, tc := range map[string]struct {
		accounts string
		since    int64
		why      string
	}{
		"none":       {`{"accounts":[]}`, 0, "no logged-in WhatsApp account"},
		"telegram":   {telegramOnly, 0, "no logged-in WhatsApp account"},
		"two":        {two, 0, "2 logged-in WhatsApp accounts"},
		"stale":      {stale, 10, "not CONNECTED in this run"},
		"connecting": {connecting, 0, "not CONNECTED in this run (acc_K3V9QWERTY whatsapp CONNECTING)"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFake(t, oneDirectChat())
			f.accounts = tc.accounts
			var buf bytes.Buffer
			env := f.env(&buf)
			env.since = tc.since
			code := sendTest(env, opts(t, realSendArgs...))
			out := buf.String()
			checkClean(t, out)
			if code == 0 || !strings.Contains(out, tc.why) || !strings.Contains(out, "nothing was sent") {
				t.Fatalf("exit %d:\n%s", code, out)
			}
			if f.calls["chats.list"] != 0 || len(f.sendCalls) != 0 {
				t.Fatalf("went on after refusing the account: %v", f.calls)
			}
		})
	}
	if !whatsAppReady([]accountView{{Account: testAccount, Network: "whatsapp", LoggedIn: true, State: "CONNECTED", StateTs: ptr(int64(9))}}, 5) {
		t.Error("a fresh CONNECTED WhatsApp account is not ready")
	}
	if whatsAppReady(nil, 0) {
		t.Error("no account is ready")
	}
}

func ptr[T any](v T) *T { return &v }

func TestSendTestRefusesAnEngineError(t *testing.T) {
	// messages.list fails for one chat: the run must refuse rather than skip that chat and pick another.
	f := newFake(t, oneDirectChat(), fakeChat{id: "cht_ZQ4MGONE", kind: "dm"})
	var buf bytes.Buffer
	env := f.env(&buf)
	env.api = &brokenListFake{fakeEngine: f, failChat: "cht_ZQ4MGONE"}
	code := sendTest(env, opts(t, realSendArgs...))
	out := buf.String()
	checkClean(t, out)
	if code == 0 || !strings.Contains(out, "refused: engine error 1010 (chat_not_found)") || len(f.sendCalls) != 0 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
}

type brokenListFake struct {
	*fakeEngine
	failChat string
}

func (b *brokenListFake) call(method string, params any, timeout time.Duration) (json.RawMessage, error) {
	if p, _ := params.(map[string]any); method == "messages.list" && p["chat"] == b.failChat {
		e := &rpcError{Code: 1010, Message: "CANARY no such chat +15550002222"}
		e.Data.Reason = "chat_not_found"
		return nil, e
	}
	return b.fakeEngine.call(method, params, timeout)
}

func TestParseArgsSendTest(t *testing.T) {
	o := opts(t)
	if o.text != defaultTestText || !o.dryRun || o.yes {
		t.Fatalf("defaults: %+v", o)
	}
	if !o.windowStart.Equal(time.Date(2026, 11, 14, 4, 26, 50, 0, time.UTC)) || !o.windowEnd.Equal(time.Date(2026, 11, 14, 4, 27, 35, 0, time.UTC)) {
		t.Fatalf("window %s .. %s", o.windowStart, o.windowEnd)
	}
	o = opts(t, "--window-start", "2026-11-13T22:00:00-06:00", "--window-end=2026-11-13T22:10:00-06:00", "--text", "x", "--wait", "2m")
	if o.text != "x" || o.windowEnd.Sub(o.windowStart) != 10*time.Minute || o.wait != 2*time.Minute {
		t.Fatalf("flags not applied: %+v", o)
	}
	for _, args := range [][]string{
		{"watch", "--yes"},
		{"--text", "hi", "status"},
		{"login", "--dry-run=false"},
		{"send-test", "--window-start", "yesterday"},
		{"send-test", "--window-end", "2026-11-13T22:26:00-06:00"},   // before the default start
		{"send-test", "--window-start", "2026-11-13T20:00:00-06:00"}, // longer than an hour
		{"send-test", "--text", "   "},
		{"send-test", "--text", "a\x00b"},
		{"send-test", "--dry-run", "false", "--yes", "--chat", "cht_ZQ4MAAAA"}, // a bool flag needs =; "false" becomes an extra argument
		{"send-test", "extra"},
		{"send-test", "--dry-run=false", "--yes"},                  // a real send needs --chat
		{"--dry-run=false", "--yes", "send-test"},                  // also with the flags before the subcommand
		{"send-test", "--dry-run=false", "--chat", "cht_ZQ4MAAAA"}, // and --yes
		{"send-test", "--dry-run=false", "--yes", "--chat="},       // an empty --chat
		{"send-test", "--chat", "cht_"},                            // not a chat ID, even in a dry run
		{"send-test", "--chat", "ZQ4MAAAA"},
		{"send-test", "--chat", "acc_K3V9QWERTY"},
		{"send-test", "--chat", "cht_ZQ4M AAAA"},
		{"send-test", "--dry-run=false", "--yes", "--chat", "cht_ZQ4M/A"},
		{"status", "--chat", "cht_ZQ4MAAAA"}, // a send-test flag
	} {
		if _, _, ok := parseArgs(args); ok {
			t.Errorf("parseArgs(%q) accepted", args)
		}
	}
	for _, args := range [][]string{{"watch"}, {"status", "--wait", "5s"}, {"send-test", "--chat", "cht_ZQ4MAAAA"},
		{"send-test", "--dry-run=false", "--yes", "--chat", "cht_ZQ4MAAAA"}, {"--chat=cht_ZQ4MAAAA", "--yes", "--dry-run=false", "send-test"}} {
		if o, _, ok := parseArgs(args); !ok {
			t.Errorf("parseArgs(%q) refused", args)
		} else if args[0] != "watch" && args[0] != "status" && o.chat != "cht_ZQ4MAAAA" {
			t.Errorf("parseArgs(%q): chat %q", args, o.chat)
		}
	}
}

func TestClientTxnDependsOnlyOnTheChat(t *testing.T) {
	a, b := clientTxn("cht_ZQ4MAAAA"), clientTxn("cht_ZQ4MAAAA")
	if a != b || !validID(strings.Replace(a, "t8-send-test-", "x_", 1), "x_") || len(a) > 128 {
		t.Fatalf("%q %q", a, b)
	}
	if strings.Contains(a, "Cappy") || strings.Contains(a, "ZQ4M") {
		t.Fatalf("clientTxn carries content: %q", a)
	}
	if clientTxn("cht_ZQ4MBBBB") == a {
		t.Fatal("clientTxn ignores the chat")
	}
}

func TestStatusNotesAreRoutedOnlyWhenArmed(t *testing.T) {
	c := &client{pending: map[string]chan wireMsg{}, notes: make(chan wireMsg, 4), statuses: make(chan statusNote, 4)}
	line := func(change string, fromMe bool, status string) []byte {
		msg := fakeMsg{id: "msg_SENT1", fromMe: fromMe, ts: 1}.wire("cht_ZQ4MAAAA")
		msg["status"] = status
		return mustJSON(map[string]any{"jsonrpc": "2.0", "method": "message.updated",
			"params": map[string]any{"message": msg, "change": change}})
	}
	c.dispatch(line("status", true, "sent"))
	if len(c.statuses) != 0 {
		t.Fatal("a status note was routed before send-test armed it")
	}
	c.wantStatus.Store(true)
	c.dispatch(line("reactions", true, "sent"))
	c.dispatch(line("status", false, "sent"))
	c.dispatch(line("status", true, "delivered"))
	if len(c.statuses) != 1 {
		t.Fatalf("%d status notes, want 1", len(c.statuses))
	}
	if n := <-c.statuses; n != (statusNote{Message: "msg_SENT1", Status: "delivered"}) {
		t.Fatalf("note %+v", n)
	}
	if len(c.notes) != 0 || c.msgNew.Load() != 0 {
		t.Fatal("message.updated reached the other channels")
	}
}

func TestIncomingNeedsAPersonAsSender(t *testing.T) {
	for _, tc := range []struct {
		name string
		msg  fakeMsg
		want bool
	}{
		{"message from the contact", fakeMsg{}, true},
		{"notice from the contact", fakeMsg{kind: "notice"}, true},
		{"own message", fakeMsg{fromMe: true}, false},
		{"timeline update", fakeMsg{kind: "update"}, false},
		{"notice with sender null", fakeMsg{kind: "notice", noSender: true}, false},
		{"message with sender null", fakeMsg{noSender: true}, false},
		{"bot sender", fakeMsg{botSender: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.msg.id, tc.msg.ts = "msg_m7A1", in(30)
			wire := tc.msg.wire("cht_ZQ4MAAAA")
			msgs, _, err := decodeMessagesPage(mustJSON(map[string]any{"messages": []any{wire}, "hasMore": false}))
			if err != nil || len(msgs) != 1 {
				t.Fatalf("decode: %v", err)
			}
			if got := incoming(msgs[0]); got != tc.want {
				t.Fatalf("incoming = %v, want %v", got, tc.want)
			}
		})
	}
	// A sender object without a contact ID does not count either.
	msgs, _, err := decodeMessagesPage(json.RawMessage(`{"messages":[{"message":"msg_m7A1","chat":"cht_ZQ4MAAAA",
		"sender":{"contact":"","network":"whatsapp","name":"CANARY","avatar":null,"isBot":false},"fromMe":false,"ts":1,
		"kind":"message"}],"hasMore":false}`))
	if err != nil || len(msgs) != 1 || incoming(msgs[0]) {
		t.Fatalf("a sender without a contact ID counts as incoming: %v %+v", err, msgs)
	}
}

func TestSendTestIgnoresANoticeWithoutASender(t *testing.T) {
	// The only in-window message of the direct chat is a notice with sender null (like a bridge notice): no match.
	for _, args := range [][]string{nil, realSendArgs} {
		f := newFake(t, fakeChat{id: "cht_ZQ4MAAAA", kind: "dm", msgs: []fakeMsg{
			{id: "msg_m7A1", ts: winStart - 60_000},
			{id: "msg_m7A2", ts: in(30), kind: "notice", noSender: true},
		}})
		f.sendStatus = "sent"
		code, out := runFake(t, f, opts(t, args...))
		if code == 0 || !strings.Contains(out, "matched chats: 0\n") || !strings.Contains(out, "refused: no chat") {
			t.Fatalf("%v: exit %d:\n%s", args, code, out)
		}
		if len(f.sendCalls) != 0 {
			t.Fatal("messages.send was called")
		}
	}
}

func TestSendTestRefusesAMessageFromAnotherContact(t *testing.T) {
	for name, msgs := range map[string][]fakeMsg{
		"only another contact": {{id: "msg_m7A1", ts: in(30), sender: "con_9Z"}},
		"one of two":           {{id: "msg_m7A1", ts: in(20)}, {id: "msg_m7A2", ts: in(30), sender: "con_9Z"}},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFake(t, fakeChat{id: "cht_ZQ4MAAAA", kind: "dm", msgs: msgs})
			f.sendStatus = "sent"
			code, out := runFake(t, f, opts(t, realSendArgs...))
			if code == 0 || !strings.Contains(out, "matched chats: 1\n") ||
				!strings.Contains(out, "refused: a matched message in the direct chat is not from the chat's contact") {
				t.Fatalf("exit %d:\n%s", code, out)
			}
			if strings.Contains(out, "cht_") || strings.Contains(out, "con_") {
				t.Errorf("a refusal names a chat or contact:\n%s", out)
			}
			if len(f.sendCalls) != 0 {
				t.Fatal("messages.send was called")
			}
		})
	}
}

func TestSendTestRealSendNeedsTheDryRunChat(t *testing.T) {
	t.Run("without --chat", func(t *testing.T) {
		// parseArgs refuses it, so run exits 2 before the engine is started (or its folders are made).
		dir := t.TempDir()
		data, log := filepath.Join(dir, "data"), filepath.Join(dir, "logs", "engine.log")
		args := []string{"--engine", filepath.Join(dir, "no-engine"), "--data-dir", data, "--engine-log", log,
			"send-test", "--dry-run=false", "--yes"}
		if code := run(args); code != 2 {
			t.Fatalf("exit %d, want 2", code)
		}
		for _, p := range []string{data, filepath.Dir(log)} {
			if _, err := os.Stat(p); !os.IsNotExist(err) {
				t.Fatalf("%s was created: %v", p, err)
			}
		}
		// With parseArgs bypassed, sendTest refuses before its first engine call.
		o := opts(t)
		o.dryRun, o.yes = false, true
		f := newFake(t, oneDirectChat())
		code, out := runFake(t, f, o)
		if code == 0 || !strings.Contains(out, "refused: a real send needs --chat") || len(f.calls) != 0 {
			t.Fatalf("exit %d, calls %v:\n%s", code, f.calls, out)
		}
	})
	t.Run("mismatching --chat", func(t *testing.T) {
		for _, args := range [][]string{{"--chat", "cht_ZQ4MZZZZ"}, {"--dry-run=false", "--yes", "--chat", "cht_ZQ4MZZZZ"}} {
			f := newFake(t, oneDirectChat())
			f.sendStatus = "sent"
			code, out := runFake(t, f, opts(t, args...))
			if code == 0 || !strings.Contains(out, "chat: cht_ZQ4MAAAA\n") ||
				!strings.Contains(out, "refused: the selected chat is not the one given with --chat; nothing was sent") {
				t.Fatalf("%v: exit %d:\n%s", args, code, out)
			}
			if len(f.sendCalls) != 0 || f.armed {
				t.Fatalf("%v: messages.send was called or armed", args)
			}
		}
	})
	t.Run("matching --chat", func(t *testing.T) {
		f := newFake(t, oneDirectChat())
		f.sendStatus = "sent"
		code, out := runFake(t, f, opts(t, "--dry-run=false", "--yes", "--chat", "cht_ZQ4MAAAA"))
		if code != 0 || !strings.Contains(out, "message: msg_SENT1 status: sent\n") {
			t.Fatalf("exit %d:\n%s", code, out)
		}
		if len(f.sendCalls) != 1 || f.created != 1 || f.sendCalls[0]["chat"] != "cht_ZQ4MAAAA" {
			t.Fatalf("messages.send calls %v", f.sendCalls)
		}
		// A dry run given the matching --chat goes through too, and sends nothing.
		f = newFake(t, oneDirectChat())
		code, out = runFake(t, f, opts(t, "--chat", "cht_ZQ4MAAAA"))
		if code != 0 || !strings.Contains(out, "dry run: nothing was sent") || len(f.sendCalls) != 0 {
			t.Fatalf("exit %d:\n%s", code, out)
		}
	})
}

func TestSendTestReRunForTheSameChatDoesNotSendAgain(t *testing.T) {
	f := newFake(t, oneDirectChat())
	f.sendStatus = "sent"
	if code, out := runFake(t, f, opts(t, realSendArgs...)); code != 0 || f.created != 1 {
		t.Fatalf("first run: exit %d, %d sent:\n%s", code, f.created, out)
	}
	// A second run for the same chat, with another text and a wider window: the engine returns the first message.
	f.armed = false
	code, out := runFake(t, f, opts(t, append([]string{"--text", "another test", "--window-start", "2026-11-13T22:00:00-06:00"},
		realSendArgs...)...))
	if code == 0 || !strings.Contains(out, "message: msg_SENT1 status: sent\n") ||
		!strings.Contains(out, "an earlier run already sent the test message to this chat, so nothing new was sent") {
		t.Fatalf("second run: exit %d:\n%s", code, out)
	}
	if f.created != 1 || len(f.sendCalls) != 2 || f.sendCalls[0]["clientTxn"] != f.sendCalls[1]["clientTxn"] {
		t.Fatalf("%d sent, calls %v", f.created, f.sendCalls)
	}
}
