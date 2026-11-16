package engine

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/event"

	"cappy.local/engine/internal/fakenet"
	"cappy.local/engine/internal/inbox"
	"cappy.local/engine/internal/localmx"
	"cappy.local/engine/internal/logredact"
	"cappy.local/engine/internal/rpc"
)

// ---- a small JSON-RPC client over pipes ----

type wireMsg struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
	Error  *rpc.Error      `json:"error"`
}

type client struct {
	t  *testing.T
	w  io.Writer
	mu sync.Mutex

	nextID  int
	pending map[string]chan wireMsg

	notesMu sync.Mutex
	notes   []wireMsg
	noteSig chan struct{}
}

func newClient(t *testing.T, w io.Writer, r io.Reader) *client {
	c := &client{t: t, w: w, pending: map[string]chan wireMsg{}, noteSig: make(chan struct{}, 1)}
	go func() {
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 1<<20), 16<<20)
		for sc.Scan() {
			var m wireMsg
			if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
				t.Errorf("engine wrote a line that is not JSON")
				return
			}
			if m.Method != "" {
				c.notesMu.Lock()
				c.notes = append(c.notes, m)
				c.notesMu.Unlock()
				select {
				case c.noteSig <- struct{}{}:
				default:
				}
				continue
			}
			c.mu.Lock()
			ch := c.pending[string(m.ID)]
			delete(c.pending, string(m.ID))
			c.mu.Unlock()
			if ch != nil {
				ch <- m
			}
		}
	}()
	return c
}

func (c *client) call(method string, params any) (json.RawMessage, *rpc.Error) {
	c.t.Helper()
	c.mu.Lock()
	c.nextID++
	id := c.nextID
	ch := make(chan wireMsg, 1)
	c.pending[jsonInt(id)] = ch
	c.mu.Unlock()
	line, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
	if _, err := c.w.Write(append(line, '\n')); err != nil {
		c.t.Fatalf("write: %v", err)
	}
	select {
	case m := <-ch:
		return m.Result, m.Error
	case <-time.After(30 * time.Second):
		c.t.Fatalf("timed out waiting for %s", method)
	}
	return nil, nil
}

func jsonInt(i int) string {
	b, _ := json.Marshal(i)
	return string(b)
}

func (c *client) mustCall(method string, params any, out any) {
	c.t.Helper()
	res, err := c.call(method, params)
	if err != nil {
		c.t.Fatalf("%s failed: %d %s (%s)", method, err.Code, err.Data.Reason, err.Message)
	}
	if out != nil {
		if uerr := json.Unmarshal(res, out); uerr != nil {
			c.t.Fatalf("%s: bad result: %v", method, uerr)
		}
	}
}

// waitNote waits for a notification after index from (inclusive) matching method and pred; it returns the params
// and the index after it.
func (c *client) waitNote(from int, method string, pred func(json.RawMessage) bool) (json.RawMessage, int) {
	c.t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		c.notesMu.Lock()
		for i := from; i < len(c.notes); i++ {
			if c.notes[i].Method == method && (pred == nil || pred(c.notes[i].Params)) {
				p := c.notes[i].Params
				c.notesMu.Unlock()
				return p, i + 1
			}
		}
		c.notesMu.Unlock()
		select {
		case <-c.noteSig:
		case <-time.After(50 * time.Millisecond):
		case <-deadline:
			c.t.Fatalf("timed out waiting for notification %s", method)
		}
	}
}

func (c *client) noteCount() int {
	c.notesMu.Lock()
	defer c.notesMu.Unlock()
	return len(c.notes)
}

func (c *client) notesSince(from int) []wireMsg {
	c.notesMu.Lock()
	defer c.notesMu.Unlock()
	return append([]wireMsg(nil), c.notes[from:]...)
}

// ---- the test ----

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type testEngine struct {
	eng    *Engine
	net    *fakenet.Network
	c      *client
	logs   *syncBuffer
	inW    *io.PipeWriter
	exitCh chan int
}

func startTestEngine(t *testing.T, dir string) *testEngine {
	t.Helper()
	return startTestEngineWith(t, dir, nil)
}

// startTestEngineWith runs an engine with the fake network; configure (optional) runs while hello builds the bridge.
func startTestEngineWith(t *testing.T, dir string, configure func(bridgev2.NetworkConnector, *HelloParams) error) *testEngine {
	t.Helper()
	logs := &syncBuffer{}
	redactor := logredact.New(logs)
	log := zerolog.New(redactor).Level(zerolog.InfoLevel).With().Timestamp().Logger()
	fake := fakenet.New()
	eng, err := New(context.Background(), Options{
		DataDir: dir,
		Log:     log,
		Networks: []NetworkSpec{{
			ID:        "fake",
			New:       func() bridgev2.NetworkConnector { return fake },
			Configure: configure,
		}},
		OnInstallKey: redactor.SetKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	exitCh := make(chan int, 1)
	go func() { exitCh <- eng.Run(context.Background(), inR, outW) }()
	return &testEngine{eng: eng, net: fake, c: newClient(t, inW, outR), logs: logs, inW: inW, exitCh: exitCh}
}

func (te *testEngine) stop(t *testing.T) {
	t.Helper()
	te.c.mustCall("shutdown", map[string]any{}, nil)
	select {
	case code := <-te.exitCh:
		if code != 0 {
			t.Fatalf("engine exited with %d", code)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("engine did not stop")
	}
	_ = te.inW.Close()
}

func (te *testEngine) queue(t *testing.T, evt bridgev2.RemoteEvent) {
	t.Helper()
	login := te.eng.Bridge("fake").GetCachedUserLoginByID(fakenet.LoginID)
	if login == nil {
		t.Fatal("fake login not loaded")
	}
	if res := login.QueueRemoteEvent(evt); !res.Success {
		t.Fatalf("remote event not handled: %+v", res)
	}
}

func TestEndToEndWithFakeNetwork(t *testing.T) {
	te := startTestEngine(t, t.TempDir())
	c := te.c

	// Before hello only hello/ping/stats/shutdown work.
	if _, err := c.call("chats.list", map[string]any{}); err == nil || err.Code != CodeNotReady {
		t.Fatalf("expected not_ready before hello, got %+v", err)
	}
	var hello map[string]any
	c.mustCall("hello", map[string]any{"protocol": 1, "client": map[string]string{"name": "test", "version": "0"}}, &hello)
	if hello["protocol"].(float64) != 1 {
		t.Fatal("bad hello")
	}
	var nets struct {
		Networks []struct {
			Network string `json:"network"`
			State   string `json:"state"`
			Flows   []struct{ ID string }
		} `json:"networks"`
	}
	c.mustCall("networks.list", map[string]any{}, &nets)
	if len(nets.Networks) != 1 || nets.Networks[0].State != StateReady || len(nets.Networks[0].Flows) != 2 {
		t.Fatalf("unexpected networks: %+v", nets)
	}

	// A QR login that nobody scans: start, refuse a second start, cancel.
	var started struct {
		LoginID string             `json:"loginID"`
		Step    bridgev2.LoginStep `json:"step"`
	}
	c.mustCall("login.start", map[string]any{"network": "fake", "flow": "qr"}, &started)
	if started.Step.Type != bridgev2.LoginStepTypeDisplayAndWait || started.Step.DisplayAndWaitParams.Type != bridgev2.LoginDisplayTypeQR {
		t.Fatalf("expected a QR step, got %s", started.Step.Type)
	}
	if _, err := c.call("login.start", map[string]any{"network": "fake", "flow": "qr"}); err == nil || err.Code != CodeLoginInProgress {
		t.Fatalf("expected login_in_progress, got %+v", err)
	}
	mark := c.noteCount()
	c.mustCall("login.cancel", map[string]any{"loginID": started.LoginID}, nil)
	c.waitNote(mark, "login.step", func(p json.RawMessage) bool { return strings.Contains(string(p), `"status":"cancelled"`) })
	if _, err := c.call("login.cancel", map[string]any{"loginID": started.LoginID}); err == nil || err.Code != CodeLoginNotFound {
		t.Fatalf("expected login_not_found, got %+v", err)
	}
	if _, err := c.call("login.start", map[string]any{"network": "nope", "flow": "qr"}); err == nil || err.Code != CodeUnknownNetwork {
		t.Fatalf("expected unknown_network, got %+v", err)
	}

	// An instant login completes in the first response.
	var completed struct {
		LoginID   string        `json:"loginID"`
		Completed inbox.Account `json:"completed"`
	}
	mark = c.noteCount()
	c.mustCall("login.start", map[string]any{"network": "fake", "flow": "instant"}, &completed)
	if completed.Completed.Account == "" || !completed.Completed.LoggedIn {
		t.Fatalf("login did not complete: %+v", completed)
	}
	c.waitNote(mark, "login.step", func(p json.RawMessage) bool { return strings.Contains(string(p), `"status":"completed"`) })
	c.waitNote(mark, "account.state", func(p json.RawMessage) bool { return strings.Contains(string(p), `"CONNECTED"`) })
	var accounts struct{ Accounts []inbox.Account }
	c.mustCall("accounts.list", map[string]any{}, &accounts)
	if len(accounts.Accounts) != 1 || accounts.Accounts[0].State != "CONNECTED" || !accounts.Accounts[0].LoggedIn {
		t.Fatalf("unexpected accounts: %+v", accounts)
	}
	accountID := accounts.Accounts[0].Account

	// A remote message becomes a stored message and a message.new notification, followed by chat.updated.
	mark = c.noteCount()
	base := time.Now().Add(-time.Hour)
	te.queue(t, fakenet.TextMessage(fakenet.DMKey(), "alice", "a1", "CAPPY-CANARY-1 synthetic hello", base))
	p, after := c.waitNote(mark, "message.new", nil)
	var newMsg struct {
		Message inbox.Message     `json:"message"`
		Chat    inbox.ChatSummary `json:"chat"`
	}
	if err := json.Unmarshal(p, &newMsg); err != nil {
		t.Fatal(err)
	}
	if newMsg.Message.Text == nil || *newMsg.Message.Text != "CAPPY-CANARY-1 synthetic hello" || newMsg.Message.FromMe ||
		newMsg.Message.Sender == nil || newMsg.Chat.Unread != 1 || newMsg.Message.Account == nil || *newMsg.Message.Account != accountID {
		t.Fatalf("unexpected message.new: %s", p)
	}
	chatID := newMsg.Message.Chat
	firstMsgID := newMsg.Message.Message
	c.waitNote(after, "chat.updated", func(p json.RawMessage) bool { return strings.Contains(string(p), chatID) })

	var chats struct {
		Chats      []inbox.Chat `json:"chats"`
		NextCursor *string      `json:"nextCursor"`
	}
	c.mustCall("chats.list", map[string]any{"account": accountID}, &chats)
	if len(chats.Chats) != 1 || chats.Chats[0].Chat != chatID || chats.Chats[0].Kind != "dm" || chats.Chats[0].Unread != 1 ||
		chats.Chats[0].DMContact == nil || chats.Chats[0].DMContact.Name == nil || *chats.Chats[0].DMContact.Name != "Alice Synthetic" ||
		chats.Chats[0].LastMessage == nil || chats.Chats[0].LastMessage.Message != firstMsgID {
		t.Fatalf("unexpected chats: %+v", chats)
	}
	var msgs struct {
		Messages []inbox.Message `json:"messages"`
		HasMore  bool            `json:"hasMore"`
	}
	c.mustCall("messages.list", map[string]any{"chat": chatID}, &msgs)
	if len(msgs.Messages) != 1 || msgs.Messages[0].Message != firstMsgID {
		t.Fatalf("unexpected messages: %+v", msgs)
	}
	var found struct {
		Results []inbox.SearchResult `json:"results"`
	}
	c.mustCall("search", map[string]any{"query": "synthetic hel"}, &found)
	if len(found.Results) != 1 || found.Results[0].Message.Message != firstMsgID || !strings.Contains(found.Results[0].Snippet, "[") {
		t.Fatalf("unexpected search results: %+v", found)
	}

	// The same remote message again (eg after a reconnect) is not stored or announced twice.
	mark = c.noteCount()
	te.queue(t, fakenet.TextMessage(fakenet.DMKey(), "alice", "a1", "CAPPY-CANARY-1 synthetic hello", base))
	time.Sleep(400 * time.Millisecond)
	for _, n := range c.notesSince(mark) {
		if n.Method == "message.new" {
			t.Fatal("duplicate remote message produced message.new")
		}
	}

	// Edit and reaction from the network become message.updated.
	mark = c.noteCount()
	te.queue(t, fakenet.Edit(fakenet.DMKey(), "alice", "a1", "CAPPY-CANARY-2 edited text", base.Add(time.Minute)))
	c.waitNote(mark, "message.updated", func(p json.RawMessage) bool { return strings.Contains(string(p), `"change":"edit"`) })
	te.queue(t, fakenet.Reaction(fakenet.DMKey(), "alice", "a1", "👍"))
	p, _ = c.waitNote(mark, "message.updated", func(p json.RawMessage) bool { return strings.Contains(string(p), `"change":"reactions"`) })
	if !strings.Contains(string(p), `"key":"👍"`) {
		t.Fatalf("reaction missing: %s", p)
	}

	// messages.send: pending right away, then the fake network receives it and the status becomes "sent".
	var sent struct {
		Message inbox.Message `json:"message"`
	}
	mark = c.noteCount()
	c.mustCall("messages.send", map[string]any{"chat": chatID, "text": "CAPPY-CANARY-3 reply", "clientTxn": "txn-1", "replyTo": firstMsgID}, &sent)
	if sent.Message.Status == nil || *sent.Message.Status != "pending" || !sent.Message.FromMe || sent.Message.ReplyTo == nil {
		t.Fatalf("unexpected send result: %+v", sent.Message)
	}
	c.waitNote(mark, "message.updated", func(p json.RawMessage) bool {
		return strings.Contains(string(p), sent.Message.Message) && strings.Contains(string(p), `"status":"sent"`)
	})
	if got := te.net.Sent(); len(got) != 1 || got[0].Content.Body != "CAPPY-CANARY-3 reply" || got[0].ReplyTo == nil {
		t.Fatalf("fake network did not receive the reply: %d", len(got))
	}
	// Retrying with the same clientTxn returns the same message and sends nothing new.
	var again struct {
		Message inbox.Message `json:"message"`
	}
	c.mustCall("messages.send", map[string]any{"chat": chatID, "text": "CAPPY-CANARY-3 reply", "clientTxn": "txn-1"}, &again)
	if again.Message.Message != sent.Message.Message || len(te.net.Sent()) != 1 {
		t.Fatal("clientTxn retry was not deduplicated")
	}
	// A network failure becomes status "failed".
	te.net.SetFailSend(true)
	var failing struct {
		Message inbox.Message `json:"message"`
	}
	mark = c.noteCount()
	c.mustCall("messages.send", map[string]any{"chat": chatID, "text": "will fail"}, &failing)
	c.waitNote(mark, "message.updated", func(p json.RawMessage) bool {
		return strings.Contains(string(p), failing.Message.Message) && strings.Contains(string(p), `"status":"failed"`)
	})
	te.net.SetFailSend(false)
	// Bad sends.
	for _, bad := range []map[string]any{
		{"chat": chatID, "text": ""},
		{"chat": chatID, "text": "x", "extra": 1},
		{"chat": chatID, "text": "nul\x00byte"},
		{"chat": "cht_NOPE", "text": "x"},
	} {
		if _, err := c.call("messages.send", bad); err == nil {
			t.Fatalf("bad send accepted: %v", bad)
		}
	}
	if _, err := c.call("messages.send", map[string]any{"chat": chatID, "text": strings.Repeat("x", 65537)}); err == nil || err.Code != CodeTooLarge {
		t.Fatalf("expected too_large, got %+v", err)
	}

	// Backfill: a new group chat brings history through BatchSend; it is stored but never announced as new.
	te.net.SetHistory(fakenet.GroupKey().ID, []*bridgev2.BackfillMessage{
		fakenet.BackfillText("bob", "g1", "CAPPY-CANARY-4 older group text", base.Add(-2*time.Hour)),
		fakenet.BackfillText("alice", "g2", "CAPPY-CANARY-5 newer group text", base.Add(-90*time.Minute)),
	})
	mark = c.noteCount()
	te.queue(t, fakenet.ChatResync(fakenet.GroupKey()))
	groupNote, _ := c.waitNote(mark, "chat.updated", func(p json.RawMessage) bool {
		return strings.Contains(string(p), `"kind":"group"`) && strings.Contains(string(p), `"lastMessage":{`)
	})
	var groupUpd struct {
		Chat inbox.Chat `json:"chat"`
	}
	_ = json.Unmarshal(groupNote, &groupUpd)
	c.mustCall("messages.list", map[string]any{"chat": groupUpd.Chat.Chat}, &msgs)
	if len(msgs.Messages) != 2 || msgs.Messages[0].Text == nil || !strings.Contains(*msgs.Messages[0].Text, "older") {
		t.Fatalf("backfill not stored: %d messages", len(msgs.Messages))
	}
	for _, n := range c.notesSince(mark) {
		if n.Method == "message.new" {
			t.Fatal("backfill produced message.new")
		}
	}
	// The same batch delivered twice is stored once (deterministic IDs + upserts).
	n := te.eng.networkMap["fake"]
	portal, err := te.eng.Bridge("fake").GetExistingPortalByKey(context.Background(), fakenet.GroupKey())
	if err != nil || portal == nil {
		t.Fatalf("group portal: %v", err)
	}
	dbMsg := &database.Message{ID: "g1", Room: fakenet.GroupKey()}
	evtID := n.mx.GenerateDeterministicEventID(portal.MXID, fakenet.GroupKey(), "g1", "")
	batch := &mautrix.ReqBeeperBatchSend{Events: []*event.Event{{
		ID: evtID, Type: event.EventMessage, Sender: n.mx.FormatGhostMXID("bob"), Timestamp: base.Add(-2 * time.Hour).UnixMilli(),
		Content: event.Content{Parsed: &event.MessageEventContent{MsgType: event.MsgText, Body: "CAPPY-CANARY-4 older group text"}},
	}}}
	for i := 0; i < 2; i++ {
		if _, err = n.mx.BatchSend(context.Background(), portal.MXID, batch, []*bridgev2.MatrixSendExtra{{MessageMeta: dbMsg}}); err != nil {
			t.Fatal(err)
		}
	}
	c.mustCall("messages.list", map[string]any{"chat": groupUpd.Chat.Chat}, &msgs)
	if len(msgs.Messages) != 2 {
		t.Fatalf("repeated batch created duplicates: %d messages", len(msgs.Messages))
	}

	// Chat ordering: the DM (activity an hour ago plus our sends) is newer than the group's backfill.
	c.mustCall("chats.list", map[string]any{}, &chats)
	if len(chats.Chats) != 2 || chats.Chats[0].Chat != chatID {
		t.Fatalf("unexpected chat order: %+v", chats.Chats)
	}

	// A remote deletion blanks the message and removes it from search.
	mark = c.noteCount()
	te.queue(t, fakenet.Remove(fakenet.DMKey(), "alice", "a1"))
	c.waitNote(mark, "message.updated", func(p json.RawMessage) bool { return strings.Contains(string(p), `"change":"delete"`) })
	c.mustCall("search", map[string]any{"query": "edited"}, &found)
	if len(found.Results) != 0 {
		t.Fatal("deleted message is still searchable")
	}

	var stats map[string]any
	c.mustCall("engine.stats", map[string]any{}, &stats)
	if stats["rssBytes"].(float64) <= 0 || stats["goroutines"].(float64) <= 0 {
		t.Fatalf("bad stats: %v", stats)
	}

	te.stop(t)

	// T7: no synthetic message text or names reached the logs.
	logs := te.logs.String()
	for _, canary := range []string{"CAPPY-CANARY", "Alice Synthetic", "Bob Synthetic", "Me Myself"} {
		if strings.Contains(logs, canary) {
			t.Fatalf("canary %q leaked into logs", canary)
		}
	}
	if !strings.Contains(logs, "Engine stopped") {
		t.Fatal("expected engine logs to be captured")
	}
}

// TestRestartKeepsData checks that a second engine on the same data folder sees the same chats and messages, and that
// an outgoing message left pending by a crash is resolved.
func TestRestartKeepsData(t *testing.T) {
	dir := t.TempDir()
	te := startTestEngine(t, dir)
	te.c.mustCall("hello", map[string]any{"protocol": 1, "client": map[string]string{"name": "test", "version": "0"}}, nil)
	te.c.mustCall("login.start", map[string]any{"network": "fake", "flow": "instant"}, nil)
	mark := te.c.noteCount()
	te.queue(t, fakenet.TextMessage(fakenet.DMKey(), "alice", "r1", "synthetic before restart", time.Now()))
	p, _ := te.c.waitNote(mark, "message.new", nil)
	var nm struct{ Message inbox.Message }
	_ = json.Unmarshal(p, &nm)
	// Simulate an interrupted send: a pending local row that never reached the bridge.
	_, err := te.eng.inbox.Tx(context.Background(), func(tx *inbox.Tx) error {
		_, err := tx.PutMessage(inbox.NewMessage{EventID: "$cINTERRUPTED", RoomID: inbox.RoomFromChatID(nm.Message.Chat), FromMe: true,
			TS: time.Now().UnixMilli(), Kind: "message", Body: "x", Status: "pending", Source: inbox.SourceLocal})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	te.stop(t)

	te2 := startTestEngine(t, dir)
	te2.c.mustCall("hello", map[string]any{"protocol": 1, "client": map[string]string{"name": "test", "version": "0"}}, nil)
	var accounts struct{ Accounts []inbox.Account }
	te2.c.mustCall("accounts.list", map[string]any{}, &accounts)
	if len(accounts.Accounts) != 1 || !accounts.Accounts[0].LoggedIn {
		t.Fatalf("login not restored: %+v", accounts)
	}
	var msgs struct{ Messages []inbox.Message }
	te2.c.mustCall("messages.list", map[string]any{"chat": nm.Message.Chat}, &msgs)
	if len(msgs.Messages) != 2 {
		t.Fatalf("expected 2 messages after restart, got %d", len(msgs.Messages))
	}
	for _, m := range msgs.Messages {
		if m.Message == "msg_cINTERRUPTED" && (m.Status == nil || *m.Status != "failed" || m.StatusError == nil || *m.StatusError != "interrupted") {
			t.Fatalf("interrupted send not resolved: %+v", m)
		}
	}
	// The ghost intent for the restarted bridge maps back to the same contact.
	if _, ok := te2.eng.networkMap["fake"].mx.ParseGhostMXID(te2.eng.networkMap["fake"].mx.FormatGhostMXID("alice")); !ok {
		t.Fatal("ghost MXID round trip failed")
	}
	_ = localmx.OwnerMXID
	te2.stop(t)
}

// TestPurgeLogoutThenRelogin logs out with purgeLocal and logs the same account in again. The bridge must recreate the
// chat: before the fix, its portal still pointed at the purged room and every event for it was dropped.
func TestPurgeLogoutThenRelogin(t *testing.T) {
	te := startTestEngine(t, t.TempDir())
	c := te.c
	c.mustCall("hello", map[string]any{"protocol": 1, "client": map[string]string{"name": "test", "version": "0"}}, nil)
	var completed struct {
		Completed inbox.Account `json:"completed"`
	}
	c.mustCall("login.start", map[string]any{"network": "fake", "flow": "instant"}, &completed)
	mark := c.noteCount()
	te.queue(t, fakenet.TextMessage(fakenet.DMKey(), "alice", "p1", "synthetic before purge", time.Now().Add(-time.Minute)))
	c.waitNote(mark, "message.new", nil)
	portal, err := te.eng.Bridge("fake").GetExistingPortalByKey(context.Background(), fakenet.DMKey())
	if err != nil || portal == nil || portal.MXID == "" {
		t.Fatalf("DM portal has no room: %v", err)
	}
	oldRoom := portal.MXID

	c.mustCall("accounts.logout", map[string]any{"account": completed.Completed.Account, "purgeLocal": true}, nil)
	var chats struct {
		Chats []inbox.Chat `json:"chats"`
	}
	c.mustCall("chats.list", map[string]any{}, &chats)
	if len(chats.Chats) != 0 {
		t.Fatalf("purge left %d chats", len(chats.Chats))
	}
	if p, _ := te.eng.Bridge("fake").GetExistingPortalByKey(context.Background(), fakenet.DMKey()); p != nil && p.MXID != "" {
		t.Fatal("purge left the portal pointing at the deleted room")
	}

	c.mustCall("login.start", map[string]any{"network": "fake", "flow": "instant"}, &completed)
	mark = c.noteCount()
	te.queue(t, fakenet.TextMessage(fakenet.DMKey(), "alice", "p2", "synthetic after relogin", time.Now()))
	p, _ := c.waitNote(mark, "message.new", nil)
	var nm struct{ Message inbox.Message }
	_ = json.Unmarshal(p, &nm)
	if inbox.RoomFromChatID(nm.Message.Chat) == "" {
		t.Fatalf("bad chat in message.new: %s", p)
	}
	c.mustCall("chats.list", map[string]any{}, &chats)
	if len(chats.Chats) != 1 || chats.Chats[0].Chat != nm.Message.Chat {
		t.Fatalf("chat not recreated after relogin: %+v", chats.Chats)
	}
	_ = oldRoom
	te.stop(t)
}

// TestShutdownDuringHello closes stdin right behind a hello. The stop sequence must wait for hello and then stop every
// bridge it started (run with -race to check the networks list is not read while hello writes it).
func TestShutdownDuringHello(t *testing.T) {
	// 300 ms: hello finishes while the stop sequence waits for requests in flight. 2.5 s: that wait times out first, so
	// the stop sequence must wait on hello's lock before it reads the networks.
	for _, delay := range []time.Duration{0, 300 * time.Millisecond, 2500 * time.Millisecond} {
		entered := make(chan struct{})
		var once sync.Once
		te := startTestEngineWith(t, t.TempDir(), func(bridgev2.NetworkConnector, *HelloParams) error {
			once.Do(func() { close(entered) })
			time.Sleep(delay)
			return nil
		})
		if _, err := te.inW.Write([]byte(`{"jsonrpc":"2.0","id":1,"method":"hello","params":{"protocol":1,"client":{"name":"t","version":"0"}}}` + "\n")); err != nil {
			t.Fatal(err)
		}
		if delay > 0 {
			<-entered // hello is building the bridges
		}
		_ = te.inW.Close()
		select {
		case code := <-te.exitCh:
			if code != 0 {
				t.Fatalf("delay %s: engine exited with %d", delay, code)
			}
		case <-time.After(15 * time.Second):
			t.Fatalf("delay %s: engine did not stop", delay)
		}
		logs := te.logs.String()
		started := strings.Contains(logs, "Starting bridge")
		if delay > 0 && !started {
			t.Fatalf("delay %s: hello did not start the bridge", delay)
		}
		if started && !strings.Contains(logs, "Shutting down bridge") {
			t.Fatalf("delay %s: a bridge started by hello was never stopped", delay)
		}
		if strings.Index(logs, "Shutting down bridge") < strings.Index(logs, "Starting bridge") {
			t.Fatalf("delay %s: bridge stopped before it started", delay)
		}
	}
}

// TestNoLoginOrSendAfterStop checks that requests arriving while the engine stops are refused, not started.
func TestNoLoginOrSendAfterStop(t *testing.T) {
	te := startTestEngine(t, t.TempDir())
	te.c.mustCall("hello", map[string]any{"protocol": 1, "client": map[string]string{"name": "test", "version": "0"}}, nil)
	te.eng.stopping.Store(true)
	if _, err := te.eng.handleLoginStart(context.Background(), json.RawMessage(`{"network":"fake","flow":"qr"}`)); err == nil {
		t.Fatal("login.start registered a session while stopping")
	}
	if te.eng.beginSend() {
		t.Fatal("beginSend accepted a send while stopping")
	}
	te.eng.loginMu.Lock()
	n := len(te.eng.logins)
	te.eng.loginMu.Unlock()
	if n != 0 {
		t.Fatalf("%d login sessions registered while stopping", n)
	}
	te.eng.stopping.Store(false)
	te.stop(t)
}
