package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Fake engine results in the DESIGN.md section 13 shapes, full of content that must never reach the terminal.
const (
	fakeAccounts = `{"accounts":[{"account":"acc_K3V9QWERTY","network":"whatsapp","name":"CANARY-OWNER Pat +15550001111",
		"loggedIn":true,"state":"CONNECTED","error":null,"message":"CANARY-STATE-MESSAGE","userAction":null,
		"stateTs":1763050000000,"createdAt":1763040000000}]}`

	fakeChats = `{"chats":[
		{"chat":"cht_ZQ4MAAAA","account":"acc_K3V9QWERTY","network":"whatsapp","kind":"dm","title":"CANARY-TITLE Alice Example",
		 "avatar":null,"dmContact":{"contact":"con_4F","network":"whatsapp","name":"CANARY-CONTACT Alice +15550002222","avatar":null,"isBot":false},
		 "messageRequest":false,"lastActivity":1763050000000,
		 "lastMessage":{"message":"msg_m7Q","fromMe":false,"sender":"con_4F","ts":1763050000000,"kind":"message","msgtype":"m.text",
		                "preview":"CANARY-BODY meet me at the station, call 0612345678"},
		 "unread":3,"markedUnread":false,"mutedUntil":null,"pinned":false,"archived":false,"deleted":false},
		{"chat":"cht_ZQ4MBBBB","account":"acc_K3V9QWERTY","network":"whatsapp","kind":"group","title":"CANARY-GROUP Family Chat",
		 "topic":"CANARY-TOPIC holiday plans","avatar":null,"dmContact":null,"messageRequest":false,"lastActivity":1763050000001,
		 "lastMessage":{"message":"msg_m7R","fromMe":true,"sender":null,"ts":1763050000001,"kind":"message","msgtype":"m.text",
		                "preview":"CANARY-BODY-2 see you tomorrow"},
		 "unread":0,"markedUnread":false,"mutedUntil":null,"pinned":true,"archived":false,"deleted":false}
	],"nextCursor":null}`
)

var forbidden = []string{
	"CANARY", "Pat", "Alice", "Example", "Family", "holiday", "meet me", "station", "tomorrow",
	"+1555", "5550001111", "5550002222", "0612345678", "con_4F", "msg_m7",
}

func TestStatusLineHasNoChatTitlesOrMessageText(t *testing.T) {
	accs, err := decodeAccounts(json.RawMessage(fakeAccounts))
	if err != nil {
		t.Fatal(err)
	}
	chats, next, err := decodeChatsPage(json.RawMessage(fakeChats))
	if err != nil {
		t.Fatal(err)
	}
	if next != nil {
		t.Fatal("nextCursor should be nil")
	}
	line := formatStatus(snapshot{
		Time:       time.Date(2026, 11, 13, 18, 0, 0, 0, time.UTC),
		Accounts:   accs,
		Chats:      chats,
		MessageNew: 7,
		Stats:      &statsView{RSSBytes: 50 << 20, MaxRSSBytes: 64 << 20},
	})
	for _, bad := range forbidden {
		if strings.Contains(line, bad) {
			t.Errorf("status line contains %q: %s", bad, line)
		}
	}
	for _, want := range []string{
		"2026-11-13T18:00:00Z", "accounts=1", "[acc_K3V9QWERTY whatsapp CONNECTED]", "chats=2", "message.new=7",
		"rss=50.0MiB", "peak=64.0MiB",
	} {
		if !strings.Contains(line, want) {
			t.Errorf("status line lacks %q: %s", want, line)
		}
	}
	if strings.Contains(line, "\n") {
		t.Errorf("status line spans several lines: %q", line)
	}
}

func TestStatusLineWithErrorsAndNoStats(t *testing.T) {
	line := formatStatus(snapshot{Time: time.Unix(0, 0).UTC(), AccountsErr: errFake, ChatsErr: errFake})
	for _, want := range []string{"accounts=?", "chats=?", "message.new=0", "rss=?"} {
		if !strings.Contains(line, want) {
			t.Errorf("status line lacks %q: %s", want, line)
		}
	}
}

func TestAccountStateLineIsContentFree(t *testing.T) {
	var p struct {
		Account accountView `json:"account"`
	}
	raw := `{"account":{"account":"acc_K3V9QWERTY","network":"whatsapp","name":"CANARY-OWNER +15550001111","loggedIn":false,
		"state":"BAD_CREDENTIALS","error":"wa-logged-out","message":"CANARY-MESSAGE You were logged out from +15550001111",
		"userAction":"RESTART","stateTs":1,"createdAt":1}}`
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		t.Fatal(err)
	}
	got := accountLabel(p.Account)
	if want := "acc_K3V9QWERTY whatsapp BAD_CREDENTIALS logged-out error=wa-logged-out"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestScrubText(t *testing.T) {
	got := scrubText("pair failed for 31612345678@s.whatsapp.net code 12345678 at step 3")
	if strings.Contains(got, "31612345678") || strings.Contains(got, "12345678") || strings.Contains(got, "@") {
		t.Fatalf("identifiers survived: %q", got)
	}
	if !strings.Contains(got, "step 3") {
		t.Fatalf("short numbers should stay: %q", got)
	}
}

func TestPendingAccounts(t *testing.T) {
	ts := func(v int64) *int64 { return &v }
	accs := []accountView{
		{Account: "acc_A", LoggedIn: true, State: "CONNECTED", StateTs: ts(2000)}, // fresh and connected
		{Account: "acc_B", LoggedIn: true, State: "CONNECTED", StateTs: ts(500)},  // stale state from the last run
		{Account: "acc_C", LoggedIn: false, State: "CONNECTED", StateTs: ts(2000)},
		{Account: "acc_D", LoggedIn: true, State: "CONNECTING", StateTs: ts(2000)},
	}
	if n := pendingAccounts(accs, 1000); n != 3 {
		t.Fatalf("pending %d, want 3", n)
	}
	if n := pendingAccounts(nil, 1000); n != 0 {
		t.Fatalf("pending %d for no accounts, want 0", n)
	}
}

var errFake = errors.New("fake")

func TestWriteQRIsPrivatePNG(t *testing.T) {
	path := filepath.Join(t.TempDir(), "qr.png")
	for _, data := range []string{"2@fake-ref,fake-noise,fake-identity,fake-adv", "2@rotated-ref,fake-noise,fake-identity,fake-adv"} {
		if err := writeQR(data, path); err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.HasPrefix(b, []byte("\x89PNG\r\n\x1a\n")) {
			t.Fatal("not a PNG")
		}
		if bytes.Contains(b, []byte("fake-")) {
			t.Fatal("the QR payload is stored as text in the PNG")
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("mode %o, want 600", info.Mode().Perm())
		}
	}
	if entries, _ := os.ReadDir(filepath.Dir(path)); len(entries) != 1 {
		t.Fatalf("%d files left in the QR folder, want 1", len(entries))
	}
	if err := writeQR("", path); err == nil {
		t.Fatal("empty QR data accepted")
	}
}
