package rpc

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

type harness struct {
	t    *testing.T
	srv  *Server
	inW  *io.PipeWriter
	out  *bufio.Reader
	done chan error
}

func newHarness(t *testing.T, register func(s *Server)) *harness {
	t.Helper()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	srv := NewServer(inR, outW, zerolog.Nop())
	if register != nil {
		register(srv)
	}
	h := &harness{t: t, srv: srv, inW: inW, out: bufio.NewReaderSize(outR, 1<<20), done: make(chan error, 1)}
	go func() { h.done <- srv.Serve(context.Background()) }()
	t.Cleanup(func() {
		_ = inW.Close()
		_ = outR.Close()
	})
	return h
}

func (h *harness) send(line string) {
	h.t.Helper()
	if _, err := io.WriteString(h.inW, line+"\n"); err != nil {
		h.t.Fatal(err)
	}
}

type msg struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	Result  json.RawMessage `json:"result"`
	Error   *Error          `json:"error"`
}

func (h *harness) read() msg {
	h.t.Helper()
	type res struct {
		line string
		err  error
	}
	ch := make(chan res, 1)
	go func() {
		line, err := h.out.ReadString('\n')
		ch <- res{line, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			h.t.Fatalf("read: %v", r.err)
		}
		var m msg
		if err := json.Unmarshal([]byte(r.line), &m); err != nil {
			h.t.Fatalf("output is not one JSON object per line: %v", err)
		}
		if m.JSONRPC != "2.0" {
			h.t.Fatalf("missing jsonrpc 2.0")
		}
		return m
	case <-time.After(5 * time.Second):
		h.t.Fatal("timed out waiting for output")
	}
	return msg{}
}

type echoParams struct {
	Text  string `json:"text"`
	Count int    `json:"count"`
}

func register(s *Server) {
	s.Handle("echo", func(ctx context.Context, params json.RawMessage) (any, error) {
		var p echoParams
		if err := Decode(params, &p); err != nil {
			return nil, err
		}
		if p.Count < 0 {
			return nil, InvalidParams("count must not be negative")
		}
		return map[string]any{"text": p.Text, "count": p.Count}, nil
	})
	s.Handle("fail", func(ctx context.Context, params json.RawMessage) (any, error) {
		return nil, NewError(1010, "chat_not_found", "no such chat")
	})
	s.Handle("boom", func(ctx context.Context, params json.RawMessage) (any, error) {
		panic("secret-value-should-not-leak")
	})
	s.Handle("internal", func(ctx context.Context, params json.RawMessage) (any, error) {
		return nil, io.ErrUnexpectedEOF
	})
}

func TestFramingAndEcho(t *testing.T) {
	h := newHarness(t, register)
	h.send(`{"jsonrpc":"2.0","id":1,"method":"echo","params":{"text":"hi","count":2}}`)
	m := h.read()
	if string(m.ID) != "1" || m.Error != nil {
		t.Fatalf("unexpected response: id=%s err=%v", m.ID, m.Error)
	}
	var res echoParams
	if err := json.Unmarshal(m.Result, &res); err != nil || res.Text != "hi" || res.Count != 2 {
		t.Fatalf("bad result: %s", m.Result)
	}
	// String ids are echoed verbatim; blank lines are ignored; CRLF is tolerated.
	h.send("")
	h.send(`{"jsonrpc":"2.0","id":"abc","method":"echo"}` + "\r")
	m = h.read()
	if string(m.ID) != `"abc"` || m.Error != nil {
		t.Fatalf("unexpected response: id=%s err=%v", m.ID, m.Error)
	}
}

func TestUnknownMethod(t *testing.T) {
	h := newHarness(t, register)
	h.send(`{"jsonrpc":"2.0","id":7,"method":"nope","params":{}}`)
	m := h.read()
	if m.Error == nil || m.Error.Code != CodeMethodNotFound || string(m.ID) != "7" {
		t.Fatalf("expected -32601 for id 7, got %+v", m)
	}
	// A notification (no id) for an unknown method gets no response: the next output is for the next request.
	h.send(`{"jsonrpc":"2.0","method":"nope"}`)
	h.send(`{"jsonrpc":"2.0","id":8,"method":"echo"}`)
	if m = h.read(); string(m.ID) != "8" {
		t.Fatalf("expected the response to id 8, got %+v", m)
	}
}

func TestBadParams(t *testing.T) {
	h := newHarness(t, register)
	cases := []string{
		`{"jsonrpc":"2.0","id":1,"method":"echo","params":{"count":"two"}}`,
		`{"jsonrpc":"2.0","id":1,"method":"echo","params":{"unknown":1}}`,
		`{"jsonrpc":"2.0","id":1,"method":"echo","params":[1,2]}`,
		`{"jsonrpc":"2.0","id":1,"method":"echo","params":{"count":-1}}`,
	}
	for _, c := range cases {
		h.send(c)
		m := h.read()
		if m.Error == nil || m.Error.Code != CodeInvalidParams {
			t.Fatalf("expected -32602, got %+v", m)
		}
	}
}

func TestMalformedAndInvalid(t *testing.T) {
	h := newHarness(t, register)
	h.send(`{"jsonrpc":"2.0","id":1,"method":`)
	if m := h.read(); m.Error == nil || m.Error.Code != CodeParseError || string(m.ID) != "null" {
		t.Fatalf("expected -32700 with null id, got %+v", m)
	}
	h.send(`{"jsonrpc":"1.0","id":2,"method":"echo"}`)
	if m := h.read(); m.Error == nil || m.Error.Code != CodeInvalidRequest {
		t.Fatalf("expected -32600, got %+v", m)
	}
	h.send(`[{"jsonrpc":"2.0","id":3,"method":"echo"}]`)
	if m := h.read(); m.Error == nil || m.Error.Code != CodeInvalidRequest {
		t.Fatalf("expected -32600 for batch, got %+v", m)
	}
	h.send(`{"jsonrpc":"2.0","id":{"x":1},"method":"echo"}`)
	if m := h.read(); m.Error == nil || m.Error.Code != CodeInvalidRequest {
		t.Fatalf("expected -32600 for object id, got %+v", m)
	}
	// The server survives all of the above.
	h.send(`{"jsonrpc":"2.0","id":4,"method":"echo"}`)
	if m := h.read(); m.Error != nil || string(m.ID) != "4" {
		t.Fatalf("server did not survive: %+v", m)
	}
}

func TestEngineErrorsPanicsAndInternal(t *testing.T) {
	h := newHarness(t, register)
	h.send(`{"jsonrpc":"2.0","id":1,"method":"fail"}`)
	if m := h.read(); m.Error == nil || m.Error.Code != 1010 || m.Error.Data.Reason != "chat_not_found" {
		t.Fatalf("expected engine error 1010, got %+v", m)
	}
	h.send(`{"jsonrpc":"2.0","id":2,"method":"boom"}`)
	m := h.read()
	if m.Error == nil || m.Error.Code != CodeInternalError {
		t.Fatalf("expected -32603 after panic, got %+v", m)
	}
	if strings.Contains(m.Error.Message, "secret") {
		t.Fatal("panic value leaked into the error")
	}
	h.send(`{"jsonrpc":"2.0","id":3,"method":"internal"}`)
	if m := h.read(); m.Error == nil || m.Error.Code != CodeInternalError || strings.Contains(m.Error.Message, "EOF") {
		t.Fatalf("expected generic -32603, got %+v", m)
	}
}

func TestOversizedLine(t *testing.T) {
	h := newHarness(t, register)
	big := `{"jsonrpc":"2.0","id":1,"method":"echo","params":{"text":"` + strings.Repeat("a", MaxLineBytes) + `"}}`
	go h.send(big)
	if m := h.read(); m.Error == nil || m.Error.Code != CodeInvalidRequest || m.Error.Data.Reason != "too_large" {
		t.Fatalf("expected -32600 too_large, got %+v", m.Error)
	}
	h.send(`{"jsonrpc":"2.0","id":2,"method":"echo","params":{"text":"ok"}}`)
	if m := h.read(); m.Error != nil || string(m.ID) != "2" {
		t.Fatalf("server did not survive an oversized line: %+v", m)
	}
}

func TestGateAndNotifications(t *testing.T) {
	ready := false
	h := newHarness(t, func(s *Server) {
		register(s)
		s.Gate = func(method string) *Error {
			if !ready && method != "echo" {
				return NewError(1000, "not_ready", "send hello first")
			}
			return nil
		}
	})
	h.send(`{"jsonrpc":"2.0","id":1,"method":"fail"}`)
	if m := h.read(); m.Error == nil || m.Error.Code != 1000 {
		t.Fatalf("expected gate error 1000, got %+v", m)
	}
	h.srv.Notify("chat.updated", map[string]string{"chat": "cht_X", "reason": "activity"})
	m := h.read()
	if m.Method != "chat.updated" || len(m.ID) != 0 && string(m.ID) != "null" {
		t.Fatalf("expected a notification, got %+v", m)
	}
}

func TestNotificationOverflowSendsResync(t *testing.T) {
	inR, _ := io.Pipe()
	outR, outW := io.Pipe()
	srv := NewServer(inR, outW, zerolog.Nop())
	// Nothing reads the output yet, so the writer blocks on the first line and the queue fills up.
	for i := 0; i < NotifyQueueLen+50; i++ {
		srv.Notify("message.new", map[string]int{"i": i})
	}
	r := bufio.NewReader(outR)
	sawResync := false
	count := 0
	deadline := time.After(10 * time.Second)
	for !sawResync {
		lineCh := make(chan string, 1)
		go func() {
			line, _ := r.ReadString('\n')
			lineCh <- line
		}()
		select {
		case line := <-lineCh:
			var m msg
			if err := json.Unmarshal([]byte(line), &m); err != nil {
				t.Fatal(err)
			}
			if m.Method == "engine.resync" {
				sawResync = true
			} else {
				count++
			}
		case <-deadline:
			t.Fatalf("no engine.resync after %d notifications", count)
		}
	}
	if count > NotifyQueueLen+1 {
		t.Fatalf("expected drops, but %d notifications were delivered", count)
	}
}
