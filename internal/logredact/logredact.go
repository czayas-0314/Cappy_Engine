// Package logredact is an io.Writer for zerolog (and Go's std log) that removes personal data from log lines before
// they reach stderr.
//
// Each line is parsed as JSON. Deny-listed keys get "redacted:<8 hex of HMAC(key, value)>" so values can still be
// correlated across lines, and every remaining string is scrubbed for WhatsApp JIDs, phone-like digit runs and ghost
// Matrix IDs. Lines that aren't JSON are scrubbed as text.
package logredact

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"regexp"
	"sync"
)

var denyKeys = map[string]bool{}

func init() {
	for _, k := range []string{
		"body", "text", "caption", "formatted_body", "name", "displayname", "remote_name", "push_name",
		"business_name", "full_name", "first_name", "last_name", "username", "phone", "phone_number",
		"user_login_id", "login_id", "remote_id", "jid", "chat_jid", "sender", "sender_id", "sender_jid",
		"ghost_id", "user_id", "portal_id", "portal_receiver", "portal_key", "receiver", "event_data", "qr",
		"code", "url", "topic", "title", "query", "input", "id", "chat_id", "room_name", "target", "data",
		"remote_profile", "identifiers", "api_hash", "password", "token", "access_token",
	} {
		denyKeys[k] = true
	}
}

var scrubbers = []struct {
	re   *regexp.Regexp
	repl string
}{
	{regexp.MustCompile(`[0-9A-Za-z.:_\-]+@(s\.whatsapp\.net|lid|g\.us|newsletter|broadcast|c\.us)`), "[jid]"},
	{regexp.MustCompile(`@(whatsapp|telegram)_[^\s"',:]+(:cappy\.local)?`), "[ghost]"},
	{regexp.MustCompile(`\+?\d[\d\- ]{5,}\d`), "[digits]"},
}

// Scrub removes JIDs, ghost IDs and phone-like digit runs from s.
func Scrub(s string) string {
	for _, sc := range scrubbers {
		s = sc.re.ReplaceAllString(s, sc.repl)
	}
	return s
}

type Writer struct {
	out io.Writer
	mu  sync.Mutex
	key []byte
}

func New(out io.Writer) *Writer {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	return &Writer{out: out, key: key}
}

// SetKey switches correlation hashes to the install key, so they are stable across restarts.
func (w *Writer) SetKey(key []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.key = append([]byte(nil), key...)
}

func (w *Writer) tag(v any) string {
	var data []byte
	if s, ok := v.(string); ok {
		data = []byte(s)
	} else {
		data, _ = json.Marshal(v)
	}
	h := hmac.New(sha256.New, w.key)
	h.Write(data)
	return "redacted:" + hex.EncodeToString(h.Sum(nil))[:8]
}

func (w *Writer) Write(p []byte) (int, error) {
	n := len(p)
	out := w.Redact(p)
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err := w.out.Write(out)
	return n, err
}

// Redact returns the redacted form of one or more log lines.
func (w *Writer) Redact(p []byte) []byte {
	var buf bytes.Buffer
	for _, line := range bytes.SplitAfter(p, []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		trimmed := bytes.TrimRight(line, "\r\n")
		var obj map[string]any
		dec := json.NewDecoder(bytes.NewReader(trimmed))
		dec.UseNumber()
		if len(trimmed) > 0 && trimmed[0] == '{' && dec.Decode(&obj) == nil {
			w.mu.Lock()
			red := w.redactValue(obj, false)
			w.mu.Unlock()
			data, err := json.Marshal(red)
			if err == nil {
				buf.Write(data)
				buf.WriteByte('\n')
				continue
			}
		}
		buf.WriteString(Scrub(string(trimmed)))
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}

func (w *Writer) redactValue(v any, deny bool) any {
	if deny {
		if v == nil {
			return nil
		}
		return w.tag(v)
	}
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			t[k] = w.redactValue(val, denyKeys[k])
		}
		return t
	case []any:
		for i, val := range t {
			t[i] = w.redactValue(val, false)
		}
		return t
	case string:
		return Scrub(t)
	default:
		return v
	}
}
