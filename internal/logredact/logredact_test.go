package logredact

import (
	"bytes"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

func TestRedactsKeysAndScrubsStrings(t *testing.T) {
	var out bytes.Buffer
	w := New(&out)
	log := zerolog.New(w)
	log.Info().
		Str("body", "CAPPY-CANARY-body").
		Str("user_login_id", "15550000001").
		Any("portal_key", map[string]string{"id": "15550000002@s.whatsapp.net", "receiver": "15550000001"}).
		Str("method", "chats.list").
		Int64("time_ms", 1763050000000).
		Msg("Handled event from @whatsapp_15550000003:cappy.local in chat 15550000004@g.us with +1 555 000 0005")
	log.Info().Str("qr", "2@CAPPY-CANARY-qr,abc,def").Msg("plain")
	got := out.String()
	for _, leak := range []string{"CAPPY-CANARY", "15550000001", "15550000002", "15550000003", "15550000004", "555 000 0005"} {
		if strings.Contains(got, leak) {
			t.Fatalf("leaked %q in %s", leak, got)
		}
	}
	for _, keep := range []string{`"method":"chats.list"`, `"time_ms":1763050000000`, "redacted:", "[ghost]", "[jid]"} {
		if !strings.Contains(got, keep) {
			t.Fatalf("expected %q in %s", keep, got)
		}
	}
	// Same value, same tag (correlation), across lines.
	out.Reset()
	log.Info().Str("user_login_id", "x").Msg("a")
	first := out.String()
	out.Reset()
	log.Info().Str("user_login_id", "x").Msg("a")
	if first != out.String() {
		t.Fatal("tags are not stable")
	}
}

func TestNonJSONLines(t *testing.T) {
	var out bytes.Buffer
	w := New(&out)
	_, _ = w.Write([]byte("panic: something with 15550000009@s.whatsapp.net\n"))
	if strings.Contains(out.String(), "15550000009") {
		t.Fatal("non-JSON line not scrubbed")
	}
}
