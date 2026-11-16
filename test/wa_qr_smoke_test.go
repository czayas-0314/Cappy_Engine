package test

import (
	"encoding/json"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestWhatsAppQRSmoke (T4) is opt-in: CAPPY_WA_QR_SMOKE=1. It is the only test that contacts a network. It asks
// WhatsApp for an unauthenticated pairing QR code through the real engine binary, checks that the first login step
// arrives with the expected shape, cancels the login and shuts down. The QR data is never printed, rendered or saved;
// only its step type, display type and the number of comma-separated fields are reported.
func TestWhatsAppQRSmoke(t *testing.T) {
	if os.Getenv("CAPPY_WA_QR_SMOKE") != "1" {
		t.Skip("set CAPPY_WA_QR_SMOKE=1 to run the WhatsApp QR shape check (contacts WhatsApp, signs in to nothing)")
	}
	dir := dataDir(t)
	p := spawn(t, dir)
	p.hello()

	m := p.call("login.start", map[string]any{"network": "whatsapp", "flow": "qr"}, 60*time.Second)
	if m.Error != nil {
		t.Fatalf("login.start failed: code %d reason %s", m.Error.Code, m.Error.Data.Reason)
	}
	var res struct {
		LoginID string `json:"loginID"`
		Step    struct {
			Type           string `json:"type"`
			StepID         string `json:"step_id"`
			DisplayAndWait *struct {
				Type string `json:"type"`
				Data string `json:"data"`
			} `json:"display_and_wait"`
		} `json:"step"`
	}
	if err := json.Unmarshal(m.Result, &res); err != nil {
		t.Fatal("login.start result is not the expected shape")
	}
	step := res.Step
	if step.Type != "display_and_wait" || step.DisplayAndWait == nil {
		t.Fatalf("first step type %q, want display_and_wait", step.Type)
	}
	qr := step.DisplayAndWait.Data
	fields := len(strings.Split(qr, ","))
	t.Logf("first login step arrived: type=%s step_id=%s display=%s data_present=%v comma_fields=%d",
		step.Type, step.StepID, step.DisplayAndWait.Type, qr != "", fields)
	if step.StepID != "fi.mau.whatsapp.login.qr" || step.DisplayAndWait.Type != "qr" || qr == "" || fields < 4 {
		t.Fatalf("unexpected QR step shape: step_id=%s display=%s fields=%d", step.StepID, step.DisplayAndWait.Type, fields)
	}

	// T3 during a live login: still no listening sockets.
	pid := p.cmd.Process.Pid
	if s := sockets(t, pid, true); s != "" {
		t.Fatal("engine has a listening socket during login")
	}
	var stats map[string]any
	_ = json.Unmarshal(p.call("engine.stats", map[string]any{}, 5*time.Second).Result, &stats)
	t.Logf("engine.stats during QR step: rss=%.1f MiB goroutines=%v", stats["rssBytes"].(float64)/(1<<20), stats["goroutines"])

	m = p.call("login.cancel", map[string]any{"loginID": res.LoginID}, 10*time.Second)
	if m.Error != nil || !strings.Contains(string(m.Result), `"cancelled":true`) {
		t.Fatal("login.cancel failed")
	}
	p.waitNote("login.step", func(params json.RawMessage) bool {
		return strings.Contains(string(params), `"status":"cancelled"`)
	}, 10*time.Second)
	t.Log("login cancelled; login.step status=cancelled received")

	m = p.call("accounts.list", map[string]any{}, 5*time.Second)
	if m.Error != nil || !strings.Contains(string(m.Result), `"accounts":[]`) {
		t.Fatal("an account exists after a cancelled login")
	}
	if m = p.call("shutdown", map[string]any{}, 5*time.Second); m.Error != nil {
		t.Fatal("shutdown failed")
	}
	code, took := p.waitExit(10 * time.Second)
	if code != 0 {
		t.Fatalf("exit code %d after shutdown", code)
	}
	t.Logf("clean shutdown in %s", took.Round(time.Millisecond))

	// The QR content must not be in the engine's logs.
	if strings.Contains(p.stderr.String(), qr) {
		t.Fatal("QR data leaked into stderr")
	}
	if out, _ := exec.Command("/bin/ps", "-p", strconv.Itoa(pid)).Output(); strings.Contains(string(out), strconv.Itoa(pid)+" ") {
		t.Fatal("engine process still running")
	}
}
