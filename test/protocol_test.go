// Package test runs the real cappy-engine binary as a child process and talks to it over stdin/stdout, the way the
// Rust host will. Nothing here signs in anywhere. Only the opt-in WhatsApp QR smoke test (CAPPY_WA_QR_SMOKE=1)
// contacts a network: it fetches an unauthenticated pairing QR from WhatsApp, asserts its shape, and cancels. The QR
// content is never printed, rendered or saved.
package test

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var engineBin string

func repoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Dir(filepath.Dir(file))
}

func TestMain(m *testing.M) {
	code := func() int {
		if bin := os.Getenv("CAPPY_ENGINE_BIN"); bin != "" {
			engineBin = bin
			return m.Run()
		}
		dir := filepath.Join(repoRoot(), "tmp.noindex", "testbin")
		if err := os.MkdirAll(dir, 0o700); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		engineBin = filepath.Join(dir, "cappy-engine")
		goBin := filepath.Join(runtime.GOROOT(), "bin", "go")
		cmd := exec.Command(goBin, "build", "-tags", "sqlite_fts5", "-o", engineBin, "./cmd/cappy-engine")
		cmd.Dir = repoRoot()
		cmd.Env = append(os.Environ(), "CGO_ENABLED=1")
		cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
		if err := cmd.Run(); err != nil {
			fmt.Fprintln(os.Stderr, "build failed:", err)
			return 1
		}
		return m.Run()
	}()
	os.Exit(code)
}

// dataDir is a fresh folder under tmp.noindex (which Spotlight skips), removed after the test.
func dataDir(t *testing.T) string {
	t.Helper()
	root := filepath.Join(repoRoot(), "tmp.noindex")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(root, "run-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

type rpcMsg struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code int `json:"code"`
		Data struct {
			Reason string `json:"reason"`
		} `json:"data"`
	} `json:"error"`
}

type proc struct {
	t      *testing.T
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stderr *bytes.Buffer
	exited chan error

	mu      sync.Mutex
	nextID  int
	pending map[string]chan rpcMsg
	notes   []rpcMsg
	badLine bool
}

func spawn(t *testing.T, dir string) *proc {
	t.Helper()
	cmd := exec.Command(engineBin, "--data-dir", dir)
	cmd.Env = []string{"HOME=" + os.Getenv("HOME"), "TMPDIR=" + os.TempDir(), "GOMEMLIMIT=256MiB", "PATH=/usr/bin:/bin"}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	p := &proc{t: t, cmd: cmd, stdin: stdin, stderr: &bytes.Buffer{}, exited: make(chan error, 1), pending: map[string]chan rpcMsg{}}
	cmd.Stderr = &lockedWriter{w: p.stderr}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		sc := bufio.NewScanner(stdout)
		sc.Buffer(make([]byte, 1<<20), 16<<20)
		for sc.Scan() {
			var m rpcMsg
			if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
				p.mu.Lock()
				p.badLine = true
				p.mu.Unlock()
				continue
			}
			p.mu.Lock()
			if m.Method != "" {
				p.notes = append(p.notes, m)
			} else if ch := p.pending[string(m.ID)]; ch != nil {
				delete(p.pending, string(m.ID))
				ch <- m
			}
			p.mu.Unlock()
		}
		p.exited <- cmd.Wait()
	}()
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
		}
	})
	return p
}

type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(b []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(b)
}

func (p *proc) writeLine(s string) {
	p.t.Helper()
	if _, err := io.WriteString(p.stdin, s+"\n"); err != nil {
		p.t.Fatalf("write to engine: %v", err)
	}
}

func (p *proc) call(method string, params any, timeout time.Duration) rpcMsg {
	p.t.Helper()
	p.mu.Lock()
	p.nextID++
	id := strconv.Itoa(p.nextID)
	ch := make(chan rpcMsg, 1)
	p.pending[id] = ch
	p.mu.Unlock()
	line, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": p.nextID, "method": method, "params": params})
	p.writeLine(string(line))
	select {
	case m := <-ch:
		return m
	case <-time.After(timeout):
		p.t.Fatalf("timed out waiting for %s", method)
	}
	return rpcMsg{}
}

// rawCall sends a raw line and returns the next response with a null id.
func (p *proc) rawNullIDResponse(line string) rpcMsg {
	p.t.Helper()
	ch := make(chan rpcMsg, 1)
	p.mu.Lock()
	p.pending["null"] = ch
	p.mu.Unlock()
	p.writeLine(line)
	select {
	case m := <-ch:
		return m
	case <-time.After(5 * time.Second):
		p.t.Fatal("timed out waiting for an error response")
	}
	return rpcMsg{}
}

func (p *proc) waitNote(method string, pred func(json.RawMessage) bool, timeout time.Duration) json.RawMessage {
	p.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		p.mu.Lock()
		for _, n := range p.notes {
			if n.Method == method && (pred == nil || pred(n.Params)) {
				p.mu.Unlock()
				return n.Params
			}
		}
		p.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
	p.t.Fatalf("timed out waiting for notification %s", method)
	return nil
}

func (p *proc) waitExit(timeout time.Duration) (int, time.Duration) {
	p.t.Helper()
	start := time.Now()
	select {
	case err := <-p.exited:
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return ee.ExitCode(), time.Since(start)
		} else if err != nil {
			p.t.Fatalf("wait: %v", err)
		}
		return 0, time.Since(start)
	case <-time.After(timeout):
		p.t.Fatalf("engine did not exit within %s", timeout)
	}
	return -1, 0
}

func (p *proc) hello() {
	p.t.Helper()
	m := p.call("hello", map[string]any{"protocol": 1, "client": map[string]string{"name": "protocol-test", "version": "0"}}, 60*time.Second)
	if m.Error != nil {
		p.t.Fatalf("hello failed: %d %s", m.Error.Code, m.Error.Data.Reason)
	}
}

// sockets lists the engine's open internet sockets via lsof (empty if none). listenOnly limits it to listening TCP
// sockets.
func sockets(t *testing.T, pid int, listenOnly bool) string {
	t.Helper()
	args := []string{"-nP", "-a", "-p", strconv.Itoa(pid)}
	if listenOnly {
		args = append(args, "-iTCP", "-sTCP:LISTEN")
	} else {
		args = append(args, "-i")
	}
	out, err := exec.Command("/usr/sbin/lsof", args...).Output()
	var ee *exec.ExitError
	if err != nil && !(errors.As(err, &ee) && ee.ExitCode() == 1) { // lsof exits 1 when nothing matches
		t.Fatalf("lsof: %v", err)
	}
	return strings.TrimSpace(string(out))
}

func udpSockets(t *testing.T, pid int) string {
	t.Helper()
	out, _ := exec.Command("/usr/sbin/lsof", "-nP", "-a", "-p", strconv.Itoa(pid), "-iUDP").Output()
	return strings.TrimSpace(string(out))
}

func TestProtocol(t *testing.T) {
	dir := dataDir(t)
	p := spawn(t, dir)

	// Before hello.
	if m := p.call("chats.list", map[string]any{}, 5*time.Second); m.Error == nil || m.Error.Code != 1000 {
		t.Fatal("expected not_ready before hello")
	}
	if m := p.call("ping", map[string]any{}, 5*time.Second); m.Error != nil {
		t.Fatal("ping failed before hello")
	}
	p.hello()

	var nets struct {
		Networks []struct {
			Network string  `json:"network"`
			State   string  `json:"state"`
			Reason  *string `json:"reason"`
			Flows   []struct {
				ID string `json:"id"`
			} `json:"flows"`
		} `json:"networks"`
	}
	m := p.call("networks.list", map[string]any{}, 5*time.Second)
	if m.Error != nil || json.Unmarshal(m.Result, &nets) != nil || len(nets.Networks) != 2 {
		t.Fatalf("networks.list failed")
	}
	byID := map[string]int{}
	for i, n := range nets.Networks {
		byID[n.Network] = i
	}
	wa, tg := nets.Networks[byID["whatsapp"]], nets.Networks[byID["telegram"]]
	if wa.State != "ready" || len(wa.Flows) != 2 {
		t.Fatalf("whatsapp: state %s, %d flows", wa.State, len(wa.Flows))
	}
	// T5: Telegram is compiled in and registered, but not configured (no api_id), with its four login flows.
	if tg.State != "needs_configuration" || tg.Reason == nil || *tg.Reason != "api_credentials_missing" || len(tg.Flows) != 4 {
		t.Fatalf("telegram: state %s, %d flows", tg.State, len(tg.Flows))
	}
	t.Logf("networks: whatsapp=%s telegram=%s (%d flows)", wa.State, tg.State, len(tg.Flows))

	for _, method := range []string{"accounts.list", "chats.list"} {
		m = p.call(method, map[string]any{}, 5*time.Second)
		if m.Error != nil || (!strings.Contains(string(m.Result), `"accounts":[]`) && !strings.Contains(string(m.Result), `"chats":[]`)) {
			t.Fatalf("%s not empty", method)
		}
	}
	// Telegram login is refused before anything is contacted.
	if m = p.call("login.start", map[string]any{"network": "telegram", "flow": "qr"}, 5*time.Second); m.Error == nil || m.Error.Code != 1002 {
		t.Fatal("expected 1002 network_not_configured for telegram")
	}
	if m = p.call("no.such.method", map[string]any{}, 5*time.Second); m.Error == nil || m.Error.Code != -32601 {
		t.Fatal("expected -32601")
	}
	if m = p.call("chats.list", map[string]any{"limit": "ten"}, 5*time.Second); m.Error == nil || m.Error.Code != -32602 {
		t.Fatal("expected -32602 for bad params")
	}
	if m = p.call("messages.list", map[string]any{"chat": "cht_NOPE"}, 5*time.Second); m.Error == nil || m.Error.Code != 1010 {
		t.Fatal("expected 1010 chat_not_found")
	}
	if m = p.rawNullIDResponse(`{"jsonrpc":"2.0","id":99,"method":`); m.Error == nil || m.Error.Code != -32700 {
		t.Fatal("expected -32700 for a malformed line")
	}
	if m = p.call("ping", map[string]any{}, 5*time.Second); m.Error != nil {
		t.Fatal("engine did not survive a malformed line")
	}

	// T3: no listening sockets, and with no accounts no network connections at all.
	pid := p.cmd.Process.Pid
	if s := sockets(t, pid, true); s != "" {
		t.Fatalf("engine has a listening socket:\n%s", s)
	}
	if s := sockets(t, pid, false); s != "" {
		t.Fatalf("engine has network sockets with no accounts:\n%s", s)
	}
	if s := udpSockets(t, pid); s != "" {
		t.Fatalf("engine has UDP sockets:\n%s", s)
	}

	// T6: idle stats.
	var stats map[string]any
	m = p.call("engine.stats", map[string]any{}, 5*time.Second)
	_ = json.Unmarshal(m.Result, &stats)
	t.Logf("idle engine.stats: rss=%.1f MiB heap=%.1f MiB goroutines=%v limit=%.0f MiB",
		stats["rssBytes"].(float64)/(1<<20), stats["goHeapBytes"].(float64)/(1<<20), stats["goroutines"], stats["memoryLimitBytes"].(float64)/(1<<20))
	if out, err := exec.Command("/bin/ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output(); err == nil {
		kb, _ := strconv.Atoi(strings.TrimSpace(string(out)))
		t.Logf("idle ps rss: %.1f MiB", float64(kb)/1024)
	}

	// A second engine on the same folder exits 3 (locked).
	second := exec.Command(engineBin, "--data-dir", dir)
	second.Stdin = strings.NewReader("")
	err := second.Run()
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 3 {
		t.Fatalf("expected exit 3 for a locked data folder, got %v", err)
	}

	if m = p.call("shutdown", map[string]any{}, 5*time.Second); m.Error != nil {
		t.Fatal("shutdown failed")
	}
	code, took := p.waitExit(10 * time.Second)
	if code != 0 {
		t.Fatalf("shutdown exit code %d", code)
	}
	t.Logf("shutdown took %s", took.Round(time.Millisecond))
	p.mu.Lock()
	bad := p.badLine
	p.mu.Unlock()
	if bad {
		t.Fatal("engine wrote a non-JSON line to stdout")
	}

	// stdin EOF (the host died) also stops cleanly.
	p2 := spawn(t, dir)
	p2.hello()
	_ = p2.stdin.Close()
	if code, _ = p2.waitExit(10 * time.Second); code != 0 {
		t.Fatalf("stdin EOF exit code %d", code)
	}

	// SIGKILL, then a restart on the same folder works (the kernel releases the lock).
	p3 := spawn(t, dir)
	p3.hello()
	_ = p3.cmd.Process.Signal(syscall.SIGKILL)
	p3.waitExit(5 * time.Second)
	p4 := spawn(t, dir)
	p4.hello()
	// SIGTERM stops gracefully.
	_ = p4.cmd.Process.Signal(syscall.SIGTERM)
	if code, _ = p4.waitExit(10 * time.Second); code != 0 {
		t.Fatalf("SIGTERM exit code %d", code)
	}
}

func TestBadUsage(t *testing.T) {
	for _, args := range [][]string{{}, {"--data-dir", "relative/path"}, {"--data-dir", "/tmp/x", "--log-level", "loud"}} {
		cmd := exec.Command(engineBin, args...)
		err := cmd.Run()
		var ee *exec.ExitError
		if !errors.As(err, &ee) || ee.ExitCode() != 2 {
			t.Fatalf("args %v: expected exit 2, got %v", args, err)
		}
	}
}

// TestForeignInboxExits4 runs the real binary on a data folder whose inbox.db is not Cappy Engine's. It must exit 4
// and leave the file exactly as it was.
func TestForeignInboxExits4(t *testing.T) {
	for name, setup := range map[string]func(path string) error{
		"foreign sqlite": func(path string) error {
			db, err := sql.Open("sqlite3", "file:"+path)
			if err != nil {
				return err
			}
			defer db.Close()
			for _, s := range []string{`CREATE TABLE foreign_thing (x INTEGER)`, `INSERT INTO foreign_thing VALUES (1)`, `PRAGMA user_version=77`} {
				if _, err = db.Exec(s); err != nil {
					return err
				}
			}
			return nil
		},
		"not sqlite": func(path string) error {
			return os.WriteFile(path, bytes.Repeat([]byte("plain text, not a database\n"), 200), 0o600)
		},
	} {
		t.Run(name, func(t *testing.T) {
			dir := dataDir(t)
			path := filepath.Join(dir, "inbox.db")
			if err := setup(path); err != nil {
				t.Fatal(err)
			}
			before, _ := os.ReadFile(path)
			cmd := exec.Command(engineBin, "--data-dir", dir)
			cmd.Stdin = strings.NewReader("")
			err := cmd.Run()
			var ee *exec.ExitError
			if !errors.As(err, &ee) || ee.ExitCode() != 4 {
				t.Fatalf("expected exit 4, got %v", err)
			}
			if after, _ := os.ReadFile(path); !bytes.Equal(before, after) {
				t.Fatal("the foreign inbox.db was modified")
			}
			for _, side := range []string{"inbox.db-wal", "bridge.db"} {
				if _, err := os.Stat(filepath.Join(dir, side)); err == nil {
					t.Fatalf("%s was created next to a foreign inbox.db", side)
				}
			}
		})
	}
}
