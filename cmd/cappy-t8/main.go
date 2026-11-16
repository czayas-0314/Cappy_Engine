// Command cappy-t8 is the owner-run helper for T8, the real-account WhatsApp test (DESIGN.md section 19). It spawns
// cappy-engine and talks JSON-RPC 2.0 to it over stdin/stdout, the way the Rust host will.
//
// It prints only content-free lines: account IDs and states, counts, memory and login progress. It never prints
// message text, names, phone numbers, chat titles or the QR payload. The QR code is written to a PNG file (mode 0600),
// opened with `open`, and deleted when the login ends. The engine's own (redacted) log goes to a file, not the
// terminal.
//
// Usage:
//
//	cappy-t8 [flags] login    show the WhatsApp QR as a PNG, wait for the scan, then keep running as watch
//	cappy-t8 [flags] watch    start the engine with the stored account and print a status line every 30 s
//	cappy-t8 [flags] status   print the accounts and the chat count once, then stop the engine
//	cappy-t8 [flags] send-test  find the one direct chat that received an incoming message in a time window and
//	                            (only with --dry-run=false --yes --chat ID) send it one test message; see sendtest.go
//
// Ctrl-C (or SIGTERM) cancels a login in progress, sends shutdown and waits for the engine to exit cleanly.
//
// Copyright (C) 2026 Cappy Comm contributors. AGPL-3.0-or-later; see LICENSE.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	qrcode "github.com/skip2/go-qrcode"
)

const (
	toolVersion = "0.1.0"
	qrPath      = "./tmp.noindex/t8-qr.png"
	pageLimit   = 200 // chats.list maximum
)

type options struct {
	engine       string
	dataDir      string
	engineLog    string
	interval     time.Duration
	loginTimeout time.Duration
	wait         time.Duration

	// send-test only
	text        string
	windowStart time.Time
	windowEnd   time.Time
	dryRun      bool
	yes         bool
	chat        string // the chat ID the dry run printed; required for a real send
}

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	opts, cmd, ok := parseArgs(args)
	if !ok {
		return 2
	}
	// Ctrl-C reaches only this process (the engine runs in its own process group), so the stop is always the
	// orderly shutdown request below, not a signal racing it.
	sigs := make(chan os.Signal, 4)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	// A closed stdout pipe (for example `| tee` going away) then fails writes instead of killing the tool before it
	// has stopped the engine.
	signal.Notify(make(chan os.Signal, 1), syscall.SIGPIPE)

	c, err := spawnEngine(opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cappy-t8:", err)
		return 1
	}
	defer c.cleanup()
	if err = c.hello(); err != nil {
		fmt.Fprintln(os.Stderr, "cappy-t8: hello failed:", err)
		c.stop()
		return 1
	}
	say("engine ready (pid %d)", c.cmd.Process.Pid)

	switch cmd {
	case "login":
		if code := runLogin(c, opts, sigs); code != 0 {
			return code
		}
		return runWatch(c, opts, sigs)
	case "watch":
		return runWatch(c, opts, sigs)
	case "send-test":
		return runSendTest(c, opts, sigs)
	default: // status
		return runStatus(c, opts, sigs)
	}
}

func parseArgs(args []string) (options, string, bool) {
	var o options
	fs := flag.NewFlagSet("cappy-t8", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.StringVar(&o.engine, "engine", "./dist/cappy-engine", "path of the cappy-engine binary")
	fs.StringVar(&o.dataDir, "data-dir", "./tmp.noindex/t8-data", "engine data folder (keep it out of Spotlight: .noindex)")
	fs.StringVar(&o.engineLog, "engine-log", "./tmp.noindex/t8-engine.log", "file that receives the engine's redacted stderr log")
	fs.DurationVar(&o.interval, "interval", 30*time.Second, "watch: time between status lines")
	fs.DurationVar(&o.loginTimeout, "login-timeout", 5*time.Minute, "login: give up if the QR is not scanned in this time")
	fs.DurationVar(&o.wait, "wait", 30*time.Second, "status, send-test: how long to wait for logged-in accounts to connect")
	var windowStart, windowEnd string
	fs.StringVar(&o.text, "text", defaultTestText, "send-test: the text of the one test message")
	fs.StringVar(&windowStart, "window-start", defaultWindowStart, "send-test: start of the window (RFC3339) in which the incoming message arrived")
	fs.StringVar(&windowEnd, "window-end", defaultWindowEnd, "send-test: end of the window (RFC3339)")
	fs.BoolVar(&o.dryRun, "dry-run", true, "send-test: only print the selected chat; --dry-run=false --yes sends")
	fs.BoolVar(&o.yes, "yes", false, "send-test: confirm the real send (needs --dry-run=false too)")
	fs.StringVar(&o.chat, "chat", "", "send-test: the chat ID (cht_...) the dry run printed; required with --dry-run=false --yes")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: cappy-t8 [flags] login|watch|status|send-test [flags]")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return o, "", false
	}
	if fs.NArg() == 0 {
		fs.Usage()
		return o, "", false
	}
	cmd := fs.Arg(0)
	// Flags may also follow the subcommand.
	if err := fs.Parse(fs.Args()[1:]); err != nil {
		return o, "", false
	}
	if fs.NArg() != 0 || (cmd != "login" && cmd != "watch" && cmd != "status" && cmd != "send-test") || o.interval < time.Second {
		fs.Usage()
		return o, "", false
	}
	if cmd != "send-test" {
		var stray []string
		fs.Visit(func(f *flag.Flag) {
			if sendTestFlags[f.Name] {
				stray = append(stray, "--"+f.Name)
			}
		})
		if len(stray) > 0 {
			fmt.Fprintf(os.Stderr, "cappy-t8: %s: flags for send-test only\n", strings.Join(stray, ", "))
			return o, "", false
		}
		return o, cmd, true
	}
	if err := o.setWindow(windowStart, windowEnd); err != nil {
		fmt.Fprintln(os.Stderr, "cappy-t8 send-test:", err)
		return o, "", false
	}
	if err := checkTestText(o.text); err != nil {
		fmt.Fprintln(os.Stderr, "cappy-t8 send-test:", err)
		return o, "", false
	}
	if !o.dryRun && !o.yes {
		fmt.Fprintln(os.Stderr, "cappy-t8 send-test: a real send needs --yes as well as --dry-run=false; nothing was sent")
		return o, "", false
	}
	chatSet := false
	fs.Visit(func(f *flag.Flag) { chatSet = chatSet || f.Name == "chat" })
	if chatSet && !validID(o.chat, "cht_") {
		fmt.Fprintln(os.Stderr, "cappy-t8 send-test: --chat is not a chat ID (cht_...); nothing was sent")
		return o, "", false
	}
	if !o.dryRun && o.yes && o.chat == "" {
		fmt.Fprintln(os.Stderr, "cappy-t8 send-test: a real send needs --chat with the chat ID that the dry run printed; nothing was sent")
		return o, "", false
	}
	return o, cmd, true
}

func say(format string, a ...any) {
	_, _ = fmt.Fprintf(os.Stdout, format+"\n", a...)
}

func stamp(t time.Time) string { return t.Format(time.RFC3339) }

// ---------------------------------------------------------------------------------------------------------------
// Engine process and JSON-RPC client

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		Reason   string `json:"reason"`
		Upstream string `json:"upstream"`
	} `json:"data"`
}

// Error reports only the code and the snake_case reasons, never free text.
func (e *rpcError) Error() string {
	s := fmt.Sprintf("engine error %d (%s)", e.Code, token(e.Data.Reason))
	if e.Data.Upstream != "" {
		s += " upstream " + token(e.Data.Upstream)
	}
	return s
}

type wireMsg struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
}

type client struct {
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	logFile *os.File
	started time.Time

	wmu     sync.Mutex
	mu      sync.Mutex
	nextID  int64
	pending map[string]chan wireMsg

	notes      chan wireMsg    // login.step, account.state, engine.resync
	msgNew     atomic.Int64    // message.new notifications since start
	wantStatus atomic.Bool     // send-test: route message.updated status changes to statuses
	statuses   chan statusNote // send-test: status changes of own messages (IDs and status only)
	readerDone chan struct{}   // stdout closed
	exited     chan struct{}   // process reaped
}

func spawnEngine(o options) (*client, error) {
	bin, err := filepath.Abs(o.engine)
	if err != nil {
		return nil, err
	}
	if info, err := os.Stat(bin); err != nil || info.IsDir() {
		return nil, fmt.Errorf("engine binary not found at %s (build it with ./scripts/build-universal.sh)", bin)
	}
	dataDir, err := filepath.Abs(o.dataDir)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, err
	}
	logPath, err := filepath.Abs(o.engineLog)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(filepath.Dir(logPath), 0o700); err != nil {
		return nil, err
	}
	logFile, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}

	cmd := exec.Command(bin, "--data-dir", dataDir)
	// Like the host: a cleared environment with only what the engine needs. Nothing secret goes on argv or env.
	cmd.Env = []string{"HOME=" + os.Getenv("HOME"), "TMPDIR=" + os.TempDir(), "GOMEMLIMIT=256MiB", "PATH=/usr/bin:/bin"}
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		logFile.Close()
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		logFile.Close()
		return nil, err
	}
	c := &client{
		cmd: cmd, stdin: stdin, logFile: logFile, started: time.Now(),
		pending: map[string]chan wireMsg{}, notes: make(chan wireMsg, 1024), statuses: make(chan statusNote, 64),
		readerDone: make(chan struct{}), exited: make(chan struct{}),
	}
	if err = cmd.Start(); err != nil {
		logFile.Close()
		return nil, err
	}
	say("engine: %s", bin)
	say("data folder: %s", dataDir)
	say("engine log: %s", logPath)
	go c.readLoop(stdout)
	return c, nil
}

func (c *client) readLoop(stdout io.Reader) {
	br := bufio.NewReaderSize(stdout, 64<<10)
	for {
		line, err := br.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) > 0 {
			c.dispatch(line)
		}
		if err != nil {
			break
		}
	}
	close(c.readerDone)
	_ = c.cmd.Wait() // the exit status is read from cmd.ProcessState
	close(c.exited)
}

func (c *client) dispatch(line []byte) {
	var m wireMsg
	if err := json.Unmarshal(line, &m); err != nil {
		return
	}
	if m.Method != "" {
		switch m.Method {
		case "message.new":
			c.msgNew.Add(1) // counted only; the message itself is never decoded
		case "message.updated":
			if !c.wantStatus.Load() {
				return // not decoded at all outside a send-test send
			}
			if n, ok := decodeStatusNote(m.Params); ok {
				select {
				case c.statuses <- n:
				default:
				}
			}
		case "login.step", "account.state", "engine.resync":
			select {
			case c.notes <- m:
			default: // never block the reader; these are rare, and 1024 are buffered
			}
		}
		return
	}
	c.mu.Lock()
	ch := c.pending[string(m.ID)]
	delete(c.pending, string(m.ID))
	c.mu.Unlock()
	if ch != nil {
		ch <- m
	}
}

func (c *client) call(method string, params any, timeout time.Duration) (json.RawMessage, error) {
	if params == nil {
		params = struct{}{}
	}
	c.mu.Lock()
	c.nextID++
	id := c.nextID
	key := strconv.FormatInt(id, 10)
	ch := make(chan wireMsg, 1)
	c.pending[key] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, key)
		c.mu.Unlock()
	}()
	line, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
	if err != nil {
		return nil, err
	}
	c.wmu.Lock()
	_, err = c.stdin.Write(append(line, '\n'))
	c.wmu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("%s: the engine is not reading its input", method)
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case m := <-ch:
		if m.Error != nil {
			return nil, m.Error
		}
		return m.Result, nil
	case <-c.readerDone:
		select {
		case m := <-ch:
			if m.Error != nil {
				return nil, m.Error
			}
			return m.Result, nil
		default:
		}
		return nil, fmt.Errorf("%s: the engine exited", method)
	case <-timer.C:
		return nil, fmt.Errorf("%s: no response within %s", method, timeout)
	}
}

func (c *client) hello() error {
	_, err := c.call("hello", map[string]any{
		"protocol": 1,
		"client":   map[string]string{"name": "cappy-t8", "version": toolVersion},
	}, 60*time.Second)
	if err != nil {
		select {
		case <-c.exited:
			return fmt.Errorf("%w; %s", err, exitDescription(c.cmd.ProcessState.ExitCode()))
		case <-time.After(500 * time.Millisecond):
		}
	}
	return err
}

// stop sends shutdown and waits for a clean exit, then falls back to SIGTERM and SIGKILL like the host's quit
// sequence. It returns the engine's exit code (-1 if it was killed by a signal).
func (c *client) stop() int {
	select {
	case <-c.exited:
		return c.cmd.ProcessState.ExitCode()
	default:
	}
	began := time.Now()
	if _, err := c.call("shutdown", nil, 5*time.Second); err != nil {
		say("shutdown request: %v", err)
	}
	select {
	case <-c.exited:
	case <-time.After(12 * time.Second): // the engine's own hard deadline is 10 s
		say("engine did not exit within 12s; sending SIGTERM")
		_ = c.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-c.exited:
		case <-time.After(3 * time.Second):
			say("engine did not exit after SIGTERM; killing it")
			_ = c.cmd.Process.Kill()
			<-c.exited
		}
	}
	code := c.cmd.ProcessState.ExitCode()
	say("engine stopped (exit %d, %s)", code, time.Since(began).Round(time.Millisecond))
	return code
}

func (c *client) cleanup() {
	select {
	case <-c.exited:
	default:
		_ = c.cmd.Process.Kill()
		<-c.exited
	}
	_ = c.stdin.Close()
	_ = c.logFile.Close()
}

func exitDescription(code int) string {
	switch code {
	case 2:
		return "engine exit 2 (bad usage)"
	case 3:
		return "engine exit 3 (the data folder is in use by another engine; is cappy-t8 watch still running?)"
	case 4:
		return "engine exit 4 (inbox.db belongs to another program or a newer engine)"
	}
	return fmt.Sprintf("engine exit %d (see the engine log)", code)
}

// ---------------------------------------------------------------------------------------------------------------
// Content-free views of the engine's shapes. They decode only IDs, states and numbers: titles, names, previews and
// message text are never read into memory by this tool, so they cannot be printed.

type accountView struct {
	Account  string  `json:"account"`
	Network  string  `json:"network"`
	LoggedIn bool    `json:"loggedIn"`
	State    string  `json:"state"`
	Error    *string `json:"error"` // a bridge state error code such as wa-logged-out
	StateTs  *int64  `json:"stateTs"`
}

type chatView struct {
	Chat      string         `json:"chat"`
	Account   *string        `json:"account"`
	Kind      string         `json:"kind"` // dm | group_dm | group (chats.list never lists system or space chats)
	DMContact *contactIDView `json:"dmContact"`
	Deleted   bool           `json:"deleted"`
}

// contactIDView keeps a direct chat's contact ID and bot flag; the contact's name is never decoded.
type contactIDView struct {
	Contact string `json:"contact"`
	IsBot   bool   `json:"isBot"`
}

type statsView struct {
	RSSBytes    int64 `json:"rssBytes"`
	MaxRSSBytes int64 `json:"maxRssBytes"`
}

func decodeAccounts(raw json.RawMessage) ([]accountView, error) {
	var r struct {
		Accounts []accountView `json:"accounts"`
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, errors.New("accounts.list: unexpected result shape")
	}
	return r.Accounts, nil
}

func decodeChatsPage(raw json.RawMessage) ([]chatView, *string, error) {
	var r struct {
		Chats      []chatView `json:"chats"`
		NextCursor *string    `json:"nextCursor"`
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, nil, errors.New("chats.list: unexpected result shape")
	}
	return r.Chats, r.NextCursor, nil
}

// token keeps a short identifier-like value (an account ID, a state, an error code) and drops anything else.
func token(s string) string {
	if len(s) > 64 {
		s = s[:64]
	}
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-', r == '.':
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "?"
	}
	return b.String()
}

func accountLabel(a accountView) string {
	id := token(a.Account)
	if !strings.HasPrefix(id, "acc_") {
		id = "acc_?"
	}
	state := a.State
	if state == "" {
		state = "UNKNOWN"
	}
	s := id + " " + token(a.Network) + " " + token(state)
	if !a.LoggedIn {
		s += " logged-out"
	}
	if a.Error != nil && *a.Error != "" {
		s += " error=" + token(*a.Error)
	}
	return s
}

type snapshot struct {
	Time        time.Time
	Accounts    []accountView
	AccountsErr error
	Chats       []chatView
	ChatsErr    error
	MessageNew  int64
	Stats       *statsView
}

// formatStatus is the one status line: time, accounts with state, chat count, message.new count and engine RSS.
func formatStatus(s snapshot) string {
	var b strings.Builder
	b.WriteString(stamp(s.Time))
	if s.AccountsErr != nil {
		b.WriteString(" accounts=?")
	} else {
		fmt.Fprintf(&b, " accounts=%d", len(s.Accounts))
		for _, a := range s.Accounts {
			b.WriteString(" [" + accountLabel(a) + "]")
		}
	}
	if s.ChatsErr != nil {
		b.WriteString(" chats=?")
	} else {
		fmt.Fprintf(&b, " chats=%d", len(s.Chats))
	}
	fmt.Fprintf(&b, " message.new=%d", s.MessageNew)
	if s.Stats != nil && s.Stats.RSSBytes > 0 {
		fmt.Fprintf(&b, " rss=%.1fMiB", float64(s.Stats.RSSBytes)/(1<<20))
		if s.Stats.MaxRSSBytes > 0 {
			fmt.Fprintf(&b, " peak=%.1fMiB", float64(s.Stats.MaxRSSBytes)/(1<<20))
		}
	} else {
		b.WriteString(" rss=?")
	}
	return b.String()
}

func (c *client) accounts() ([]accountView, error) {
	raw, err := c.call("accounts.list", nil, 10*time.Second)
	if err != nil {
		return nil, err
	}
	return decodeAccounts(raw)
}

// chats pages through chats.list for every account, archived and not.
func (c *client) chats() ([]chatView, error) { return listChats(c, "") }

// listChats pages through chats.list, archived and not, for one account ("" means every account). It keeps only IDs
// and kinds.
func listChats(api engineAPI, account string) ([]chatView, error) {
	var all []chatView
	for _, archived := range []bool{false, true} {
		var cursor *string
		for page := 0; page < 10000; page++ {
			params := map[string]any{"limit": pageLimit, "archived": archived}
			if account != "" {
				params["account"] = account
			}
			if cursor != nil {
				params["cursor"] = *cursor
			}
			raw, err := api.call("chats.list", params, 15*time.Second)
			if err != nil {
				return nil, err
			}
			chats, next, err := decodeChatsPage(raw)
			if err != nil {
				return nil, err
			}
			all = append(all, chats...)
			if next == nil || *next == "" {
				break
			}
			cursor = next
		}
	}
	return all, nil
}

func (c *client) stats() *statsView {
	raw, err := c.call("engine.stats", nil, 10*time.Second)
	if err != nil {
		return nil
	}
	var s statsView
	if json.Unmarshal(raw, &s) != nil {
		return nil
	}
	return &s
}

func (c *client) snapshot() snapshot {
	s := snapshot{Time: time.Now()}
	s.Accounts, s.AccountsErr = c.accounts()
	s.Chats, s.ChatsErr = c.chats()
	s.MessageNew = c.msgNew.Load()
	s.Stats = c.stats()
	return s
}

// handleNote prints a content-free line for account state changes and resyncs. Login steps are handled by the
// login loop; outside a login they are ignored.
func handleNote(n wireMsg) {
	switch n.Method {
	case "account.state":
		var p struct {
			Account accountView `json:"account"`
		}
		if json.Unmarshal(n.Params, &p) == nil {
			say("%s account.state %s", stamp(time.Now()), accountLabel(p.Account))
		}
	case "engine.resync":
		say("%s engine.resync: notifications were dropped; the message.new count is a lower bound", stamp(time.Now()))
	}
}

// ---------------------------------------------------------------------------------------------------------------
// watch and status

func runWatch(c *client, o options, sigs <-chan os.Signal) int {
	say("watching: a status line every %s; Ctrl-C stops the engine cleanly", o.interval)
	say("%s", formatStatus(c.snapshot()))
	ticker := time.NewTicker(o.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			say("%s", formatStatus(c.snapshot()))
		case n := <-c.notes:
			handleNote(n)
		case s := <-sigs:
			say("%s received; stopping the engine", s)
			if c.stop() != 0 {
				return 1
			}
			return 0
		case <-c.exited:
			say("%s the engine exited unexpectedly: %s", stamp(time.Now()), exitDescription(c.cmd.ProcessState.ExitCode()))
			return 1
		}
	}
}

// since is the earliest account stateTs (ms) that counts as reported in this run.
func (c *client) since() int64 { return c.started.Truncate(time.Second).Add(-time.Second).UnixMilli() }

// waitAccounts polls accounts.list until ready says yes or --wait runs out, printing account state changes meanwhile.
// It returns false if a signal arrived (the engine is then stopped) or the engine exited; both are reported.
func waitAccounts(c *client, o options, sigs <-chan os.Signal, ready func([]accountView) bool) bool {
	deadline := time.Now().Add(o.wait)
	for {
		accs, err := c.accounts()
		if err == nil && ready(accs) {
			return true
		}
		if time.Now().After(deadline) {
			return true
		}
		select {
		case n := <-c.notes:
			handleNote(n)
		case s := <-sigs:
			say("%s received; stopping the engine", s)
			c.stop()
			return false
		case <-c.exited:
			say("the engine exited unexpectedly: %s", exitDescription(c.cmd.ProcessState.ExitCode()))
			return false
		case <-time.After(time.Second):
		}
	}
}

func runStatus(c *client, o options, sigs <-chan os.Signal) int {
	// Right after a restart the stored state is the previous run's. Wait until every logged-in account has reported
	// a state in this run and that state is CONNECTED, or until --wait runs out.
	since := c.since()
	if !waitAccounts(c, o, sigs, func(accs []accountView) bool { return pendingAccounts(accs, since) == 0 }) {
		return 1
	}
	snap := c.snapshot()
	say("%s", formatStatus(snap))
	verdict := 0
	switch {
	case snap.AccountsErr != nil:
		say("status: accounts.list failed: %v", snap.AccountsErr)
		verdict = 1
	case len(snap.Accounts) == 0:
		say("status: no accounts in this data folder")
	default:
		bad := pendingAccounts(snap.Accounts, since)
		if bad == 0 {
			say("status: every account is logged in and CONNECTED in this run, without a new login")
		} else {
			say("status: %d account(s) not logged in and CONNECTED within %s", bad, o.wait)
			verdict = 1
		}
	}
	if c.stop() != 0 {
		return 1
	}
	return verdict
}

// pendingAccounts counts accounts that are logged out, or not yet CONNECTED with a state reported since `since`.
func pendingAccounts(accs []accountView, since int64) int {
	n := 0
	for _, a := range accs {
		fresh := a.StateTs != nil && *a.StateTs >= since
		if !a.LoggedIn || !fresh || a.State != "CONNECTED" {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------------------------------------------------
// login

type loginStep struct {
	Type           string `json:"type"`
	StepID         string `json:"step_id"`
	DisplayAndWait *struct {
		Type string `json:"type"`
		Data string `json:"data"`
	} `json:"display_and_wait"`
}

type loginError struct {
	Code     string `json:"code"`
	Message  string `json:"message"`
	Upstream string `json:"upstream"`
}

func runLogin(c *client, o options, sigs <-chan os.Signal) int {
	accs, err := c.accounts()
	if err != nil {
		say("accounts.list failed: %v", err)
		c.stop()
		return 1
	}
	for _, a := range accs {
		if a.Network == "whatsapp" && a.LoggedIn {
			say("a WhatsApp account is already logged in here (%s); use `cappy-t8 watch` or `cappy-t8 status`", accountLabel(a))
			c.stop()
			return 1
		}
	}

	qr, err := filepath.Abs(qrPath)
	if err != nil {
		say("qr path: %v", err)
		c.stop()
		return 1
	}
	defer func() { _ = os.Remove(qr) }() // the pairing QR is useless after the login ends; do not leave it around

	raw, err := c.call("login.start", map[string]any{"network": "whatsapp", "flow": "qr"}, 90*time.Second)
	if err != nil {
		say("login.start failed: %v", err)
		c.stop()
		return 1
	}
	var start struct {
		LoginID   string       `json:"loginID"`
		Step      *loginStep   `json:"step"`
		Completed *accountView `json:"completed"`
	}
	if err = json.Unmarshal(raw, &start); err != nil || start.LoginID == "" {
		say("login.start: unexpected result shape")
		c.stop()
		return 1
	}
	if start.Completed != nil {
		say("logged in: %s", accountLabel(*start.Completed))
		return 0
	}
	if !showStep(start.Step, qr) {
		return failLogin(c, start.LoginID)
	}

	timeout := time.NewTimer(o.loginTimeout)
	defer timeout.Stop()
	for {
		select {
		case n := <-c.notes:
			if n.Method != "login.step" {
				handleNote(n)
				continue
			}
			var p struct {
				LoginID string       `json:"loginID"`
				Status  string       `json:"status"`
				Step    *loginStep   `json:"step"`
				Account *accountView `json:"account"`
				Error   *loginError  `json:"error"`
			}
			if json.Unmarshal(n.Params, &p) != nil || p.LoginID != start.LoginID {
				continue
			}
			switch p.Status {
			case "step":
				if !showStep(p.Step, qr) {
					return failLogin(c, start.LoginID)
				}
			case "completed":
				if p.Account == nil {
					say("logged in (the engine sent no account)")
				} else {
					say("logged in: %s", accountLabel(*p.Account))
				}
				return 0
			default: // failed or cancelled
				say("login %s: %s", token(p.Status), describeLoginError(p.Error))
				return failLogin(c, start.LoginID)
			}
		case <-timeout.C:
			say("login timed out: the QR was not scanned within %s", o.loginTimeout)
			return failLogin(c, start.LoginID)
		case s := <-sigs:
			say("%s received; cancelling the login", s)
			return failLogin(c, start.LoginID)
		case <-c.exited:
			say("the engine exited during the login: %s", exitDescription(c.cmd.ProcessState.ExitCode()))
			return 1
		}
	}
}

// showStep renders a QR step and reports whether the login can go on.
func showStep(step *loginStep, qr string) bool {
	if step == nil {
		say("login: the engine sent an empty step")
		return false
	}
	if step.Type != "display_and_wait" || step.DisplayAndWait == nil {
		say("login: step %s (%s) is not supported by cappy-t8", token(step.Type), token(step.StepID))
		return false
	}
	if step.DisplayAndWait.Type != "qr" {
		say("login: waiting (step %s, display %s)", token(step.StepID), token(step.DisplayAndWait.Type))
		return true
	}
	if err := writeQR(step.DisplayAndWait.Data, qr); err != nil {
		say("login: could not render the QR: %v", err)
		return false
	}
	say("QR ready: %s (scan it in WhatsApp > Linked devices)", qr)
	if err := exec.Command("/usr/bin/open", qr).Run(); err != nil {
		say("open failed (%v); open the file by hand", err)
	}
	return true
}

// writeQR renders the pairing payload as a PNG (mode 0600). It replaces the file in one rename, so a viewer never
// reads a half-written image. The payload itself is never printed or logged.
func writeQR(data, path string) error {
	if data == "" {
		return errors.New("empty QR data")
	}
	png, err := qrcode.Encode(data, qrcode.Medium, 512)
	if err != nil {
		return errors.New("QR encoding failed")
	}
	dir := filepath.Dir(path)
	if err = os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".t8-qr-*.png")
	if err != nil {
		return err
	}
	tmp := f.Name()
	if _, err = f.Write(png); err == nil {
		err = f.Close()
	} else {
		_ = f.Close()
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err != nil {
		_ = os.Remove(tmp)
	}
	return err
}

func describeLoginError(e *loginError) string {
	if e == nil {
		return "no reason given"
	}
	s := token(e.Code)
	if e.Upstream != "" {
		s += " upstream " + token(e.Upstream)
	}
	if e.Message != "" {
		// The engine passes on only the connectors' own error messages. WhatsApp's pair error can carry library
		// text, so identifiers are scrubbed anyway.
		s += ": " + scrubText(e.Message)
	}
	return s
}

// scrubText shortens a free-text error message, replaces runs of 5 or more digits with # and drops words that
// contain @ (JIDs, MXIDs).
func scrubText(s string) string {
	if len(s) > 200 {
		s = s[:200]
	}
	words := strings.Fields(s)
	for i, w := range words {
		if strings.Contains(w, "@") {
			words[i] = "<id>"
			continue
		}
		var b strings.Builder
		digits := 0
		flush := func(run string) {
			if len(run) >= 5 {
				b.WriteByte('#')
			} else {
				b.WriteString(run)
			}
		}
		start := 0
		for j := 0; j < len(w); j++ {
			if w[j] >= '0' && w[j] <= '9' {
				if digits == 0 {
					start = j
				}
				digits++
				continue
			}
			if digits > 0 {
				flush(w[start:j])
				digits = 0
			}
			b.WriteByte(w[j])
		}
		if digits > 0 {
			flush(w[start:])
		}
		words[i] = b.String()
	}
	return strings.Join(words, " ")
}

// failLogin cancels the login (a no-op if the engine already ended it) and stops the engine.
func failLogin(c *client, loginID string) int {
	if _, err := c.call("login.cancel", map[string]any{"loginID": loginID}, 10*time.Second); err != nil {
		var re *rpcError
		if !errors.As(err, &re) || re.Data.Reason != "login_not_found" {
			say("login.cancel: %v", err)
		}
	} else {
		say("login cancelled")
	}
	c.stop()
	return 1
}
