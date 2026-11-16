// Package engine is Cappy Engine's core: it builds one bridgev2 bridge per network (each with its own local Matrix
// connector), runs logins, answers the host's JSON-RPC requests and turns inbox changes into notifications.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2"

	"cappy.local/engine/internal/inbox"
	"cappy.local/engine/internal/media"
	"cappy.local/engine/internal/rpc"
)

const ProtocolVersion = 1

// Version is the engine's own version, reported in hello.
var Version = "0.1.0-spike"

// Upstream versions linked into this build (DESIGN.md section 2).
var Upstream = map[string]string{
	"mautrix-go":       "v0.32.0",
	"mautrix-whatsapp": "v0.2610.0",
	"mautrix-telegram": "v0.2610.0",
}

// Engine error codes (DESIGN.md section 13).
const (
	CodeNotReady             = 1000
	CodeUnknownNetwork       = 1001
	CodeNetworkNotConfigured = 1002
	CodeNetworkFailed        = 1003
	CodeLoginInProgress      = 1004
	CodeLoginNotFound        = 1005
	CodeLoginStepMismatch    = 1006
	CodeLoginFailed          = 1007
	CodeUnsupportedStep      = 1008
	CodeAccountNotFound      = 1009
	CodeChatNotFound         = 1010
	CodeNotLoggedIn          = 1011
	CodeInvalidCursor        = 1012
	CodeTooLarge             = 1013
)

type Options struct {
	DataDir string
	Log     zerolog.Logger
	// Networks to build on hello. Nil means WhatsApp and Telegram.
	Networks []NetworkSpec
	// Called with the install key once inbox.db is open (eg to key log redaction).
	OnInstallKey func(key []byte)
}

type Engine struct {
	opts    Options
	log     zerolog.Logger
	dataDir string
	inbox   *inbox.DB
	media   *media.Store
	srv     *rpc.Server
	started time.Time

	helloMu    sync.Mutex
	helloDone  atomic.Bool
	helloResp  map[string]any
	bridgeDB   *dbutil.Database
	networks   []*network
	networkMap map[string]*network

	loginMu     sync.Mutex
	logins      map[string]*loginSession
	activeLogin map[string]string

	notifier *notifier

	stopping atomic.Bool
	stopCh   chan struct{}
	stopOnce sync.Once
	bgCtx    context.Context
	cancelBg context.CancelFunc
	// sendMu orders sendWG.Add (in beginSend) against the shutdown sequence's wait.
	sendMu    sync.Mutex
	sendWG    sync.WaitGroup
	closeOnce sync.Once
}

// New opens the data folder (inbox.db, media) but builds no bridges: that happens on hello.
func New(ctx context.Context, opts Options) (*Engine, error) {
	if opts.DataDir == "" {
		return nil, errors.New("data dir is required")
	}
	if err := os.MkdirAll(opts.DataDir, 0o700); err != nil {
		return nil, err
	}
	ib, err := inbox.Open(ctx, filepath.Join(opts.DataDir, "inbox.db"))
	if err != nil {
		return nil, fmt.Errorf("inbox: %w", err)
	}
	if opts.OnInstallKey != nil {
		opts.OnInstallKey(ib.InstallKey())
	}
	ms, err := media.New(opts.DataDir)
	if err != nil {
		_ = ib.Close()
		return nil, fmt.Errorf("media: %w", err)
	}
	if opts.Networks == nil {
		opts.Networks = DefaultNetworks()
	}
	e := &Engine{
		opts:        opts,
		log:         opts.Log,
		dataDir:     opts.DataDir,
		inbox:       ib,
		media:       ms,
		started:     time.Now(),
		networkMap:  make(map[string]*network),
		logins:      make(map[string]*loginSession),
		activeLogin: make(map[string]string),
		stopCh:      make(chan struct{}),
	}
	e.bgCtx, e.cancelBg = context.WithCancel(opts.Log.WithContext(context.Background()))
	return e, nil
}

// Inbox exposes the store (for tests).
func (e *Engine) Inbox() *inbox.DB { return e.inbox }

// Bridge returns a network's bridge after hello (for tests and diagnostics).
func (e *Engine) Bridge(network string) *bridgev2.Bridge {
	if n := e.networkMap[network]; n != nil {
		return n.br
	}
	return nil
}

// Stop asks a running engine to shut down gracefully (as the "shutdown" request does).
func (e *Engine) Stop() {
	e.stopOnce.Do(func() { close(e.stopCh) })
}

// Run serves JSON-RPC on in/out until stdin ends, shutdown is requested or ctx is cancelled (a signal), then stops
// gracefully. It returns the process exit code: 0 after a clean stop, 1 if the 10 s hard deadline passed.
func (e *Engine) Run(ctx context.Context, in io.Reader, out io.Writer) int {
	e.srv = rpc.NewServer(in, out, e.log.With().Str("component", "rpc").Logger())
	e.notifier = newNotifier(e)
	e.registerHandlers()
	e.srv.Gate = e.gate
	serveDone := make(chan error, 1)
	go func() { serveDone <- e.srv.Serve(e.bgCtx) }()
	reason := ""
	select {
	case err := <-serveDone:
		reason = "stdin_closed"
		if err != nil {
			reason = "stdin_error"
		}
	case <-e.stopCh:
		reason = "shutdown_request"
	case <-ctx.Done():
		reason = "signal"
	}
	e.log.Info().Str("reason", reason).Msg("Stopping engine")
	return e.shutdown()
}

func (e *Engine) gate(method string) *rpc.Error {
	if e.stopping.Load() {
		return rpc.NewError(CodeNotReady, "not_ready", "the engine is stopping")
	}
	switch method {
	case "hello", "ping", "engine.stats", "shutdown":
		return nil
	}
	if !e.helloDone.Load() {
		return rpc.NewError(CodeNotReady, "not_ready", "send hello first")
	}
	return nil
}

// shutdown runs the graceful stop sequence with a hard deadline. Once stopping is set the gate refuses new requests,
// and hello, login.start and messages.send check it again under their own locks, so nothing they start can slip past
// the steps below.
func (e *Engine) shutdown() int {
	e.stopping.Store(true)
	done := make(chan struct{})
	go func() {
		defer close(done)
		// 1. Let requests that already passed the gate finish, so none of them registers a login or a send after the
		// logins are cancelled and the sends are waited for.
		e.srv.WaitInflight(2 * time.Second)
		// 2. Cancel logins in progress (login.start refuses to register a new one from here on).
		e.cancelAllLogins("engine_stopping")
		// 3. Let queued sends reach their networks while the bridges still run (beginSend refuses new ones).
		// Taking sendMu once is a barrier: every beginSend that saw stopping=false has called sendWG.Add by now.
		e.sendMu.Lock()
		e.sendMu.Unlock()
		waitTimeout(&e.sendWG, 2*time.Second)
		// 4. Stop the bridges. hello holds helloMu while it builds them, so this waits for a hello in progress and then
		// stops every bridge it started.
		e.helloMu.Lock()
		networks := e.networks
		e.helloMu.Unlock()
		var wg sync.WaitGroup
		for _, n := range networks {
			if n.br == nil || !n.started {
				continue
			}
			wg.Add(1)
			go func(n *network) {
				defer wg.Done()
				n.br.StopWithTimeout(5 * time.Second)
			}(n)
		}
		wg.Wait()
		e.notifier.flush()
		e.srv.Close()
		e.closeStores()
	}()
	select {
	case <-done:
		e.cancelBg()
		e.log.Info().Msg("Engine stopped")
		return 0
	case <-time.After(10 * time.Second):
		e.log.Error().Msg("Graceful stop timed out")
		return 1
	}
}

func (e *Engine) closeStores() {
	e.closeOnce.Do(func() {
		if e.bridgeDB != nil {
			_, _ = e.bridgeDB.Exec(context.Background(), "PRAGMA wal_checkpoint(TRUNCATE)")
			if err := e.bridgeDB.Close(); err != nil {
				e.log.Warn().Err(err).Msg("Failed to close bridge.db")
			}
		}
		if err := e.inbox.Close(); err != nil {
			e.log.Warn().Err(err).Msg("Failed to close inbox.db")
		}
	})
}

// Close releases the stores of an engine that never ran (eg startup failed after New).
func (e *Engine) Close() {
	e.closeStores()
}

// beginSend registers a send that will run in the background. It reports false once the engine is stopping.
func (e *Engine) beginSend() bool {
	e.sendMu.Lock()
	defer e.sendMu.Unlock()
	if e.stopping.Load() {
		return false
	}
	e.sendWG.Add(1)
	return true
}

func waitTimeout(wg *sync.WaitGroup, d time.Duration) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d):
	}
}

func (e *Engine) registerHandlers() {
	h := map[string]rpc.Handler{
		"hello":           e.handleHello,
		"ping":            e.handlePing,
		"engine.stats":    e.handleStats,
		"shutdown":        e.handleShutdown,
		"networks.list":   e.handleNetworksList,
		"login.start":     e.handleLoginStart,
		"login.submit":    e.handleLoginSubmit,
		"login.cancel":    e.handleLoginCancel,
		"accounts.list":   e.handleAccountsList,
		"accounts.logout": e.handleAccountsLogout,
		"chats.list":      e.handleChatsList,
		"messages.list":   e.handleMessagesList,
		"messages.send":   e.handleMessagesSend,
		"chats.markRead":  e.handleChatsMarkRead,
		"media.preview":   e.handleMediaPreview,
		"search":          e.handleSearch,
	}
	for method, handler := range h {
		e.srv.Handle(method, handler)
	}
}

func (e *Engine) handlePing(ctx context.Context, params json.RawMessage) (any, error) {
	var p struct{}
	if err := rpc.Decode(params, &p); err != nil {
		return nil, err
	}
	return map[string]int64{"ts": time.Now().UnixMilli()}, nil
}

func (e *Engine) handleShutdown(ctx context.Context, params json.RawMessage) (any, error) {
	var p struct{}
	if err := rpc.Decode(params, &p); err != nil {
		return nil, err
	}
	// Stop after this response is written: the stop sequence waits for in-flight requests.
	go func() {
		time.Sleep(20 * time.Millisecond)
		e.Stop()
	}()
	return map[string]bool{"ok": true}, nil
}

func (e *Engine) handleStats(ctx context.Context, params json.RawMessage) (any, error) {
	var p struct{}
	if err := rpc.Decode(params, &p); err != nil {
		return nil, err
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	fileSize := func(names ...string) int64 {
		var total int64
		for _, n := range names {
			if info, err := os.Stat(filepath.Join(e.dataDir, n)); err == nil {
				total += info.Size()
			}
		}
		return total
	}
	return map[string]any{
		"rssBytes":         currentRSS(),
		"maxRssBytes":      maxRSS(),
		"goHeapBytes":      ms.HeapAlloc,
		"goSysBytes":       ms.Sys,
		"goroutines":       runtime.NumGoroutine(),
		"gcCycles":         ms.NumGC,
		"memoryLimitBytes": debug.SetMemoryLimit(-1),
		"uptimeMs":         time.Since(e.started).Milliseconds(),
		"dbBytes": map[string]int64{
			"bridge": fileSize("bridge.db", "bridge.db-wal"),
			"inbox":  fileSize("inbox.db", "inbox.db-wal"),
		},
		"mediaBytes": e.media.Size(),
	}, nil
}
