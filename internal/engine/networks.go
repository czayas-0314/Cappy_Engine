package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/id"

	telegram "go.mau.fi/mautrix-telegram/pkg/connector"
	whatsapp "go.mau.fi/mautrix-whatsapp/pkg/connector"

	"cappy.local/engine/internal/inbox"
	"cappy.local/engine/internal/localmx"
	"cappy.local/engine/internal/netcfg"
	"cappy.local/engine/internal/rpc"
)

// Network states shown in networks.list.
const (
	StateStarting           = "starting"
	StateReady              = "ready"
	StateNeedsConfiguration = "needs_configuration"
	StateFailed             = "failed"
)

// MaxFileSize caps attachment downloads (WhatsApp downloads media into memory).
const MaxFileSize = 50 << 20

// NetworkSpec describes one network the engine can run.
type NetworkSpec struct {
	ID string
	// New returns a fresh, unconfigured connector.
	New func() bridgev2.NetworkConnector
	// Configure applies the connector config (before bridgev2 initializes it). hello carries host-provided settings.
	Configure func(conn bridgev2.NetworkConnector, hello *HelloParams) error
	// UnconfiguredReason is reported when the connector's ValidateConfig fails.
	UnconfiguredReason string
}

// DefaultNetworks are WhatsApp and Telegram, both registered in this one process.
func DefaultNetworks() []NetworkSpec {
	return []NetworkSpec{
		{
			ID:  "whatsapp",
			New: func() bridgev2.NetworkConnector { return &whatsapp.WhatsAppConnector{} },
			Configure: func(conn bridgev2.NetworkConnector, _ *HelloParams) error {
				return netcfg.Apply(conn, netcfg.WhatsApp())
			},
			UnconfiguredReason: "invalid_config",
		},
		{
			ID:  "telegram",
			New: func() bridgev2.NetworkConnector { return &telegram.TelegramConnector{} },
			Configure: func(conn bridgev2.NetworkConnector, hello *HelloParams) error {
				var creds *netcfg.TelegramCredentials
				if hello != nil && hello.Telegram != nil {
					creds = &netcfg.TelegramCredentials{APIID: hello.Telegram.APIID, APIHash: hello.Telegram.APIHash}
				}
				return netcfg.Apply(conn, netcfg.Telegram(Version, creds))
			},
			UnconfiguredReason: "api_credentials_missing",
		},
	}
}

// BridgeConfig is the bridgev2 config used for every network (DESIGN.md section 6).
func BridgeConfig() *bridgeconfig.BridgeConfig {
	return &bridgeconfig.BridgeConfig{
		// Nothing typed in Cappy can ever parse as a bridge command.
		CommandPrefix:           "\x00cappy-no-commands",
		SplitPortals:            true,
		PrivateChatPortalMeta:   true,
		PersonalFilteringSpaces: false,
		BridgeStatusNotices:     "none",
		// Each remote event is fully stored before its handler returns.
		PortalEventBuffer:             0,
		DeduplicateMatrixMessages:     true,
		CleanupOnLogout:               bridgeconfig.CleanupOnLogouts{Enabled: false},
		SendReconnectWait:             30 * time.Second,
		UnknownErrorAutoReconnect:     2 * time.Minute,
		UnknownErrorMaxAutoReconnects: 10,
		Permissions: bridgeconfig.PermissionConfig{
			string(localmx.OwnerMXID): &bridgeconfig.PermissionLevelAdmin,
		},
		Backfill: bridgeconfig.BackfillConfig{
			Enabled:            true,
			MaxInitialMessages: 50,
			MaxCatchupMessages: 500,
			Threads:            bridgeconfig.BackfillThreadsConfig{MaxInitialMessages: 0},
			Queue:              bridgeconfig.BackfillQueueConfig{Enabled: false, BatchSize: 100, BatchDelay: 20, MaxBatches: -1},
		},
	}
}

type network struct {
	spec    NetworkSpec
	conn    bridgev2.NetworkConnector
	mx      *localmx.Connector
	br      *bridgev2.Bridge
	state   string
	reason  string
	started bool
}

type HelloParams struct {
	Protocol int `json:"protocol"`
	Client   struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	} `json:"client"`
	Telegram *struct {
		APIID   int    `json:"apiId"`
		APIHash string `json:"apiHash"`
	} `json:"telegram,omitempty"`
}

func (e *Engine) handleHello(ctx context.Context, params json.RawMessage) (any, error) {
	var p HelloParams
	if err := rpc.Decode(params, &p); err != nil {
		return nil, err
	}
	if p.Protocol != ProtocolVersion {
		return nil, rpc.InvalidParams("unsupported protocol %d (engine speaks %d)", p.Protocol, ProtocolVersion)
	}
	if p.Telegram != nil && (p.Telegram.APIID < 0 || len(p.Telegram.APIHash) > 128) {
		return nil, rpc.InvalidParams("bad telegram credentials")
	}
	e.helloMu.Lock()
	defer e.helloMu.Unlock()
	if e.helloDone.Load() {
		return e.helloResp, nil
	}
	// Checked under helloMu: shutdown takes helloMu after setting stopping, so it either sees the bridges this hello
	// builds or this hello sees that the engine is stopping.
	if e.stopping.Load() {
		return nil, rpc.NewError(CodeNotReady, "not_ready", "the engine is stopping")
	}
	if err := e.buildBridges(e.bgCtx, &p); err != nil {
		e.log.Err(err).Msg("Failed to start bridges")
		e.resetBridges()
		return nil, rpc.NewError(CodeNetworkFailed, "network_failed", "failed to start the bridges")
	}
	e.helloResp = map[string]any{
		"protocol": ProtocolVersion,
		"engine": map[string]string{
			"version": Version,
			"commit":  buildCommit(),
			"go":      runtime.Version(),
		},
		"upstream":         Upstream,
		"pid":              os.Getpid(),
		"dataDir":          e.dataDir,
		"memoryLimitBytes": debug.SetMemoryLimit(-1),
	}
	e.helloDone.Store(true)
	return e.helloResp, nil
}

func buildCommit() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			if s.Key == "vcs.revision" {
				return s.Value
			}
		}
	}
	return "unknown"
}

func (e *Engine) buildBridges(ctx context.Context, hello *HelloParams) error {
	dbPath := filepath.Join(e.dataDir, "bridge.db")
	if f, err := os.OpenFile(dbPath, os.O_RDWR|os.O_CREATE, 0o600); err != nil {
		return err
	} else {
		_ = f.Close()
	}
	dbLog := e.log.With().Str("component", "bridge_db").Logger()
	db, err := dbutil.NewFromConfig("cappy-engine", dbutil.Config{PoolConfig: dbutil.PoolConfig{
		Type:         "sqlite3-fk-wal",
		URI:          "file:" + dbPath + "?_txlock=immediate",
		MaxOpenConns: 5,
		MaxIdleConns: 2,
	}}, dbutil.ZeroLogger(dbLog))
	if err != nil {
		return fmt.Errorf("open bridge.db: %w", err)
	}
	e.bridgeDB = db
	for _, spec := range e.opts.Networks {
		n := &network{spec: spec, state: StateStarting}
		e.networks = append(e.networks, n)
		e.networkMap[spec.ID] = n
		n.conn = spec.New()
		if spec.Configure != nil {
			if err = spec.Configure(n.conn, hello); err != nil {
				e.log.Err(err).Str("network", spec.ID).Msg("Failed to configure network")
				n.state, n.reason = StateFailed, "config_error"
				continue
			}
		}
		log := e.log.With().Str("network", spec.ID).Logger()
		n.mx = localmx.New(spec.ID, e.inbox, e.media, e.notifier, log.With().Str("component", "localmx").Logger())
		n.br = bridgev2.NewBridge(networkid.BridgeID(spec.ID), db, log, BridgeConfig(), n.mx, n.conn, commands.NewProcessor)
		n.br.ExternallyManagedDB = true
		if sizer, ok := n.conn.(bridgev2.MaxFileSizeingNetwork); ok {
			sizer.SetMaxFileSize(MaxFileSize)
		}
	}
	// bridgev2's core tables are shared (every row is keyed by bridge_id), so they are upgraded once.
	var first *network
	for _, n := range e.networks {
		if n.br != nil {
			first = n
			break
		}
	}
	if first == nil {
		return errors.New("no network could be configured")
	}
	if err = first.br.DB.Upgrade(ctx); err != nil {
		return fmt.Errorf("upgrade bridge.db: %w", err)
	}
	for _, n := range e.networks {
		if n.br == nil {
			continue
		}
		e.startNetwork(ctx, n)
	}
	e.reconcileAccounts(ctx)
	if _, err = e.inbox.Tx(ctx, func(tx *inbox.Tx) error {
		_, err := tx.FailPending(func(eventID string) bool {
			for _, n := range e.networks {
				if n.br == nil {
					continue
				}
				if part, err := n.br.DB.Message.GetPartByMXID(ctx, id.EventID(eventID)); err == nil && part != nil {
					return true
				}
			}
			return false
		})
		return err
	}); err != nil {
		e.log.Err(err).Msg("Failed to resolve pending outgoing messages")
	}
	return nil
}

// resetBridges undoes a buildBridges that failed before starting any bridge, so a later hello can try again. Called
// with helloMu held.
func (e *Engine) resetBridges() {
	if e.bridgeDB != nil {
		_ = e.bridgeDB.Close()
		e.bridgeDB = nil
	}
	e.networks = nil
	e.networkMap = make(map[string]*network)
}

func (e *Engine) startNetwork(ctx context.Context, n *network) {
	log := e.log.With().Str("network", n.spec.ID).Logger()
	configured := true
	if validator, ok := n.conn.(bridgev2.ConfigValidatingNetwork); ok {
		if err := validator.ValidateConfig(); err != nil {
			configured = false
			n.state, n.reason = StateNeedsConfiguration, n.spec.UnconfiguredReason
			log.Info().Msg("Network is not configured")
		}
	}
	if !configured {
		users, err := n.br.DB.UserLogin.GetAllUserIDsWithLogins(ctx)
		if err != nil || len(users) > 0 {
			// Starting would connect stored logins with an invalid config: leave the network stopped.
			log.Warn().Msg("Unconfigured network has stored logins; not starting it")
			return
		}
	}
	if err := n.br.Start(ctx); err != nil {
		log.Err(err).Msg("Failed to start network")
		n.state, n.reason = StateFailed, "start_failed"
		return
	}
	n.started = true
	if configured {
		n.state, n.reason = StateReady, ""
	}
}

// reconcileAccounts makes inbox.accounts match the bridges' stored logins.
func (e *Engine) reconcileAccounts(ctx context.Context) {
	for _, n := range e.networks {
		if n.br == nil {
			continue
		}
		user, err := n.br.GetUserByMXID(ctx, localmx.OwnerMXID)
		if err != nil || user == nil {
			continue
		}
		live := map[string]bool{}
		for _, login := range user.GetUserLogins() {
			live[string(login.ID)] = true
			_, err = e.inbox.Tx(ctx, func(tx *inbox.Tx) error {
				accountID, err := tx.EnsureAccount(n.spec.ID, string(login.ID))
				if err != nil {
					return err
				}
				if err = tx.SetAccountName(accountID, login.RemoteName); err != nil {
					return err
				}
				return tx.SetAccountLoggedIn(accountID)
			})
			if err != nil {
				e.log.Err(err).Str("network", n.spec.ID).Msg("Failed to reconcile account")
			}
		}
		accounts, err := e.inbox.ListAccounts(ctx)
		if err != nil {
			continue
		}
		for _, a := range accounts {
			if a.Network == n.spec.ID && !live[a.LoginID] && a.LoggedOutAt == nil {
				_, _ = e.inbox.Tx(ctx, func(tx *inbox.Tx) error { return tx.SetAccountLoggedOut(a.Account, time.Now().UnixMilli()) })
			}
		}
	}
}

func (e *Engine) network(name string) (*network, *rpc.Error) {
	n := e.networkMap[name]
	if n == nil {
		return nil, rpc.NewError(CodeUnknownNetwork, "unknown_network", "unknown network")
	}
	return n, nil
}

// usable reports whether the network can log in and send.
func (n *network) usable() *rpc.Error {
	switch {
	case n.state == StateNeedsConfiguration:
		return rpc.NewError(CodeNetworkNotConfigured, "network_not_configured", "the network is not configured")
	case n.state != StateReady || n.br == nil:
		return rpc.NewError(CodeNetworkFailed, "network_failed", "the network is not running")
	}
	return nil
}

type flowView struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

func (e *Engine) handleNetworksList(ctx context.Context, params json.RawMessage) (any, error) {
	var p struct{}
	if err := rpc.Decode(params, &p); err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(e.networks))
	for _, n := range e.networks {
		var reason any
		if n.reason != "" {
			reason = n.reason
		}
		count, _ := e.inbox.CountAccounts(ctx, n.spec.ID)
		name := n.spec.ID
		flows := []flowView{}
		if n.conn != nil {
			name = n.conn.GetName().DisplayName
			for _, f := range n.conn.GetLoginFlows() {
				flows = append(flows, flowView{ID: f.ID, Name: f.Name, Description: f.Description})
			}
		}
		entry := map[string]any{
			"network":  n.spec.ID,
			"name":     name,
			"state":    n.state,
			"reason":   reason,
			"accounts": count,
			"flows":    flows,
		}
		if n.mx != nil {
			if st := n.mx.GlobalState(); st.StateEvent != "" {
				entry["bridgeState"] = string(st.StateEvent)
			}
		}
		out = append(out, entry)
	}
	return map[string]any{"networks": out}, nil
}

// networkStateChanged is called by localmx for bridge-wide states.
func (e *Engine) networkStateChanged(network string, st status.BridgeState) {
	e.log.Debug().Str("network", network).Str("state", strings.ToLower(string(st.StateEvent))).Msg("Bridge state")
}
