package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"cappy.local/engine/internal/inbox"
	"cappy.local/engine/internal/localmx"
	"cappy.local/engine/internal/rpc"
)

// loginSession is one login in progress. Its context is owned by the session, not by any request, because WhatsApp
// cancels a login as soon as the context passed to Wait ends.
type loginSession struct {
	id      string
	network *network
	proc    bridgev2.LoginProcess
	ctx     context.Context
	cancel  context.CancelFunc

	mu      sync.Mutex
	step    *bridgev2.LoginStep
	done    bool
	waiting bool
}

func (s *loginSession) finished() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.done
}

// markDone reports whether this call ended the session (false if it had already ended).
func (s *loginSession) markDone() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done {
		return false
	}
	s.done = true
	return true
}

func safeCancel(proc bridgev2.LoginProcess) {
	if proc == nil {
		return
	}
	defer func() { _ = recover() }()
	proc.Cancel()
}

type loginStepNotice struct {
	LoginID string              `json:"loginID"`
	Network string              `json:"network"`
	Status  string              `json:"status"`
	Step    *bridgev2.LoginStep `json:"step,omitempty"`
	Account *inbox.Account      `json:"account,omitempty"`
	Error   *loginErrorView     `json:"error,omitempty"`
}

type loginErrorView struct {
	Code     string `json:"code"`
	Message  string `json:"message"`
	Upstream string `json:"upstream,omitempty"`
}

// upstreamError extracts a connector's error code and its curated message. Other error texts are not passed on: they
// can contain identifiers.
func upstreamError(err error) (code, message string) {
	var re bridgev2.RespError
	var rep *bridgev2.RespError
	switch {
	case errors.As(err, &re):
		return re.ErrCode, re.Err
	case errors.As(err, &rep) && rep != nil:
		return rep.ErrCode, rep.Err
	case errors.Is(err, context.DeadlineExceeded):
		return "", "timed out"
	}
	return "", "login failed"
}

func loginFailed(err error) *rpc.Error {
	code, msg := upstreamError(err)
	rerr := rpc.NewError(CodeLoginFailed, "login_failed", msg)
	rerr.Data.Upstream = code
	return rerr
}

type loginStartParams struct {
	Network string  `json:"network"`
	Flow    string  `json:"flow"`
	Replace bool    `json:"replace"`
	Relogin *string `json:"relogin"`
}

func (e *Engine) handleLoginStart(ctx context.Context, params json.RawMessage) (any, error) {
	var p loginStartParams
	if err := rpc.Decode(params, &p); err != nil {
		return nil, err
	}
	n, rerr := e.network(p.Network)
	if rerr != nil {
		return nil, rerr
	}
	// An unconfigured network (Telegram without api_id) is refused here, before CreateLogin, so nothing is contacted.
	if rerr = n.usable(); rerr != nil {
		return nil, rerr
	}
	validFlow := false
	for _, f := range n.conn.GetLoginFlows() {
		if f.ID == p.Flow {
			validFlow = true
		}
	}
	if !validFlow {
		return nil, rpc.InvalidParams("unknown login flow")
	}
	var override *bridgev2.UserLogin
	if p.Relogin != nil {
		acc, err := e.inbox.Account(ctx, *p.Relogin)
		if err != nil || acc.Network != n.spec.ID {
			return nil, rpc.NewError(CodeAccountNotFound, "account_not_found", "no such account")
		}
		override = n.br.GetCachedUserLoginByID(networkid.UserLoginID(acc.LoginID))
	}

	e.loginMu.Lock()
	// Checked under loginMu: shutdown cancels logins under loginMu after setting stopping, so a session registered
	// here is either cancelled by shutdown or never created.
	if e.stopping.Load() {
		e.loginMu.Unlock()
		return nil, rpc.NewError(CodeNotReady, "not_ready", "the engine is stopping")
	}
	if existing := e.activeLogin[n.spec.ID]; existing != "" {
		if !p.Replace {
			e.loginMu.Unlock()
			return nil, rpc.NewError(CodeLoginInProgress, "login_in_progress", "a login is already in progress for this network")
		}
		if old := e.logins[existing]; old != nil {
			e.loginMu.Unlock()
			e.endLogin(old, "cancelled", nil)
			e.loginMu.Lock()
			if e.stopping.Load() {
				e.loginMu.Unlock()
				return nil, rpc.NewError(CodeNotReady, "not_ready", "the engine is stopping")
			}
		}
	}
	user, err := n.br.GetUserByMXID(ctx, localmx.OwnerMXID)
	if err != nil || user == nil {
		e.loginMu.Unlock()
		return nil, rpc.NewError(CodeNetworkFailed, "network_failed", "the bridge has no owner user")
	}
	sessCtx, cancel := context.WithCancel(e.bgCtx)
	s := &loginSession{id: "lgn_" + inbox.RandomB32(16), network: n, ctx: sessCtx, cancel: cancel}
	e.logins[s.id] = s
	e.activeLogin[n.spec.ID] = s.id
	e.loginMu.Unlock()

	proc, err := n.conn.CreateLogin(sessCtx, user, p.Flow)
	if err != nil {
		e.endLogin(s, "", nil)
		return nil, loginFailed(err)
	}
	s.mu.Lock()
	s.proc = proc
	s.mu.Unlock()
	var step *bridgev2.LoginStep
	if ow, ok := proc.(bridgev2.LoginProcessWithOverride); ok && override != nil {
		step, err = ow.StartWithOverride(sessCtx, override)
	} else {
		step, err = proc.Start(sessCtx)
	}
	if s.finished() {
		return nil, rpc.NewError(CodeLoginNotFound, "login_not_found", "the login was cancelled")
	}
	if err != nil {
		e.endLogin(s, "", nil)
		return nil, loginFailed(err)
	}
	return e.advanceLogin(s, step)
}

// advanceLogin handles a new step from the connector and returns the response for the request that produced it.
func (e *Engine) advanceLogin(s *loginSession, step *bridgev2.LoginStep) (any, error) {
	if step == nil {
		e.endLogin(s, "failed", &loginErrorView{Code: "login_failed", Message: "the network returned no step"})
		return nil, rpc.NewError(CodeLoginFailed, "login_failed", "the network returned no step")
	}
	switch step.Type {
	case bridgev2.LoginStepTypeComplete:
		// A completed process is not cancelled: the new login's client is already running.
		s.mu.Lock()
		s.proc = nil
		s.mu.Unlock()
		acc, err := e.completeLogin(s, step)
		if err != nil {
			e.endLogin(s, "failed", &loginErrorView{Code: "login_failed", Message: "failed to record the account"})
			return nil, rpc.NewError(CodeLoginFailed, "login_failed", "failed to record the account")
		}
		e.endLogin(s, "", nil)
		e.srv.NotifyPriority("login.step", loginStepNotice{LoginID: s.id, Network: s.network.spec.ID, Status: "completed", Account: acc})
		return map[string]any{"loginID": s.id, "completed": acc}, nil
	case bridgev2.LoginStepTypeDisplayAndWait:
		s.mu.Lock()
		s.step = step
		startWaiter := !s.waiting
		s.waiting = true
		s.mu.Unlock()
		if startWaiter {
			go e.waitLogin(s)
		}
		return map[string]any{"loginID": s.id, "step": step}, nil
	case bridgev2.LoginStepTypeUserInput, bridgev2.LoginStepTypeCookies:
		s.mu.Lock()
		s.step = step
		s.mu.Unlock()
		return map[string]any{"loginID": s.id, "step": step}, nil
	default:
		// webauthn (WhatsApp passkeys) and client_http are out of scope for the spike.
		e.endLogin(s, "failed", &loginErrorView{Code: "unsupported_step", Message: "this login step is not supported yet"})
		return nil, rpc.NewError(CodeUnsupportedStep, "unsupported_step", "this login step is not supported yet")
	}
}

// waitLogin loops on Wait while the login shows something (a QR code that rotates, a pairing code). Each new step is
// pushed as a login.step notification.
func (e *Engine) waitLogin(s *loginSession) {
	defer func() {
		s.mu.Lock()
		s.waiting = false
		s.mu.Unlock()
	}()
	s.mu.Lock()
	proc := s.proc
	s.mu.Unlock()
	for {
		waiter, ok := proc.(bridgev2.LoginProcessDisplayAndWait)
		if !ok {
			e.endLogin(s, "failed", &loginErrorView{Code: "unsupported_step", Message: "the login cannot wait"})
			return
		}
		step, err := waiter.Wait(s.ctx)
		if s.finished() {
			return
		}
		if err != nil {
			code, msg := upstreamError(err)
			e.endLogin(s, "failed", &loginErrorView{Code: "login_failed", Message: msg, Upstream: code})
			return
		}
		if step == nil {
			e.endLogin(s, "failed", &loginErrorView{Code: "login_failed", Message: "the network returned no step"})
			return
		}
		switch step.Type {
		case bridgev2.LoginStepTypeDisplayAndWait:
			s.mu.Lock()
			s.step = step
			s.mu.Unlock()
			e.srv.NotifyPriority("login.step", loginStepNotice{LoginID: s.id, Network: s.network.spec.ID, Status: "step", Step: step})
			continue
		case bridgev2.LoginStepTypeUserInput, bridgev2.LoginStepTypeCookies:
			s.mu.Lock()
			s.step = step
			s.mu.Unlock()
			e.srv.NotifyPriority("login.step", loginStepNotice{LoginID: s.id, Network: s.network.spec.ID, Status: "step", Step: step})
			return
		default:
			_, _ = e.advanceLogin(s, step) // complete, or an unsupported step; both notify
			return
		}
	}
}

// completeLogin records the new account.
func (e *Engine) completeLogin(s *loginSession, step *bridgev2.LoginStep) (*inbox.Account, error) {
	if step.CompleteParams == nil {
		return nil, errors.New("complete step without params")
	}
	loginID := string(step.CompleteParams.UserLoginID)
	name := ""
	if ul := step.CompleteParams.UserLogin; ul != nil {
		loginID = string(ul.ID)
		name = ul.RemoteName
	}
	if loginID == "" {
		return nil, errors.New("complete step without a login ID")
	}
	var accountID string
	ch, err := e.inbox.Tx(s.ctx, func(tx *inbox.Tx) error {
		var err error
		if accountID, err = tx.EnsureAccount(s.network.spec.ID, loginID); err != nil {
			return err
		}
		if err = tx.SetAccountName(accountID, name); err != nil {
			return err
		}
		return tx.SetAccountLoggedIn(accountID)
	})
	if err != nil {
		return nil, err
	}
	e.notifier.Changed(s.ctx, ch)
	acc, err := e.inbox.Account(context.WithoutCancel(s.ctx), accountID)
	if err != nil {
		return nil, err
	}
	e.decorateAccount(acc)
	return acc, nil
}

// endLogin ends a session once. status "" ends it silently (the request's response reports the outcome).
func (e *Engine) endLogin(s *loginSession, status string, lerr *loginErrorView) {
	if !s.markDone() {
		return
	}
	s.cancel()
	s.mu.Lock()
	proc := s.proc
	s.mu.Unlock()
	// Cancel is idempotent in the connectors; after completion or failure it only releases what is left.
	safeCancel(proc)
	e.loginMu.Lock()
	delete(e.logins, s.id)
	if e.activeLogin[s.network.spec.ID] == s.id {
		delete(e.activeLogin, s.network.spec.ID)
	}
	e.loginMu.Unlock()
	if status != "" {
		e.srv.NotifyPriority("login.step", loginStepNotice{LoginID: s.id, Network: s.network.spec.ID, Status: status, Error: lerr})
	}
}

func (e *Engine) cancelAllLogins(reason string) {
	e.loginMu.Lock()
	sessions := make([]*loginSession, 0, len(e.logins))
	for _, s := range e.logins {
		sessions = append(sessions, s)
	}
	e.loginMu.Unlock()
	for _, s := range sessions {
		e.endLogin(s, "cancelled", &loginErrorView{Code: reason, Message: "the engine is stopping"})
	}
}

func (e *Engine) session(loginID string) (*loginSession, *rpc.Error) {
	e.loginMu.Lock()
	defer e.loginMu.Unlock()
	s := e.logins[loginID]
	if s == nil {
		return nil, rpc.NewError(CodeLoginNotFound, "login_not_found", "no such login in progress")
	}
	return s, nil
}

type loginSubmitParams struct {
	LoginID string            `json:"loginID"`
	StepID  string            `json:"stepID"`
	Input   map[string]string `json:"input"`
}

func (e *Engine) handleLoginSubmit(ctx context.Context, params json.RawMessage) (any, error) {
	var p loginSubmitParams
	if err := rpc.Decode(params, &p); err != nil {
		return nil, err
	}
	if len(p.Input) > 32 {
		return nil, rpc.InvalidParams("too many input fields")
	}
	for k, v := range p.Input {
		if len(k) > 256 || len(v) > 64<<10 {
			return nil, rpc.NewError(CodeTooLarge, "too_large", "input too large")
		}
	}
	s, rerr := e.session(p.LoginID)
	if rerr != nil {
		return nil, rerr
	}
	s.mu.Lock()
	step := s.step
	proc := s.proc
	s.mu.Unlock()
	if step == nil || step.StepID != p.StepID {
		return nil, rpc.NewError(CodeLoginStepMismatch, "login_step_mismatch", "that is not the current login step")
	}
	var next *bridgev2.LoginStep
	var err error
	switch step.Type {
	case bridgev2.LoginStepTypeUserInput:
		in, ok := proc.(bridgev2.LoginProcessUserInput)
		if !ok {
			return nil, rpc.NewError(CodeLoginStepMismatch, "login_step_mismatch", "the login does not take input")
		}
		next, err = in.SubmitUserInput(s.ctx, p.Input)
	case bridgev2.LoginStepTypeCookies:
		in, ok := proc.(bridgev2.LoginProcessCookies)
		if !ok {
			return nil, rpc.NewError(CodeLoginStepMismatch, "login_step_mismatch", "the login does not take cookies")
		}
		next, err = in.SubmitCookies(s.ctx, p.Input)
	default:
		return nil, rpc.NewError(CodeLoginStepMismatch, "login_step_mismatch", "the current step takes no input")
	}
	if s.finished() {
		return nil, rpc.NewError(CodeLoginNotFound, "login_not_found", "the login was cancelled")
	}
	if err != nil {
		e.endLogin(s, "", nil)
		return nil, loginFailed(err)
	}
	return e.advanceLogin(s, next)
}

func (e *Engine) handleLoginCancel(ctx context.Context, params json.RawMessage) (any, error) {
	var p struct {
		LoginID string `json:"loginID"`
	}
	if err := rpc.Decode(params, &p); err != nil {
		return nil, err
	}
	s, rerr := e.session(p.LoginID)
	if rerr != nil {
		return nil, rerr
	}
	e.endLogin(s, "cancelled", nil)
	return map[string]bool{"cancelled": true}, nil
}
