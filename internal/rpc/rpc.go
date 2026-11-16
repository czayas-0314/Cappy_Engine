// Package rpc is the engine's JSON-RPC 2.0 server over a byte stream (stdin/stdout in production).
//
// Framing: one JSON object per '\n'-terminated line (NDJSON) in both directions. A line longer than MaxLineBytes is
// answered with -32600 and discarded; the server keeps running. Requests run concurrently (at most MaxConcurrent at a
// time) and responses are matched by id. Notifications go through a bounded queue: when it overflows, ordinary
// notifications are dropped and one "engine.resync" notification is sent once the queue has room again. Responses are
// never dropped.
//
// Params and results are never logged: only the method, id, duration and error code.
package rpc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

const (
	MaxLineBytes   = 8 << 20
	MaxConcurrent  = 16
	NotifyQueueLen = 1024
)

// Standard JSON-RPC 2.0 error codes.
const (
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternalError  = -32603
)

// Error is a JSON-RPC error object. Data always carries a snake_case "reason".
type Error struct {
	Code    int       `json:"code"`
	Message string    `json:"message"`
	Data    ErrorData `json:"data"`
}

type ErrorData struct {
	Reason   string `json:"reason"`
	Upstream string `json:"upstream,omitempty"`
}

func (e *Error) Error() string {
	return fmt.Sprintf("rpc error %d (%s): %s", e.Code, e.Data.Reason, e.Message)
}

func NewError(code int, reason, message string) *Error {
	return &Error{Code: code, Message: message, Data: ErrorData{Reason: reason}}
}

// InvalidParams wraps a decoding or validation problem as -32602.
func InvalidParams(format string, args ...any) *Error {
	return NewError(CodeInvalidParams, "invalid_params", fmt.Sprintf(format, args...))
}

// Handler runs one request. params is the raw "params" value (may be nil). Returning an *Error sends it as is; any
// other error becomes -32603 with a generic message (the error text is not sent, as it may carry user data).
type Handler func(ctx context.Context, params json.RawMessage) (any, error)

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *Error          `json:"error,omitempty"`
}

type notification struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  any    `json:"params"`
}

var nullID = json.RawMessage("null")

type Server struct {
	in  io.Reader
	out io.Writer
	log zerolog.Logger

	outMu    sync.Mutex
	handlers map[string]Handler
	// Gate is consulted before every request; a non-nil error is returned instead of running the handler (eg
	// "not_ready" before hello, or while stopping).
	Gate func(method string) *Error

	sem      chan struct{}
	inflight sync.WaitGroup
	// gateMu makes "pass the gate, then count as in flight" one step relative to WaitInflight, so a request the gate
	// let through is always waited for.
	gateMu sync.RWMutex

	notifyCh     chan []byte
	priorityCh   chan []byte
	overflow     atomic.Bool
	notifyDone   chan struct{}
	closeOnce    sync.Once
	closed       chan struct{}
	notifyClosed atomic.Bool
}

func NewServer(in io.Reader, out io.Writer, log zerolog.Logger) *Server {
	s := &Server{
		in:         in,
		out:        out,
		log:        log,
		handlers:   make(map[string]Handler),
		sem:        make(chan struct{}, MaxConcurrent),
		notifyCh:   make(chan []byte, NotifyQueueLen),
		priorityCh: make(chan []byte, 64),
		notifyDone: make(chan struct{}),
		closed:     make(chan struct{}),
	}
	go s.notifyLoop()
	return s
}

// Handle registers a method. It must be called before Serve.
func (s *Server) Handle(method string, h Handler) {
	s.handlers[method] = h
}

// Serve reads requests until the input ends (io.EOF returns nil) or fails.
func (s *Server) Serve(ctx context.Context) error {
	r := bufio.NewReaderSize(s.in, 64<<10)
	for {
		line, tooLong, err := readLine(r, MaxLineBytes)
		if tooLong {
			s.writeResponse(&response{JSONRPC: "2.0", ID: nullID, Error: NewError(CodeInvalidRequest, "too_large",
				fmt.Sprintf("line exceeds %d bytes", MaxLineBytes))})
		} else if len(bytes.TrimSpace(line)) > 0 {
			s.dispatch(ctx, line)
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

// readLine reads one '\n'-terminated line. A line over max bytes is consumed up to its end and reported as tooLong.
func readLine(r *bufio.Reader, max int) (line []byte, tooLong bool, err error) {
	var buf []byte
	for {
		chunk, isPrefix, rerr := r.ReadLine()
		if !tooLong {
			if len(buf)+len(chunk) > max {
				tooLong = true
				buf = nil
			} else {
				buf = append(buf, chunk...)
			}
		}
		if rerr != nil {
			return buf, tooLong, rerr
		}
		if !isPrefix {
			return buf, tooLong, nil
		}
	}
}

func (s *Server) dispatch(ctx context.Context, line []byte) {
	var req request
	if err := json.Unmarshal(line, &req); err != nil {
		var arr []json.RawMessage
		if json.Unmarshal(line, &arr) == nil {
			s.writeResponse(&response{JSONRPC: "2.0", ID: nullID, Error: NewError(CodeInvalidRequest, "batch_unsupported", "batch requests are not supported")})
			return
		}
		s.writeResponse(&response{JSONRPC: "2.0", ID: nullID, Error: NewError(CodeParseError, "parse_error", "invalid JSON")})
		return
	}
	id := req.ID
	isNotification := len(id) == 0
	if !isNotification && !validID(id) {
		s.writeResponse(&response{JSONRPC: "2.0", ID: nullID, Error: NewError(CodeInvalidRequest, "invalid_id", "id must be a string or number")})
		return
	}
	if req.JSONRPC != "2.0" || req.Method == "" {
		if !isNotification {
			s.writeResponse(&response{JSONRPC: "2.0", ID: id, Error: NewError(CodeInvalidRequest, "invalid_request", "not a JSON-RPC 2.0 request")})
		}
		return
	}
	handler, ok := s.handlers[req.Method]
	if !ok {
		if !isNotification {
			s.writeResponse(&response{JSONRPC: "2.0", ID: id, Error: NewError(CodeMethodNotFound, "method_not_found", "unknown method")})
		}
		return
	}
	s.gateMu.RLock()
	if s.Gate != nil {
		if gerr := s.Gate(req.Method); gerr != nil {
			s.gateMu.RUnlock()
			if !isNotification {
				s.writeResponse(&response{JSONRPC: "2.0", ID: id, Error: gerr})
			}
			return
		}
	}
	s.inflight.Add(1)
	s.gateMu.RUnlock()
	s.sem <- struct{}{}
	go func() {
		defer func() {
			<-s.sem
			s.inflight.Done()
		}()
		start := time.Now()
		result, err := s.call(ctx, handler, req.Params)
		rerr := toRPCError(err)
		evt := s.log.Debug()
		if rerr != nil {
			evt = s.log.Info().Int("error_code", rerr.Code).Str("error_reason", rerr.Data.Reason)
		}
		evt.Str("method", req.Method).RawJSON("rpc_id", safeID(id)).Dur("duration", time.Since(start)).Msg("RPC request handled")
		if isNotification {
			return
		}
		if rerr != nil {
			s.writeResponse(&response{JSONRPC: "2.0", ID: id, Error: rerr})
		} else {
			if result == nil {
				result = struct{}{}
			}
			s.writeResponse(&response{JSONRPC: "2.0", ID: id, Result: result})
		}
	}()
}

func (s *Server) call(ctx context.Context, h Handler, params json.RawMessage) (result any, err error) {
	defer func() {
		if p := recover(); p != nil {
			// The panic value and stack may contain user data, so only note that it happened.
			s.log.Error().Int("stack_len", len(debug.Stack())).Msg("RPC handler panicked")
			err = NewError(CodeInternalError, "internal_error", "internal error")
		}
	}()
	return h(ctx, params)
}

func toRPCError(err error) *Error {
	if err == nil {
		return nil
	}
	var rerr *Error
	if errors.As(err, &rerr) {
		return rerr
	}
	return NewError(CodeInternalError, "internal_error", "internal error")
}

func validID(id json.RawMessage) bool {
	if len(id) == 0 {
		return false
	}
	switch id[0] {
	case '"':
		var s string
		return json.Unmarshal(id, &s) == nil
	case 'n':
		return bytes.Equal(id, nullID)
	default:
		var n json.Number
		return json.Unmarshal(id, &n) == nil
	}
}

// safeID returns the id for logging only if it is short (ids are client-chosen counters, not user data).
func safeID(id json.RawMessage) []byte {
	if len(id) == 0 || len(id) > 40 {
		return []byte("null")
	}
	return id
}

func (s *Server) writeResponse(resp *response) {
	data, err := json.Marshal(resp)
	if err != nil {
		data, _ = json.Marshal(&response{JSONRPC: "2.0", ID: resp.ID, Error: NewError(CodeInternalError, "internal_error", "failed to encode result")})
	}
	s.writeLine(data)
}

func (s *Server) writeLine(data []byte) {
	s.outMu.Lock()
	defer s.outMu.Unlock()
	data = append(data, '\n')
	if _, err := s.out.Write(data); err != nil {
		s.log.Debug().Err(err).Msg("Failed to write RPC output")
	}
}

// Notify queues a notification. Ordinary notifications are dropped when the queue is full (and an engine.resync is
// sent later); priority ones (login and account state) wait for room.
func (s *Server) Notify(method string, params any) {
	s.notify(method, params, false)
}

// NotifyPriority queues a notification that must not be dropped.
func (s *Server) NotifyPriority(method string, params any) {
	s.notify(method, params, true)
}

func (s *Server) notify(method string, params any, priority bool) {
	if s.notifyClosed.Load() {
		return
	}
	data, err := json.Marshal(&notification{JSONRPC: "2.0", Method: method, Params: params})
	if err != nil {
		s.log.Err(err).Str("method", method).Msg("Failed to encode notification")
		return
	}
	defer func() { _ = recover() }() // send on closed channel during shutdown races
	if priority {
		select {
		case s.priorityCh <- data:
		case <-s.closed:
		}
		return
	}
	select {
	case s.notifyCh <- data:
	default:
		if !s.overflow.Swap(true) {
			s.log.Warn().Msg("Notification queue overflowed; dropping until drained")
		}
	}
}

func (s *Server) notifyLoop() {
	defer close(s.notifyDone)
	for {
		select {
		case data := <-s.priorityCh:
			s.writeLine(data)
			continue
		default:
		}
		select {
		case data := <-s.priorityCh:
			s.writeLine(data)
		case data := <-s.notifyCh:
			s.writeLine(data)
			if len(s.notifyCh) == 0 && s.overflow.Swap(false) {
				resync, _ := json.Marshal(&notification{JSONRPC: "2.0", Method: "engine.resync", Params: map[string]string{"reason": "notification_overflow"}})
				s.writeLine(resync)
			}
		case <-s.closed:
			for {
				select {
				case data := <-s.priorityCh:
					s.writeLine(data)
				case data := <-s.notifyCh:
					s.writeLine(data)
				default:
					return
				}
			}
		}
	}
}

// WaitInflight waits (up to timeout) for running requests to finish. It reports whether they all did. Requests the
// gate lets through before this call are waited for; to wait for every request, make the gate refuse new ones first.
func (s *Server) WaitInflight(timeout time.Duration) bool {
	// A barrier: a dispatch that passed the gate has counted itself in flight before this returns.
	s.gateMu.Lock()
	s.gateMu.Unlock()
	done := make(chan struct{})
	go func() {
		s.inflight.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// Close drains queued notifications and stops the notification writer. Later notifications are discarded.
func (s *Server) Close() {
	s.closeOnce.Do(func() {
		s.notifyClosed.Store(true)
		close(s.closed)
		<-s.notifyDone
	})
}

// Decode strictly decodes params into v: unknown fields and trailing data are rejected with -32602. Missing or null
// params decode as an empty object.
func Decode(params json.RawMessage, v any) error {
	if len(params) == 0 || bytes.Equal(bytes.TrimSpace(params), nullID) {
		params = json.RawMessage("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(params))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return InvalidParams("bad params: %s", describeDecodeError(err))
	}
	if dec.More() {
		return InvalidParams("bad params: trailing data")
	}
	return nil
}

// describeDecodeError keeps the error short and free of values from the input.
func describeDecodeError(err error) string {
	var typeErr *json.UnmarshalTypeError
	var syntaxErr *json.SyntaxError
	switch {
	case errors.As(err, &typeErr):
		return fmt.Sprintf("field %q must be %s", typeErr.Field, typeErr.Type.String())
	case errors.As(err, &syntaxErr):
		return "invalid JSON"
	default:
		msg := err.Error()
		if len(msg) > 120 {
			msg = msg[:120]
		}
		return msg
	}
}
