// Command cappy-engine is Cappy Comm's messaging engine sidecar: the mautrix bridgev2 WhatsApp and Telegram connectors
// in one process, with a local Matrix connector writing to SQLite, driven by JSON-RPC 2.0 over stdin/stdout.
//
// Usage: cappy-engine --data-dir <absolute path> [--log-level info] [--memory-limit 256MiB]
//
// Exit codes: 0 clean stop, 1 error or hard stop deadline, 2 bad usage, 3 data folder locked by another engine,
// 4 data folder belongs to another program or to a newer engine.
//
// Copyright (C) 2026 Cappy Comm contributors. AGPL-3.0-or-later; see LICENSE.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"

	"github.com/rs/zerolog"

	"cappy.local/engine/internal/engine"
	"cappy.local/engine/internal/inbox"
	"cappy.local/engine/internal/logredact"
)

const defaultMemoryLimit = 256 << 20

func main() {
	os.Exit(run())
}

func run() int {
	fs := flag.NewFlagSet("cappy-engine", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	dataDir := fs.String("data-dir", "", "absolute path of the engine's data folder (required)")
	logLevel := fs.String("log-level", "info", "log level: debug, info, warn or error")
	memLimit := fs.String("memory-limit", "", "soft memory limit, eg 256MiB (default: GOMEMLIMIT, else 256MiB)")
	showVersion := fs.Bool("version", false, "print the version and exit")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return 2
	}
	if *showVersion {
		fmt.Fprintf(os.Stderr, "cappy-engine %s\n", engine.Version)
		return 0
	}
	if fs.NArg() != 0 || *dataDir == "" || !filepath.IsAbs(*dataDir) {
		fmt.Fprintln(os.Stderr, "usage: cappy-engine --data-dir <absolute path> [--log-level info] [--memory-limit 256MiB]")
		return 2
	}
	level, err := zerolog.ParseLevel(*logLevel)
	if err != nil || level == zerolog.NoLevel {
		fmt.Fprintln(os.Stderr, "invalid --log-level")
		return 2
	}
	if *memLimit != "" {
		limit, err := parseBytes(*memLimit)
		if err != nil || limit <= 0 {
			fmt.Fprintln(os.Stderr, "invalid --memory-limit")
			return 2
		}
		debug.SetMemoryLimit(limit)
	} else if os.Getenv("GOMEMLIMIT") == "" {
		debug.SetMemoryLimit(defaultMemoryLimit)
	}

	// Keep the real stdout for RPC only. File descriptor 1 and os.Stdout now go to stderr, so a stray print in any
	// library (Go or C) cannot corrupt the protocol stream.
	rpcOut, err := captureStdout()
	if err != nil {
		fmt.Fprintln(os.Stderr, "failed to capture stdout:", err)
		return 1
	}

	redactor := logredact.New(os.Stderr)
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	logger := zerolog.New(redactor).Level(level).With().Timestamp().Logger()
	zerolog.DefaultContextLogger = &logger
	log.SetFlags(0)
	log.SetOutput(redactor)

	if err = os.MkdirAll(*dataDir, 0o700); err != nil {
		logger.Error().Err(err).Msg("Failed to create data folder")
		return 1
	}
	_ = os.Chmod(*dataDir, 0o700)
	lock, err := lockDataDir(*dataDir)
	if err != nil {
		logger.Error().Err(err).Msg("Data folder is in use by another engine")
		return 3
	}
	defer lock.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	eng, err := engine.New(ctx, engine.Options{
		DataDir:      *dataDir,
		Log:          logger,
		OnInstallKey: redactor.SetKey,
	})
	if errors.Is(err, inbox.ErrNewerSchema) || errors.Is(err, inbox.ErrForeignOwner) {
		logger.Error().Err(err).Msg("Refusing to open data folder")
		return 4
	} else if err != nil {
		logger.Error().Err(err).Msg("Failed to open data folder")
		return 1
	}
	logger.Info().Str("version", engine.Version).Int64("memory_limit", debug.SetMemoryLimit(-1)).Msg("Engine ready for hello")
	return eng.Run(ctx, os.Stdin, rpcOut)
}

func captureStdout() (*os.File, error) {
	fd, err := syscall.Dup(1)
	if err != nil {
		return nil, err
	}
	syscall.CloseOnExec(fd)
	if err = syscall.Dup2(2, 1); err != nil {
		return nil, err
	}
	os.Stdout = os.Stderr
	return os.NewFile(uintptr(fd), "rpc-stdout"), nil
}

// lockDataDir takes an exclusive, non-blocking flock on engine.lock and writes the pid into it.
func lockDataDir(dir string) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(dir, "engine.lock"), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, err
	}
	_ = f.Truncate(0)
	_, _ = f.WriteAt([]byte(strconv.Itoa(os.Getpid())+"\n"), 0)
	return f, nil
}

// parseBytes parses sizes like 268435456, 256MiB, 256MB, 1GiB.
func parseBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	units := []struct {
		suffix string
		mult   int64
	}{{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}, {"GB", 1e9}, {"MB", 1e6}, {"KB", 1e3}, {"B", 1}}
	for _, u := range units {
		if num, ok := strings.CutSuffix(s, u.suffix); ok {
			n, err := strconv.ParseInt(strings.TrimSpace(num), 10, 64)
			if err != nil {
				return 0, err
			}
			return n * u.mult, nil
		}
	}
	return strconv.ParseInt(s, 10, 64)
}
