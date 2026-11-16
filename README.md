# Cappy Engine

Cappy Engine is a local helper program (sidecar) for Cappy Comm. It runs the
open-source mautrix bridgev2 connectors for WhatsApp and Telegram in **one Go
process**. A custom local Matrix connector (`internal/localmx`) replaces the
Matrix homeserver, so there is no homeserver and nothing goes to Beeper. Chats
and messages are stored in local SQLite with FTS5 search. The Rust host talks
to the engine with JSON-RPC 2.0 over stdin/stdout. The engine opens no network
port.

License: **AGPL-3.0-only** (see `LICENSE`, `NOTICE.md` and
`third_party/licenses/`). This is a private, local repository. Never push or
publish it.

## Status (2026-11-13): spike implemented

The go/no-go spike from `DESIGN.md` is built and tested by agents. Signing in
to real accounts is still the owner's job (T8 and T9).

A review pass then fixed five problems: a foreign `inbox.db` was adopted
instead of refused, shutdown could race a `hello` still starting bridges (and a
send or login that had already passed the gate), deleted messages kept their
text in `raw_events`, a `purgeLocal` logout broke a later re-login of the same
account, and a merged WhatsApp DM stayed listed twice. Every check below was
re-run on the rebuilt binary after those fixes.

| Check (DESIGN.md §19) | Result |
| --- | --- |
| T0 build and vet | Pass. One binary with both connectors. |
| T1 localmx against a fake network | Pass. See `internal/engine/e2e_test.go` and `internal/localmx`. |
| T2 protocol against the real binary | Pass. See `test/protocol_test.go`, including exit 4 for a foreign or non-SQLite `inbox.db` (file left unchanged). |
| T3 no listening sockets | Pass. Checked with `lsof` in T2 and T4. |
| T4 WhatsApp QR shape (opt-in, live) | Pass on 2026-11-13 (re-run after the fixes, `dist/cappy-engine`). The first step was `display_and_wait`, step `fi.mau.whatsapp.login.qr`, display `qr`, with 5 comma-separated fields. The QR was never shown or saved. `login.cancel` and a clean shutdown followed. |
| T5 Telegram registered but not configured | Pass. State `needs_configuration`, 4 login flows. Telegram was never contacted. |
| T6 memory | Idle with no accounts: **about 37 MiB RSS** (`ps -o rss`, `GOMEMLIMIT=256MiB`, after `hello`). About 31 MiB before `hello`, and about 46 MiB while the WhatsApp QR step was showing. |
| T7 no canaries in the logs | Pass. Synthetic message text and names never reached stderr. |
| Universal macOS binary | Built: `dist/cappy-engine` (x86_64 + arm64, about 125 MB, stripped). Both slices pass T2; x86_64 was run under Rosetta. |
| Race detector | Pass: `go test -race` over every package, including a test that closes stdin while `hello` is still building the bridges. |

## Build, run and test

```sh
cd Cappy_Engine
source ./env.sh                         # local Go 1.27.4; caches stay in .gopath/ and .gocache/

go vet -tags sqlite_fts5 ./...
go test -count=1 -tags sqlite_fts5 ./...          # unit, end-to-end and binary protocol tests (no network)
go test -count=1 -race -tags sqlite_fts5 ./...    # the same with the race detector

go build -tags sqlite_fts5 -o tmp.noindex/bin/cappy-engine ./cmd/cappy-engine   # development build
./scripts/build-universal.sh                      # universal macOS binary in dist/
```

- The `sqlite_fts5` build tag is **mandatory**. Without it, search fails at
  runtime.
- cgo is needed (Xcode command-line tools: `clang` and `lipo`).
- The binary protocol tests build their own engine into
  `tmp.noindex/testbin/`. Set `CAPPY_ENGINE_BIN=/path/to/cappy-engine` to test
  another build, for example `dist/cappy-engine`.

**The live WhatsApp QR check (T4)** is opt-in. It contacts WhatsApp without
authentication and signs in to nothing:

```sh
CAPPY_WA_QR_SMOKE=1 go test -count=1 -v -tags sqlite_fts5 -run TestWhatsAppQRSmoke ./test/
```

It reports only the step type, display type and number of fields. It never
prints, renders or saves the QR. Do not scan anything.

**Run the engine by hand:**

```sh
GOMEMLIMIT=256MiB tmp.noindex/bin/cappy-engine --data-dir "$PWD/tmp.noindex/data"
# one JSON object per line on stdin, for example:
{"jsonrpc":"2.0","id":1,"method":"hello","params":{"protocol":1,"client":{"name":"manual","version":"0"}}}
{"jsonrpc":"2.0","id":2,"method":"networks.list"}
{"jsonrpc":"2.0","id":3,"method":"shutdown"}
```

- Flags: `--data-dir` (absolute path, required), `--log-level`
  (default `info`), `--memory-limit` (for example `256MiB`; otherwise
  `GOMEMLIMIT`, otherwise 256 MiB) and `--version`.
- Exit codes: 0 clean stop, 1 error or the 10 s hard stop, 2 bad usage,
  3 data folder locked by another engine, 4 `inbox.db` is not this engine's
  or was written by a newer engine.
  - "Not this engine's" means: a SQLite file with another `application_id`,
    with tables, views, indexes or triggers but no Cappy `engine_meta` owner
    row, with an `engine_meta` naming another owner, or a file that is not
    SQLite at all. An empty file is adopted.
  - The check runs on a separate query-only connection before anything is
    written, so a refused file is left byte-for-byte unchanged (no WAL switch,
    no `bridge.db`).
  - Cappy's own `inbox.db` carries `PRAGMA application_id = 0x43415059`
    ("CAPY"). Files from the first spike build get it on their next open.
- To pass Telegram credentials (owner only, never on argv or in env), add
  `"telegram":{"apiId":…,"apiHash":"…"}` to `hello`.

## Running T8

T8 is the owner's real-account WhatsApp test (DESIGN.md §19). Only the owner
runs `login` and scans the QR, with the second WhatsApp account. `cmd/cappy-t8`
drives `dist/cappy-engine` over JSON-RPC and prints only content-free lines:
account IDs and states, chat and `message.new` counts, and engine RSS. It never
prints message text, names, phone numbers, chat titles or the QR payload. The
engine's redacted log goes to `tmp.noindex/t8-engine.log`.

```sh
cd Cappy_Engine
source ./env.sh
go build -tags sqlite_fts5 -trimpath -o dist/cappy-t8 ./cmd/cappy-t8

# 1. Log in. The QR opens as tmp.noindex/t8-qr.png (re-rendered when WhatsApp
#    rotates it, deleted when the login ends). Scan it in WhatsApp > Linked
#    devices. After the login, the tool keeps running as `watch`.
./dist/cappy-t8 login

# 2. Watch: a status line every 30 s; Ctrl-C stops the engine cleanly.
#    For the 14-day soak, keep a copy of the lines (tee -i ignores Ctrl-C, so
#    the tool still shuts the engine down):
./dist/cappy-t8 watch 2>&1 | tee -i -a tmp.noindex/t8-watch.log

# 3. Restart check: one status line, then a clean stop. Exit 0 means every
#    account came back logged in and CONNECTED without a new login.
./dist/cappy-t8 status

# 4. Send check, dry run (the default; sends nothing): find the one direct chat
#    that received an incoming message between 2026-11-13T22:26:50-06:00 and
#    22:27:35-06:00 (where message.new went 0 -> 1) and print its opaque ID.
#    If it shows 0 matches, you may widen only --window-start (never the end),
#    for example --window-start 2026-11-13T22:20:00-06:00, and run it again.
./dist/cappy-t8 send-test

# 5. Send check, for real (owner-approved): exactly one "Cappy engine test" to
#    the chat the dry run printed; prints the message ID and its status, then
#    stops the engine. Pass the same --window-start if you widened it.
./dist/cappy-t8 send-test --dry-run=false --yes --chat <ID from the dry run>
```

- Defaults: `--engine ./dist/cappy-engine`, `--data-dir ./tmp.noindex/t8-data`,
  `--engine-log ./tmp.noindex/t8-engine.log`, `--interval 30s` (watch),
  `--login-timeout 5m` (login), `--wait 30s` (status). The engine runs with
  `GOMEMLIMIT=256MiB` and a cleared environment, in its own process group.
- Only one engine can use a data folder: stop `watch` before `status`.
- `login` refuses to start when a WhatsApp account is already logged in to
  the data folder. On a timeout, a failure or Ctrl-C it prints the reason,
  calls `login.cancel` and stops the engine.
- The receive and soak steps of T8 happen in WhatsApp on the phone; the
  status line shows the `message.new` count and the memory for each of them.
- `send-test` waits (up to `--wait`) for the one logged-in WhatsApp account to
  be CONNECTED, lists all of its chats (archived too) and pages each chat's
  messages for incoming messages inside `--window-start` .. `--window-end`
  (RFC3339, both ends included, at most 1 hour). A message is incoming only
  if it is not `fromMe`, not a timeline update, and has a sender contact who
  is not a bot; notices without a sender (`sender` null) do not count.
  - It goes on only if exactly one chat matched, that chat is a direct (`dm`)
    chat with a contact who is not a bot, and every matched message was sent
    by that chat's contact. Zero or several matches, a group, group DM,
    status broadcast or newsletter (listed as `group`), a matched message
    from another contact, or any engine error is refused with a count-only
    reason and exit 1; nothing is sent.
  - The dry run prints only the matched chat count, the chat's opaque ID, its
    kind and the matched message timestamp(s). Names, numbers, titles and
    text (including `--text`) are never decoded or printed.
  - A real send needs `--dry-run=false`, `--yes` and `--chat` with the chat
    ID the dry run printed. Without `--chat` (or with an ID that is not
    `cht_…`) it is refused with exit 2 before the engine starts; `--dry-run`
    or `--yes` alone is refused or stays a dry run. If the selected chat is
    not the `--chat` chat, it is refused (exit 1) and nothing is sent; a dry
    run given `--chat` checks it the same way. It makes exactly one
    `messages.send` and never retries. It waits up to 60 s for the
    `message.updated` status and prints only the message ID and status:
    exit 0 for `sent`, `delivered` or `read`, exit 1 for `failed` or no final
    status.
  - The `clientTxn` is derived from a fixed T8 constant and the chat ID only
    (not the text or the window), so a re-run for the same chat will not
    send: the engine returns the first run's message and the tool says so
    (exit 1), whatever `--text` or window. Check the phone before ever
    sending again.
  - Other flags: `--text` (default `Cappy engine test`). The send-test flags
    (including `--chat`) are refused on the other subcommands.

## What works

- **One process, two networks.**
  - WhatsApp (mautrix-whatsapp v0.2610.0) and Telegram (mautrix-telegram
    v0.2610.0) are both registered against mautrix-go v0.32.0.
  - Each has its own `bridgev2.Bridge` and local Matrix connector, and both
    share one `bridge.db`.
  - Both start, and both stop cleanly in parallel.
- **Local Matrix connector (`internal/localmx`).** It implements every
  `MatrixConnector` and `MatrixAPI` method in DESIGN.md §9–10:
  - HMAC-based deterministic room and event IDs.
  - Rooms become chats; ghosts become contacts.
  - Messages, edits, reactions, redactions, receipts, read markers, tags and
    mute.
  - Idempotent `BatchSend` backfill.
  - `GetEvent` from stored raw events, for WhatsApp quoted replies.
  - Deleted and disappeared messages lose their text everywhere in
    `inbox.db`: the message row, its stored raw event and the raw events of
    its edits (an edit arriving after the deletion is stored blank). A
    removed reaction's raw event is deleted. `inbox.db` runs with
    `secure_delete=on`, so replaced content is zeroed rather than left in free
    pages.
  - A tombstone (bridgev2 merging a room into another, for example a WhatsApp
    phone-number DM into its LID chat) hides the old chat, so the host does
    not list the conversation twice.
  - A content-addressed media store.
  - Bridge state becomes `account.state`. Message status becomes
    `message.updated`.
- **Store (`internal/inbox`).** The `inbox.db` schema from DESIGN.md §8, with
  FTS5:
  - Diacritic folding.
  - Prefix match on the last term.
  - Operator injection blocked by quoting each term.
  - Cursor paging for chats, messages and search.
  - Unread counts kept in the same transaction as each write.
  - An install key.
- **JSON-RPC server (`internal/rpc`).**
  - NDJSON framing with an 8 MiB line cap.
  - Strict params decoding (-32602).
  - -32700, -32600 and -32601 errors.
  - Panic recovery.
  - 16 concurrent requests.
  - A bounded notification queue with `engine.resync` on overflow.
- **Engine (`internal/engine`).** Every request and notification in DESIGN.md
  §13 is implemented:
  - Requests: `hello`, `networks.list`, `login.start`, `login.submit`,
    `login.cancel`, `accounts.list`, `accounts.logout`, `chats.list`,
    `chats.markRead`, `media.preview`, `messages.list`, `messages.send`,
    `search`, `ping`, `engine.stats` and `shutdown`.
  - Notifications: `login.step`, `account.state`, `chat.updated` (debounced
    250 ms per chat), `message.new`, `message.updated` and `engine.resync`.
  - Login sessions have their own context, a waiter that pushes rotated QR
    codes, and cancel.
  - Text sending is end to end through `QueueMatrixEvent`, with `clientTxn`
    deduplication.
  - Interrupted sends are resolved at the next start.
- **Process (`cmd/cappy-engine`).**
  - fd 1 is kept for RPC only; stray prints go to stderr.
  - An `flock` on the data folder.
  - Redacted JSON logs (`internal/logredact`).
  - `GOMEMLIMIT`.
  - SIGTERM, SIGINT, stdin EOF and `shutdown` all stop gracefully, in this
    order: refuse new requests, wait for requests in flight (2 s), cancel
    logins, wait for queued sends (2 s), wait for a `hello` still building the
    bridges, stop every bridge (5 s), flush notifications, close the stores.
    `hello`, `login.start` and `messages.send` re-check "stopping" under their
    own locks, so none of them can start a bridge, a WhatsApp login session or
    a send behind the stop sequence (tested with `-race`).
- **`accounts.logout` with `purgeLocal`** also deletes the account's bridgev2
  portals (rows, message mappings and rooms) before it deletes the inbox chats.
  Logging the same account in again therefore recreates its chats and backfills
  them, instead of dropping every event for rooms that no longer exist. If the
  portals cannot be deleted, the account is still logged out, its chats are
  kept and the request fails.

## Unproven or not done

- **Real WhatsApp account** (QR scan, first sync, receive, send, restart
  without re-login, 14-day soak). This is T8. Only the owner may run it, with
  the second WhatsApp account.
- **Telegram networking and login** (T9). These are blocked until the owner
  registers an `api_id`/`api_hash` at my.telegram.org. Telegram compiles,
  registers and stays unconfigured.
- **Write contention on the shared `bridge.db`** under real traffic from both
  networks at once. Both bridges start, migrate and stop on one file. A load
  with real accounts has not happened. The fallback is one `bridge-<network>.db`
  per network (DESIGN.md §22).
- **Memory with accounts** (targets: 150 MB steady and 400 MB at the first-sync
  peak). Only idle and QR-step memory are measured.
- **Signing and notarizing** the cgo sidecar for Tauri has not been tried.
- **Deleted text in the search index and WAL.** The FTS5 index drops a deleted
  message's words logically at once, but the old index segment keeps them on
  disk until FTS5 merges it. The WAL keeps old pages until the next checkpoint
  (on a clean stop it is truncated). A periodic FTS5 `optimize` and checkpoint
  would close this; not done in the spike.
- **Out of scope for the spike** (DESIGN.md §21):
  - Sending media, reactions or edits from Cappy.
  - Paging older history from the network.
  - WhatsApp passkey (`webauthn`) steps. These answer `unsupported_step`.
  - A Windows build.
- **AGPL.** Any distributed binary needs a source offer. Whether the Rust host
  is affected is a legal question for the owner (not legal advice).

## Choices and differences from DESIGN.md

- **SQLite driver: `mattn/go-sqlite3` (cgo) with `-tags sqlite_fts5`, not
  `modernc.org/sqlite`.**
  - mautrix's `dbutil` (the `sqlite3-fk-wal` driver) and whatsmeow's store
    already use mattn, so `bridge.db` and `inbox.db` share one SQLite build
    and one set of pragmas.
  - cgo cross-compiles to x86_64 with `clang -arch x86_64`, so the universal
    build works.
  - modernc would only matter if cgo became a problem, for example for a
    Windows cross-build later.
- **`message.new` for messages sent from the owner's other devices.**
  `message.new` is also sent for live messages the owner sent from another
  device (`fromMe: true`), so the host sees them without reloading. Local
  sends from `messages.send` are not repeated, because the response already
  carries them. Backfill never produces `message.new`.
- **Smaller deviations:**
  - The `messages` primary key column is `id` (DESIGN.md says `rowid`).
  - `inbox.db` schema version 2 adds `raw_events.replaces` (the event an edit
    replaces, indexed). Version 1 files are upgraded in place, and the stored
    content of messages they had already deleted is blanked.
  - `login.start` returns `{"loginID","completed":Account}` when the first
    step completes the login.
  - `networks.list` adds `bridgeState`.
  - `engine.stats` adds `maxRssBytes`. `rssBytes` is the current RSS from
    mach `task_info`.
  - A foreign or newer `bridge.db` makes `hello` fail with 1003; it does not
    make the process exit with 4.
  - The log level is one global level, so whatsmeow is not lowered to
    warn separately; redaction still applies to every line.
  - `login.step` and `account.state` notifications are never dropped on
    queue overflow.

## Folder map

- `cmd/cappy-engine/`: the binary: flags, stdout capture, lock, logger,
  memory limit, signals and exit codes.
- `cmd/cappy-t8/`: the owner's T8 helper (`login`, `watch`, `status`,
  `send-test`).
- `internal/engine/`: builds the bridges, handles RPC requests, runs login
  sessions, sends messages and notifications, and reports stats. It also holds
  the end-to-end tests with the fake network.
- `internal/localmx/`: the local MatrixConnector and MatrixAPI. Parts are
  adapted from bottled-bridge-core (AGPL-3.0), as noted in the file headers.
- `internal/inbox/`: the `inbox.db` schema, writes, queries, FTS5 and the
  host-facing JSON shapes.
- `internal/rpc/`: the JSON-RPC 2.0 stdio server.
- `internal/media/`: the content-addressed file store.
- `internal/netcfg/`: connector configs, as the embedded example YAML plus
  Cappy overrides, decoded once.
- `internal/logredact/`: the redacting log writer.
- `internal/fakenet/`: an in-process fake network connector for tests.
- `internal/pincheck/`: version-pin regression tests.
- `test/`: tests that spawn the real binary (protocol, T3 socket check and the
  opt-in T4 WhatsApp QR check).
- `scripts/build-universal.sh`: builds the universal macOS binary.
- `DESIGN.md`: the design. `env.sh`: the Go environment.
- Git-ignored: `.gopath/`, `.gocache/`, `upstream/` (read-only source
  checkouts), `dist/` (build output) and `tmp.noindex/` (test data,
  Spotlight-skipped).

## T8 results, 2026-11-13 (owner's second WhatsApp account, Mac arm64)

Run with `dist/cappy-t8` against `tmp.noindex/t8-data`. Only IDs, states and
counts were recorded; no message content, names or numbers were read.

| Check | Result |
| --- | --- |
| QR login | Pass. Owner scanned the QR; CONNECTING to CONNECTED in about 3 s. |
| First history sync | Pass. 39 chats within about 90 s. |
| Live sync of own messages | Pass. A message sent from the account's own phone at 22:27:24 arrived live as `message.new` (fromMe, source live). |
| Live receive from another person | Not yet tested. The 2026-11-13 message was outgoing (checked on 2026-11-15 by counts only: from_me=1, no sender), so `send-test` correctly found no incoming chat. |
| Restart after two days offline | Pass. On 2026-11-15 the account reconnected in about 1 s without a new login. |
| Restart without re-login | Pass. Clean stop (exit 0, 403 ms), restart reconnected in about 1 s with 39 chats and no QR. |
| Memory | 71.4 MiB peak during sync; 45.5 MiB after restart. Target was 150 MiB steady. |
| Send from Cappy | Skipped by the owner on 2026-11-15. `send-test` is built, reviewed and guarded, but no message was sent. |
| Live receive from another person | Still untested (no incoming message reached the account during the 2026-11-15 listening window). |
| 14-day soak | Not run yet. |
