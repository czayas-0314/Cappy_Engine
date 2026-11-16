# Cappy Engine: design for the go/no-go spike

Status: **spike implemented** (2026-11-13). The code follows this design;
`README.md` lists what passed (T0–T7), what is still unproven and where the code
differs from this document. Nothing has signed in to any account. Telegram has
never been contacted. WhatsApp was contacted once, without authentication, for
the opt-in QR shape check (T4).

The engine is a helper program (sidecar) for Cappy Comm. It runs the
open-source mautrix bridgev2 network connectors for WhatsApp and Telegram in
**one Go process**. It replaces bridgev2's Matrix side with a **local
MatrixConnector**, so there is no homeserver and nothing goes to Beeper. Chats,
contacts and messages are stored in local SQLite with FTS5 search. The Rust
host talks to the engine through **JSON-RPC over stdin/stdout**, and the engine
opens no network port. This document covers roadmap step 3 of
`Cappy_Comm/Analysis/best-approach-2026-11-13.md`.

---

## 1. Verdict and evidence

**Feasible at design level.** The evidence:

| Claim | Evidence |
| --- | --- |
| WhatsApp and Telegram share one mautrix/go version | The latest tags `mautrix-whatsapp v0.2610.0` and `mautrix-telegram v0.2610.0` both require `maunium.net/go/mautrix v0.32.0` and `go.mau.fi/util v0.11.0`. No fallback commit was needed. |
| Both connectors build together in one module | `internal/pincheck` imports both `pkg/connector` packages. `go build` and `go vet` pass with go1.27.4. `go mod verify` passes. |
| The connectors are importable libraries | Both live in `pkg/connector`, not `internal/`. The upstream `main.go` files only wrap them in `mxmain.BridgeMain`. |
| No connector hard-casts to the real Matrix connector | Searching both connectors and bridgev2 finds no cast to `*matrix.Connector` or `*matrix.ASIntent`. Every other cast is to an optional interface and has a fallback. See section 3. |
| FTS5 is available | `internal/pincheck/fts5_test.go` passes with `-tags sqlite_fts5` and fails without it. **The build tag is mandatory.** |
| Connector defaults load without mxmain or YAML files | `internal/pincheck/config_test.go` decodes both embedded example configs. Telegram's `ValidateConfig` rejects the placeholder `api_hash`. |
| A local MatrixConnector is a known working pattern | `cloud-in-a-bottle/bottled-bridge-core` (`localmx/`, AGPL-3.0, commit `e801a224`) runs the WhatsApp connector against a SQLite store with no homeserver. It uses one network per process. |

**Unproven until the spike runs:** two bridgev2 bridges in one process sharing
one bridge database, the connection stability of a real WhatsApp account,
memory use, the Telegram network path (blocked on an owner `api_id`), and
signing the cgo sidecar.

## 2. Pinned versions (verified)

| Module | Version | Commit / date |
| --- | --- | --- |
| `maunium.net/go/mautrix` (mautrix/go) | **v0.32.0** | `aae5273d1463cd69dd2f95eb2dd62e8bd9084c4d`, 2026-10-16 |
| `go.mau.fi/mautrix-whatsapp` | **v0.2610.0** | `1067291a89f4949887b328104005abe4f7163ddf`, 2026-10-16 |
| `go.mau.fi/mautrix-telegram` (Go rewrite, gotd fork in `pkg/gotd`) | **v0.2610.0** | `6991248e6a573489d2d38d603b5dbed1627113bc`, 2026-10-16 |
| `go.mau.fi/util` | v0.11.0 | required by both |
| `go.mau.fi/whatsmeow` | v0.0.0-20261016130820-0b0a1cdac3e2 | from the WhatsApp go.mod |
| `github.com/mattn/go-sqlite3` | v1.14.52 | cgo; FTS5 needs tag `sqlite_fts5` |
| Go toolchain | go1.27.4 local (`GOTOOLCHAIN=local`) | upstream go.mod says `go 1.26.0`, toolchain go1.27.2 |

Minimal version selection changed only three helper modules from what the
upstream go.mod files list: `mattn/go-colorable` 0.1.14 to 0.1.15,
`rogpeppe/go-internal` 1.10 to 1.16 and `sergi/go-diff` 1.1 to 1.3.1. These
cover terminal color and test helpers. Everything on the protocol path is
identical to upstream. mautrix releases monthly, so the pin moves in lockstep
(section 20).

## 3. Upstream facts the design depends on

Paths are relative to each module root at the pinned versions.

1. **Bot intent is mandatory.** `bridgev2/bridge.go` `NewBridge` runs
   `br.Matrix.Init(br)`, then `br.Bot = br.Matrix.BotIntent()`, then
   `br.Network.Init(br)`. `BotIntent()` must therefore return a working,
   non-nil `MatrixAPI` straight after `Init`, before `Start`.
   `Portal.GetIntentFor` panics with "bridge bot is nil" when it falls back
   to a nil bot. The connectors call the bot directly:
   - WhatsApp: `Bot.GetEvent` (caption edits, quoted replies),
     `Bot.DownloadMedia` (outgoing media), `Bot.SendState` (LID migration),
     and the bot as the backfill intent.
   - Telegram: `Bot.SendMessage`, `Bot.DownloadMediaToFile`.
   - bridgev2: `Bot.CreateRoom` and `Bot.DeleteRoom`.
2. **Command processor cast is mandatory.** Both connectors' `Init` run
   `Bridge.Commands.(*commands.Processor).AddHandlers(...)`. `NewBridge`
   must receive `commands.NewProcessor`, or `Init` panics. User text never
   reaches the processor (section 6).
3. **Startup order.** `StartConnectors` runs `DB.Upgrade` (skipped when
   `ExternallyManagedDB`), then `MigrateToSplitPortals`, `Matrix.Start` and
   `Network.Start`. Next, `StartLogins` connects every stored login. Finally
   `PostStart` resends bridge info. `stop()` closes the DB unless
   `ExternallyManagedDB` is set.
4. **`SendGlobalBridgeState` retries until `SendBridgeStatus` returns nil.**
   The local connector must therefore never return an error there.
5. **Room creation.** `createMatrixRoomInLoop` calls `Bot.CreateRoom` with
   `BeeperLocalRoomID = GenerateDeterministicRoomID(portalKey)`. The `m.bridge`
   initial state carries `Channel.ID` (portal ID), `Channel.Receiver` and
   `BeeperRoomTypeV2`, so the portal key can be recovered from the request
   itself. Management rooms (`user.go`) and spaces (`space.go`) are also
   created through `CreateRoom`. Management rooms have no deterministic ID.
6. **Backfill.** `GetCapabilities().BatchSending` makes `sendBatch` call
   `Matrix.BatchSend` with events whose IDs come from
   `GenerateDeterministicEventID` and `GenerateReactionEventID`. The backfill
   queue only runs when `BatchSending` is true.
7. **Bridge rows are written after the local write.** For both live
   (`sendConvertedMessage`) and batch (`sendBatch`) messages, bridgev2 inserts
   its `message` row after `SendMessage`/`BatchSend` returns. Local writes
   must therefore be **idempotent**, using deterministic IDs and upserts.
8. **Direct media is opt-in.** `GenerateContentURI` and `ParseContentURI` are
   only reached when direct media is on (`mc.DirectMedia`,
   `tc.useDirectMedia`). They can return `ErrDirectMediaNotEnabled`.
   WhatsApp's `Start` fails if `lazy_avatars` is on without direct media.
9. **All bridgev2 tables are keyed by `bridge_id`**, including `kv_store`, as
   shown in `bridgev2/database/upgrades/00-latest.sql`. Each connector keeps
   its own version table through `DB.Child`: `whatsmeow_version`,
   `whatsapp_version` and `telegram_version`. The `whatsmeow_*` tables are
   keyed by device JID, not by `bridge_id`, so only **one WhatsApp bridge per
   database** is possible. Use one WhatsApp bridge with several logins.
10. **WhatsApp has process-global state.** These are `store.DeviceProps` and
    `store.SetWAVersion`. The first login calls `whatsmeow.GetLatestVersion`,
    an unauthenticated HTTPS request to WhatsApp. `WALogin.Wait` **cancels the
    login when its context ends**. QR codes rotate on the schedule 60 s,
    then 5 × 20 s, and the attempt times out after about 160 s with
    `FI.MAU.WHATSAPP.LOGIN_TIMEOUT`.
11. **Telegram needs the owner's `api_id`/`api_hash`.** `ValidateConfig`
    enforces this. `Start` only upgrades the `telegram_*` tables, and nothing
    contacts Telegram until a login or a stored session exists.
12. **Optional interfaces are type-asserted with fallbacks.** These are:
    `MatrixConnectorWithArbitraryRoomState`, `WithNameDisambiguation`,
    `WithBridgeIdentifier`, `WithURLPreviews` (WhatsApp outgoing previews),
    `WithNotifications` (Telegram push), `WithAnalytics`, `WithBeeperStreams`,
    `WithHTTPSettings`, `WithPostRoomBridgeHandling`,
    `StreamOrderReadingMatrixAPI` and `MarkAsDMMatrixAPI`.
13. **Split portals.** With `SplitPortals`, every portal key carries the
    owning login as receiver. This is set in WhatsApp `id.go` and Telegram
    `ids.go`. `FindPreferredLogin` then sends with exactly that account. Once
    on, it cannot be turned off (`ErrCannotDisableSplitPortals`).
14. **Template compilation.** The connector `Config.UnmarshalYAML` methods
    compile `displayname_template`. Overrides must therefore be merged into
    the YAML before the single decode, not assigned to struct fields
    afterwards.

## 4. Architecture

```
 Cappy Comm (Rust/Tauri host)
   | spawns sidecar; JSON-RPC 2.0, one JSON object per line on stdin/stdout
   | stderr = redacted JSON logs
   v
 cappy-engine (Go, one process, no listening sockets)
   rpc.Server -> engine.Engine (login sessions, accounts, chats, send, search, notifier)
     bridge "whatsapp": bridgev2.Bridge{ID:"whatsapp"}
         Network = mautrix-whatsapp WhatsAppConnector (whatsmeow)
         Matrix  = localmx.Connector{network:"whatsapp"}
     bridge "telegram": bridgev2.Bridge{ID:"telegram"}
         Network = mautrix-telegram TelegramConnector (gotd fork)
         Matrix  = localmx.Connector{network:"telegram"}
   bridge.db  one dbutil.Database, owner "cappy-engine", shared by both bridges:
              bridgev2 core tables (bridge_id = whatsapp | telegram)
              + whatsmeow_* + whatsapp_* + telegram_*
   inbox.db   Cappy's own schema + FTS5; written only by localmx and engine
   media/     content-addressed attachments and avatars
```

Each network has one `bridgev2.Bridge`. One owner user,
`@owner:cappy.local`, holds **many logins** per network, for example two
WhatsApp accounts. Each bridge gets its own `localmx.Connector` instance. All
instances share the inbox store, media store, notifier and install key.

## 5. Package layout

```
Cappy_Engine/
  cmd/cappy-engine/main.go   flags, stdout capture, lock, logger, GOMEMLIMIT, signals, exit codes
  internal/rpc/              JSON-RPC 2.0 over stdio: line reader (8 MiB cap), single writer,
                             dispatcher (bounded concurrency), error codes, notification queue
  internal/engine/           Engine: builds the bridges, hello and networks, login sessions,
                             accounts, chats/messages/search queries, send path, shutdown, stats
  internal/localmx/          MatrixConnector and MatrixAPI for one network:
    connector.go             the interface methods (section 9)
    intent.go                MatrixAPI (owner, bot, ghost intents)
    ids.go                   ID synthesis (section 7)
    state.go                 CreateRoom, SendState, membership, chat columns derived from state
    timeline.go              messages, edits, reactions, redactions, notices, BatchSend
    status.go                bridge state to account.state; message status to message.updated
  internal/inbox/            inbox.db schema, migrations, Tx API, FTS5, cursors, install key
  internal/media/            content-addressed file store (sha256), temp staging
  internal/netcfg/           per-network config: embedded example YAML + Cappy overrides
                             (yaml.Node merge, then one decode)
  internal/logredact/        redacting io.Writer for zerolog, plus Go std log (section 17)
  internal/pincheck/         version-pin regression tests (exists today)
  test/                      protocol tests (spawn binary), fake network, opt-in WA QR smoke test
  LICENSE (AGPL-3.0)  NOTICE.md  third_party/licenses/  README.md  DESIGN.md  env.sh
```

Code adapted from bottled-bridge-core (AGPL-3.0) keeps its copyright line in
the file header.

## 6. Composition and startup inside the process

```go
db := dbutil.NewFromConfig("cappy-engine", dbutil.Config{PoolConfig: dbutil.PoolConfig{
    Type: "sqlite3-fk-wal",                        // registered by go.mau.fi/util/dbutil/litestream
    URI:  "file:" + dataDir + "/bridge.db?_txlock=immediate",
    MaxOpenConns: 5, MaxIdleConns: 2}}, zeroLogger)
inbox := inbox.Open(dataDir + "/inbox.db")         // own pool; FTS5; holds install key
for _, net := range []string{"whatsapp", "telegram"} {
    conn := newConnector(net)                      // &connector.WhatsAppConnector{} / &connector.TelegramConnector{}
    netcfg.Apply(net, conn, hello)                 // example YAML + overrides, one yaml decode
    mx := localmx.New(net, inbox, media, notifier, installKey, log)
    br := bridgev2.NewBridge(networkid.BridgeID(net), db, log, bridgeConfig(), mx, conn, commands.NewProcessor)
    br.ExternallyManagedDB = true                  // engine upgrades once and closes once
    conn.(bridgev2.MaxFileSizeingNetwork).SetMaxFileSize(50 << 20)
}
bridges["whatsapp"].DB.Upgrade(ctx)                // core schema once (shared tables, bridge_id-scoped)
// For each network: if ConfigValidatingNetwork.ValidateConfig() fails,
// network.state = needs_configuration. If it is unconfigured and has stored
// logins, skip Start entirely; otherwise call br.Start(ctx).
// Connector Start upgrades whatsmeow_/whatsapp_/telegram_ tables.
// If one bridge's Start fails, only that network is marked failed.
// Then reconcile inbox.accounts with each bridge's user_login rows.
```

**`bridgeconfig.BridgeConfig` (both bridges)**

| Field | Value | Why |
| --- | --- | --- |
| `CommandPrefix` | `"\x00cappy-no-commands"` | User text can never parse as a bridge command |
| `SplitPortals` | **true** | Every chat belongs to exactly one account, which multi-account needs. It is set before the first login and is irreversible. |
| `PrivateChatPortalMeta` | true | DMs get name and avatar state, so chats always have a title |
| `PersonalFilteringSpaces` | false | No spaces needed |
| `BridgeStatusNotices` | `"none"` | Never creates a management room |
| `PortalEventBuffer` | 0 | Each remote event is fully stored before the handler returns (synchronous) |
| `DeduplicateMatrixMessages` | true | A send retried after a restart is not sent twice |
| `CleanupOnLogout.Enabled` | false | Logout keeps local history |
| `SendReconnectWait` | 30 s | A send during a short disconnect waits |
| `UnknownErrorAutoReconnect` / `Max...` | 2 m / 10 | Automatic recovery (roadmap step 2 spirit) |
| `TagOnlyOnCreate`, `MuteOnlyOnCreate` | false | Pin, archive and mute changes keep syncing |
| `BridgeNotices`, `BridgeMatrixLeave`, `KickMatrixUsers`, `CrossRoomReplies`, `OutgoingMessageReID`, `EnableSendStateRequests`, `PhoneNumbersInProfile` | false | |
| `Permissions` | `{"@owner:cappy.local": admin}` | The only user |
| `Backfill` | Enabled, `MaxInitialMessages` 50, `MaxCatchupMessages` 500, threads 0, **queue off** | Bounded first sync. Paging deeper history is out of scope. |

**Connector config overrides** (merged into the embedded example YAML)

| Network | Key | Value |
| --- | --- | --- |
| WhatsApp | `os_name` | `Cappy Comm`. This is what the phone shows under Linked devices. |
| | `displayname_template` | `{{or .FullName .BusinessName .PushName .Phone "Unknown user"}}`, without the "(WA)" suffix |
| | `enable_status_broadcast` | false |
| | `archive_tag` | `m.lowpriority`, so archived chats map to archived |
| | `animated_sticker.target` | `disable`, because there is no converter binary |
| | `url_previews`, `lazy_avatars` | false |
| | `history_sync.media_requests.auto_request_media` | false (no requests for old media from the phone) |
| | `history_sync.max_initial_conversations` | -1 (all chats; memory measured, section 16) |
| Telegram | `api_id`, `api_hash` | From `hello` only. Kept in memory and never written to disk or logs. |
| | `device_info` | `device_model: Cappy Comm`, `system_version: macOS`, `app_version: <engine version>`. This replaces upstream `main.go`'s `PostInit`. |
| | `animated_sticker.target` | `disable` |
| | `saved_message_avatar` | `""` (upstream points at a maunium.net mxc) |

## 7. Matrix-style ID synthesis

- Server name: `cappy.local`. It is never resolved, and `ServerName()`
  returns it.
- Owner: `@owner:cappy.local`, the one bridgev2 user and double puppet.
- Bot: `@<network>bot:cappy.local`. Ghost:
  `@<network>_<id.EncodeUserLocalpart(remoteUserID)>:cappy.local`.
  `ParseGhostMXID` checks its own network prefix and decodes, so it is
  stateless and reversible. Ghost MXIDs can contain phone numbers, so logs
  are redacted (section 17).
- **Install key.** There are 32 random bytes in `inbox.db` `engine_meta`,
  created on first start. Room and message IDs are
  `HMAC-SHA256(installKey, ...)`, base32-encoded. They are deterministic for
  this install but cannot be brute-forced from phone numbers. bottled uses a
  plain SHA-256, which can be.
- Room: `!<h26>:cappy.local`, where `h26 = b32(HMAC(k, "room", network, portalID, receiver))[:26]`.
- Message event: `$m<h43>`, over (network, portal ID, receiver, message ID,
  part ID).
- Reaction: `$r<h43>`, over (network, portal key, message ID, part ID,
  sender, emoji ID).
- State changes, edits, redactions and notices get `$x<random>`. Outgoing
  messages from the host get `$c<ulid>`.
- Media: `mxc://cappy.local/<sha256-hex>`, which maps to `media/<aa>/<bb>/<sha256>`.
- The host sees `cht_<h26>` (room) and `msg_<event-id-without-$>` (message).
  These are pure string maps with no lookup. Accounts are `acc_<16 random
  b32>` and contacts are `con_<rowid b32>`, both stored in tables.

## 8. inbox.db schema (spike)

```sql
CREATE TABLE engine_meta (key TEXT PRIMARY KEY, value BLOB NOT NULL);            -- install_key, schema_version
CREATE TABLE accounts (
  account_id TEXT PRIMARY KEY, network TEXT NOT NULL, login_id TEXT NOT NULL,     -- login_id never logged
  name TEXT, state TEXT NOT NULL, state_error TEXT, state_message TEXT, user_action TEXT,
  state_ts INTEGER, created_at INTEGER NOT NULL, logged_out_at INTEGER,
  UNIQUE (network, login_id));
CREATE TABLE contacts (                                                           -- ghosts
  contact_id INTEGER PRIMARY KEY, network TEXT NOT NULL, remote_id TEXT NOT NULL, mxid TEXT NOT NULL UNIQUE,
  name TEXT, avatar_sha TEXT, identifiers TEXT /*json*/, is_bot INTEGER NOT NULL DEFAULT 0,
  UNIQUE (network, remote_id));
CREATE TABLE chats (                                                              -- rooms/portals
  room_id TEXT PRIMARY KEY, account_id TEXT REFERENCES accounts, network TEXT NOT NULL,
  portal_id TEXT NOT NULL, portal_receiver TEXT NOT NULL,
  kind TEXT NOT NULL,                     -- dm | group_dm | group | space | system
  title TEXT, topic TEXT, avatar_sha TEXT, dm_contact_id INTEGER REFERENCES contacts,
  parent_room_id TEXT, message_request INTEGER NOT NULL DEFAULT 0,
  last_activity INTEGER, last_event_id TEXT, last_read_ts INTEGER NOT NULL DEFAULT 0,
  unread_count INTEGER NOT NULL DEFAULT 0, marked_unread INTEGER NOT NULL DEFAULT 0,
  muted_until INTEGER, pinned INTEGER NOT NULL DEFAULT 0, archived INTEGER NOT NULL DEFAULT 0,
  disappear_type TEXT, disappear_ms INTEGER, deleted INTEGER NOT NULL DEFAULT 0,
  UNIQUE (network, portal_id, portal_receiver));
CREATE INDEX chats_list ON chats (account_id, deleted, archived, last_activity DESC, room_id DESC);
CREATE TABLE members (room_id TEXT NOT NULL, contact_id INTEGER, is_owner INTEGER NOT NULL DEFAULT 0,
  membership TEXT NOT NULL, PRIMARY KEY (room_id, contact_id, is_owner));
CREATE TABLE room_state (room_id TEXT NOT NULL, type TEXT NOT NULL, state_key TEXT NOT NULL,
  sender TEXT NOT NULL, content TEXT NOT NULL, PRIMARY KEY (room_id, type, state_key));
CREATE TABLE messages (
  rowid INTEGER PRIMARY KEY, event_id TEXT NOT NULL UNIQUE, room_id TEXT NOT NULL REFERENCES chats,
  sender_contact_id INTEGER REFERENCES contacts, from_me INTEGER NOT NULL, ts INTEGER NOT NULL,
  kind TEXT NOT NULL,                     -- message | notice | update
  msgtype TEXT, body TEXT, formatted_body TEXT, update_json TEXT,
  reply_to TEXT, thread_root TEXT, remote_id TEXT, part_id TEXT,
  edited_ts INTEGER, redacted_ts INTEGER, redact_mode TEXT,     -- unsend | disappear | viewed
  attachment TEXT /*json*/, link_previews TEXT /*json*/,
  status TEXT, status_error TEXT, client_txn TEXT UNIQUE,        -- outgoing only
  source TEXT NOT NULL);                  -- live | backfill | local
CREATE INDEX messages_room_ts ON messages (room_id, ts, rowid);
CREATE VIRTUAL TABLE messages_fts USING fts5(body, content='messages', content_rowid='rowid',
  tokenize='unicode61 remove_diacritics 2');
-- triggers: AFTER INSERT / AFTER UPDATE OF body / AFTER DELETE keep messages_fts in sync;
-- redaction sets body = NULL, which removes it from the index.
CREATE TABLE reactions (event_id TEXT PRIMARY KEY, target_event_id TEXT NOT NULL, room_id TEXT NOT NULL,
  sender_contact_id INTEGER, from_me INTEGER NOT NULL, key TEXT NOT NULL, ts INTEGER NOT NULL);
CREATE TABLE receipts (room_id TEXT NOT NULL, contact_id INTEGER NOT NULL, event_id TEXT NOT NULL,
  ts INTEGER NOT NULL, PRIMARY KEY (room_id, contact_id));
CREATE TABLE raw_events (event_id TEXT PRIMARY KEY, room_id TEXT NOT NULL, type TEXT NOT NULL,
  sender TEXT NOT NULL, ts INTEGER NOT NULL, content TEXT NOT NULL);  -- backs MatrixAPI.GetEvent
```

Pragmas: WAL, `synchronous=NORMAL`, `busy_timeout=5000`,
`foreign_keys=ON`, `cache_size=-4096` (4 MiB), and `_txlock=immediate`.

**Two files, not one.** The bridge write lock and the inbox write lock never
block each other. Crash consistency comes from idempotent deterministic IDs
(fact 7): a redelivered message upserts into the same row. This matches
bottled-bridge-core.

Unread count is `count(messages)` where the message is not from the owner,
has `kind = 'message'` and `ts > last_read_ts`. It is updated in the same
transaction as the write.

## 9. MatrixConnector: every required method

| Method | Implementation |
| --- | --- |
| `Init(br)` | Stores `br`. The bot, owner and ghost intent factories are ready **before** `NewBridge` reads `BotIntent()` (fact 1). |
| `Start`, `PreStop`, `Stop` | No-ops. The engine owns and closes inbox.db after every bridge has stopped. |
| `GetCapabilities` | `{BatchSending: true, ExtraProfileMeta: true}`. `AutoJoinInvites` is false, so bridgev2 syncs members explicitly; this is the proven bottled path. `ArbitraryMemberChange` and `ReplaceEntireProfile` are false. |
| `ParseGhostMXID`, `FormatGhostMXID` | As described in section 7. |
| `GhostIntent(userID)` | Returns `intent{mxid: ghost, ghost: userID}`. |
| `NewUserIntent(ctx, userID, token)` | For `@owner:cappy.local`, returns the owner intent and echoes the token. bridgev2's `User.DoublePuppet` always calls this, so "from me" events made on the phone go through the owner intent. Any other user gets an error. |
| `BotIntent` | Returns the bot intent (non-nil). |
| `SendBridgeStatus(state)` | **Always returns nil** (fact 4). If `RemoteID` is set, it upserts `accounts.state/error/message/user_action` and emits `account.state`. A global state updates the network state shown in `networks.list`. |
| `SendMessageStatus(st, info)` | Looks up the message by `info.SourceEventID`. Success becomes `sent`, or `delivered` when `DeliveredTo` is set. Pending is ignored. Anything else becomes `failed` with a short code from `ErrorReason`; the human text is passed through to the host and not logged. Emits `message.updated{change:"status"}`. Statuses for non-message events are ignored. |
| `GenerateContentURI`, `ParseContentURI` | Return `bridgev2.ErrDirectMediaNotEnabled`. Direct media is off, so they are unreachable (fact 8). Lazy media is a later step. |
| `GetPowerLevels`, `GetMembers`, `GetMemberInfo` | Read from `room_state` (`m.room.power_levels` and `m.room.member`). Missing state returns empty content, not an error. WhatsApp's announcement check calls `GetMemberInfo`. |
| `BatchSend(room, req, extras)` | Applies all events in one inbox transaction with `source = backfill`, using the IDs bridgev2 assigned. Messages and reactions are upserted, so they are idempotent. If `MarkReadBy == owner`, sets `last_read_ts` to the batch maximum. Emits `chat.updated{reason:"backfill"}` and **never** `message.new`. Returns the event IDs in request order. |
| `GenerateDeterministicRoomID`, `...EventID`, `GenerateReactionEventID` | HMAC IDs from section 7. A reaction with a nil target gets `$x<random>`. |
| `ServerName` | `"cappy.local"`. |

**Optional interfaces**

| Interface | Decision |
| --- | --- |
| `MatrixConnectorWithArbitraryRoomState.GetStateEvent` | **Implemented** from `room_state`. Used for power levels, the create event, join rules (`roomIsPublic`) and functional members. |
| `MatrixConnectorWithBridgeIdentifier` | Implemented: `"cappy-" + network`, giving a stable bridge-info state key. |
| `...WithURLPreviews` | Not implemented. The engine never fetches arbitrary URLs, and WhatsApp skips previews. |
| `...WithNotifications` | Not implemented. Host notifications are driven by `message.new`. |
| `...WithServer`, `...WithProvisioning`, `...WithPublicMedia`, `...WithAnalytics`, `...WithBeeperStreams`, `...WithPostRoomBridgeHandling`, `...WithNameDisambiguation`, `...WithHTTPSettings` | Not implemented. There is no HTTP server, no Beeper, and nothing to disambiguate in a local UI. Upstream defaults apply. |
| `StreamOrderReadingMatrixAPI`, `MarkAsDMMatrixAPI`, `MatrixAPIWithArbitraryRoomState` | Not implemented. The receipt fallback ignores receipts that have no target message. |

## 10. MatrixAPI (owner, bot and ghost intents): every method

| Method | Implementation |
| --- | --- |
| `GetMXID`, `IsDoublePuppet` | Returns the intent's MXID. `IsDoublePuppet` is true only for the owner. |
| `SendMessage(room, type, content, extra)` | Parses the content and stores raw JSON in `raw_events`. The event ID is `$m`/`$r` when `extra.MessageMeta`/`ReactionMeta` is set (matching the backfill IDs), otherwise `$x`. The timestamp is `extra.Timestamp`, falling back to now. **Routing:** `m.room.message`/`m.sticker` inserts a message. With `m.replace`, it updates `body`, `formatted_body`, `edited_ts` and FTS. `m.reaction` upserts into `reactions`. `m.room.redaction` sets `redacted_ts`, `body=NULL` and the redact mode ("Message disappeared" from the bot means disappear or viewed). Events from the bot become `kind:'notice'`. Other types are stored raw only. Emits `message.new` (live, not from the owner) or `message.updated`, plus `chat.updated{activity}`. |
| `SendState(room, type, key, content, ts)` | Upserts `room_state`, then derives chat columns: name to `title`, topic, avatar, disappearing timer, space parent/child to `parent_room_id`, and member to `members` (DM other party to `dm_contact_id`). A visible change with `ts != 0` (rename, join or leave in a group) inserts a `kind:'update'` row. Its `update_json` (for example `{"type":"rename","title":...}`) is localized by the host. Returns `$x`. |
| `MarkRead(room, event, ts)` | Owner, meaning read on the phone: sets `last_read_ts` to the target timestamp and recomputes unread, emitting `chat.updated{read}`. Ghost: upserts `receipts`, and the owner's outgoing messages at or before it become `read` (`message.updated{receipts}`). An unknown target is a no-op. |
| `MarkUnread(room, unread)` | Sets `chats.marked_unread`. |
| `MarkTyping` | No-op returning nil. Typing indicators are out of scope. |
| `DownloadMedia`, `DownloadMediaToFile` | Read `media/<sha>` from a `mxc://cappy.local/<sha>` URI. When writable is requested, the file is copied to `tmp/`. Any other URI returns `mautrix.MNotFound`. |
| `UploadMedia`, `UploadMediaStream` | Write to `tmp/`, hash with sha256, then rename into `media/`. Returns `mxc://cappy.local/<sha>` with no encryption info. The 50 MiB connector cap bounds memory. `ReplacementFile` is honored and removed. |
| `SetDisplayName`, `SetAvatarURL` | Ghost: updates `contacts.name`/`avatar_sha`. Owner and bot: no-op. |
| `SetExtraProfileMeta` | Ghost: stores `identifiers` and `is_bot`. `tel:` identifiers are already stripped by `PhoneNumbersInProfile=false`. |
| `SetProfile` | No-op. Only used with `ReplaceEntireProfile`, which is off. |
| `CreateRoom(req)` | The room ID is `req.BeeperLocalRoomID`. The portal ID, receiver and room type come from the `m.bridge` initial state (fact 5). The receiver resolves to `account_id`. Applies `PowerLevelOverride` and every `InitialState` event. Kind comes from `IsDirect`, `BeeperRoomTypeV2` or the space creation type. **With no `BeeperLocalRoomID`** (a management room), it creates a hidden `kind:'system'` chat with a random ID instead of failing. Emits `chat.updated{created}`. |
| `DeleteRoom(room, puppetsOnly)` | `puppetsOnly` is a no-op. Otherwise sets `deleted=1` (hidden, history kept), because bridgev2 also deletes rooms it replaces. Emits `chat.updated{deleted}`. |
| `EnsureJoined`, `EnsureInvited` | Membership upsert through the `SendState` path. The bot is skipped. |
| `TagRoom(tag, on)` | `m.favourite` maps to `pinned` and `m.lowpriority` to `archived`. Others are ignored. |
| `MuteRoom(until)` | `muted_until`: -1 means forever and 0 means unmuted. |
| `GetEvent(room, event)` | **Implemented** from `raw_events`. bottled returns `MNotFound` here. A redacted event gets `Unsigned.RedactedBecause` set. Without this, WhatsApp quoted replies and caption edits lose content (fact 1). |

## 11. Event flows

- **Live remote message.** The connector calls `UserLogin.QueueRemoteEvent`,
  which reaches `portal.queueEvent`. Because the buffer is 0, the work is
  synchronous. `handleRemoteMessage` and `GetIntentFor` pick the ghost
  intent, or the owner when `IsFromMe`. Then `intent.SendMessage(extra.MessageMeta)`
  writes an inbox upsert, emits notifications after the commit, and returns
  `$m...`. Finally bridgev2 inserts its `message` row in bridge.db.
- **First sync.** WhatsApp stores history-sync blobs in `whatsapp_history_sync_*`.
  After `dispatch_wait` (1 min) it creates portals through `CreateRoom` and
  member state, then backfills 50 messages per chat with `BatchSend`.
  Expect chats to appear about a minute after login.
- **Edits, reactions, redactions, receipts and chat info** use the same intent
  methods (sections 9 and 10). Chat delete calls `DeleteRoom`.
- **Outgoing (`messages.send`).** The engine first validates the chat, account
  and text. It inserts a local row with `$c<ulid>`, `status:'pending'`,
  `source:'local'` and `from_me=1`, and responds right away. A goroutine then
  builds `event.Event{Type: EventMessage, Sender: @owner, RoomID, ID: $c..., Timestamp, Content.Parsed: &MessageEventContent{MsgType: m.text, Body, RelatesTo(reply)}}`
  and calls `br.QueueMatrixEvent(ctx, evt)` on the chat's network bridge.
  bridgev2 chooses the receiver login (split portals) and the connector sends.
  bridgev2 then calls `SendMessageStatus`, and the engine emits
  `message.updated{status}`. bridgev2's `message` row uses MXID `$c...`, so
  later edits and reactions from the network match.

## 12. Login orchestration

- One active login per network. A second `login.start` returns
  `login_in_progress` unless `replace:true` is given.
- `login.start` calls `CreateLogin(user=@owner, flow)` and `Start`. If the
  first step is `display_and_wait`, the engine starts a **waiter goroutine**
  with a session-owned context, not the request context, because WhatsApp
  cancels the login when the context ends (fact 10). The waiter loops on
  `Wait()`, and each new step (for example a rotated QR) becomes a
  `login.step` notification.
- `login.submit` validates `stepID` and the step type. `user_input` calls
  `SubmitUserInput`, and `cookies` calls `SubmitCookies` (kept for future
  networks). The returned step is pushed the same way.
- `webauthn` (the WhatsApp passkey step) and `client_http` produce
  `login.step{status:"failed", error.code:"unsupported_step"}` in the spike.
- On `complete`, the engine upserts the `accounts` row from
  `CompleteParams.UserLogin` and emits `login.step{completed}` and
  `account.state`.
- `login.cancel` and engine shutdown call `proc.Cancel()` and cancel the
  waiter context.
- Re-login of an existing account uses `relogin: acc_...`, which calls
  `LoginProcessWithOverride.StartWithOverride`. This is wired but not tested
  in the spike.
- Telegram without `api_id` returns `network_not_configured` **before**
  `CreateLogin`, so nothing is contacted.
- QR data is passed to the host verbatim and is never logged. Tests assert
  its shape only (section 19).

## 13. stdio JSON-RPC protocol (version 1)

**Framing.** JSON-RPC 2.0 messages as UTF-8 JSON, **one object per `\n`-terminated
line** (NDJSON), in both directions. The maximum line is 8 MiB; a longer line
gets error -32600 and is discarded, and the engine keeps running. Logs go to
stderr only. At startup the engine dups fd 1 for RPC and points `os.Stdout`
at stderr, so a stray print in any library cannot corrupt the stream.
Timestamps are integer milliseconds since the epoch. Cappy fields use
camelCase. A `step` object is bridgev2's `LoginStep` JSON passed through
verbatim (snake_case, schema in `bridgev2/unorganized-docs/login-step.schema.json`).

**Ordering.** Requests may run concurrently (16 at a time), and responses
are matched by `id`. Notifications are emitted only after the inbox commit,
in commit order per chat. `message.new` comes before the `chat.updated` for
the same write. The notification queue holds 1024 entries. If it overflows,
pending `chat.updated` and `message.*` entries are dropped and one
`engine.resync` is sent, after which the host reloads its lists. Responses
are never dropped.

**Errors.** `{"code": n, "message": "...", "data": {"reason": "snake_case"}}`.
Standard codes are -32700, -32600, -32601, -32602 and -32603. Engine codes:

| Code | reason | Code | reason |
| --- | --- | --- | --- |
| 1000 | `not_ready` (before hello or while stopping) | 1007 | `login_failed` (`data.upstream` = upstream ErrCode) |
| 1001 | `unknown_network` | 1008 | `unsupported_step` |
| 1002 | `network_not_configured` | 1009 | `account_not_found` |
| 1003 | `network_failed` | 1010 | `chat_not_found` |
| 1004 | `login_in_progress` | 1011 | `not_logged_in` |
| 1005 | `login_not_found` | 1012 | `invalid_cursor` |
| 1006 | `login_step_mismatch` | 1013 | `too_large` |

**Shared shapes**

```jsonc
Account  {"account":"acc_K3V9...","network":"whatsapp","name":"...","loggedIn":true,
          "state":"CONNECTED","error":null,"message":null,"userAction":null,"stateTs":1763050000000,"createdAt":1763040000000}
Contact  {"contact":"con_4F","network":"whatsapp","name":"...","avatar":Media|null,"isBot":false}
Media    {"media":"sha256:9f86...","path":"media/9f/86/9f86...","mime":"image/jpeg","size":48213}   // path is relative to dataDir
Chat     {"chat":"cht_ZQ4M...","account":"acc_K3V9...","network":"whatsapp","kind":"dm",
          "title":"...","avatar":Media|null,"dmContact":Contact|null,"messageRequest":false,
          "lastActivity":1763050000000,"lastMessage":{"message":"msg_m7Q...","fromMe":false,"sender":"con_4F",
          "ts":1763050000000,"kind":"message","msgtype":"m.text","preview":"... first 140 chars ..."},
          "unread":3,"markedUnread":false,"mutedUntil":null,"pinned":false,"archived":false,"deleted":false}
Message  {"message":"msg_m7Q...","chat":"cht_ZQ4M...","account":"acc_K3V9...","sender":Contact|null,"fromMe":false,
          "ts":1763050000000,"editedTs":null,"deleted":false,"kind":"message","msgtype":"m.text",
          "text":"...","html":null,"update":null,"replyTo":"msg_...|null","threadRoot":null,
          "attachment":{"msgtype":"m.image","name":"IMG_1.jpg","caption":null,"width":1280,"height":960,
                        "durationMs":null,"isVoice":false,"isSticker":false,"blurhash":"...","file":Media|null},
          "reactions":[{"key":"👍","count":2,"fromMe":true}],"status":null,"statusError":null}
          // status for fromMe messages: pending | sent | delivered | read | failed
```

**Requests**

| Method | params | result |
| --- | --- | --- |
| `hello` (must come first) | `{"protocol":1,"client":{"name":"cappy-comm","version":"x.y.z"},"telegram":{"apiId":0,"apiHash":""}?}` | `{"protocol":1,"engine":{"version","commit","go"},"upstream":{"mautrix-go":"v0.32.0","mautrix-whatsapp":"v0.2610.0","mautrix-telegram":"v0.2610.0"},"pid":4242,"dataDir":"...","memoryLimitBytes":268435456}`. Bridges are built and started before replying; host timeout 60 s. |
| `networks.list` | `{}` | `{"networks":[{"network":"whatsapp","name":"WhatsApp","state":"ready","reason":null,"accounts":1,"flows":[{"id":"qr","name":"QR","description":"..."},{"id":"phone","name":"Pairing code","description":"..."}]},{"network":"telegram","name":"Telegram","state":"needs_configuration","reason":"api_credentials_missing","accounts":0,"flows":[{"id":"phone"},{"id":"qr"},{"id":"bot"},{"id":"manual"}]}]}`. The state is one of `starting`, `ready`, `needs_configuration` or `failed`. |
| `login.start` | `{"network":"whatsapp","flow":"qr","replace":false,"relogin":null}` | `{"loginID":"lgn_8H2...","step":LoginStep}` |
| `login.submit` | `{"loginID":"lgn_...","stepID":"fi.mau.telegram.login.code","input":{"fi.mau.telegram.login.code":"12345"}}` | `{"loginID":"lgn_...","step":LoginStep}` or `{"loginID":"lgn_...","completed":Account}` |
| `login.cancel` | `{"loginID":"lgn_..."}` | `{"cancelled":true}` |
| `accounts.list` | `{}` | `{"accounts":[Account]}` |
| `accounts.logout` | `{"account":"acc_...","remote":true,"purgeLocal":false}` | `{"ok":true}`. `remote:true` calls `UserLogin.Logout`, which unlinks the device. `false` calls `Delete` with `LogoutRemote:false`. `purgeLocal` removes the account's chats from inbox.db. |
| `chats.list` | `{"account":null,"cursor":null,"limit":50,"archived":false}` | `{"chats":[Chat],"nextCursor":"b64(...)"|null}`. Ordered by `lastActivity` descending, then chat descending. `account:null` means all accounts. Maximum limit 200. |
| `chats.markRead` | `{"chat":"cht_...","through":"msg_..."|null}` | `{"unread":0}`. Marks the chat read up to `through` (or its newest message) and clears "marked unread". When the newest read message is from someone else and the network is usable, a read receipt is queued to the network in the background (WhatsApp sends read receipts as the official app does). |
| `media.preview` | `{"chat":"cht_...","message":"msg_..."}` | `{"mime":"image/jpeg","width":1600,"height":1200,"data":"base64"}`. For a photo or sticker stored on this computer: decoded (JPEG, PNG, GIF first frame, still WebP), refused above 40 megapixels, scaled to fit 1600 px, transparency flattened to white and re-encoded as JPEG, so metadata such as location is dropped. Otherwise error 1014 `media_unavailable`. |
| `messages.list` | `{"chat":"cht_...","before":"msg_..."|null,"limit":50}` | `{"messages":[Message],"hasMore":true}`, oldest first. Local only in the spike; no network paging. |
| `messages.send` | `{"chat":"cht_...","text":"...","replyTo":"msg_..."|null,"clientTxn":"uuid"}` | `{"message":Message}` with `status:"pending"`. Text is 1–65536 characters. A repeated `clientTxn` returns the existing message. |
| `search` | `{"query":"...","account":null,"chat":null,"limit":50,"cursor":null}` | `{"results":[{"message":Message,"snippet":"...[match]..."}],"nextCursor":null}`. The query is tokenized and each term quoted, so FTS operators cannot be injected. The last term gets a prefix `*`. Ordered newest first; maximum limit 100. |
| *extra* `ping` | `{}` | `{"ts":1763050000000}` |
| *extra* `engine.stats` | `{}` | `{"rssBytes","goHeapBytes","goSysBytes","goroutines","gcCycles","memoryLimitBytes","uptimeMs","dbBytes":{"bridge","inbox"},"mediaBytes"}` |
| *extra* `shutdown` | `{}` | `{"ok":true}`, then a graceful stop (section 14) |

**Notifications** (no `id`)

| Method | params |
| --- | --- |
| `login.step` | `{"loginID","network","status":"step"|"completed"|"failed"|"cancelled","step":LoginStep?,"account":Account?,"error":{"code","message","upstream"}?}` |
| `account.state` | `{"account":Account}`, on every bridge state change |
| `chat.updated` | `{"chat":Chat,"reason":"created"|"info"|"activity"|"read"|"backfill"|"deleted"}`. Debounced to 250 ms per chat. |
| `message.new` | `{"message":Message,"chat":{"chat","unread","lastActivity"}}`. Live messages only, never backfill. |
| *extra* `message.updated` | `{"message":Message,"change":"edit"|"delete"|"reactions"|"status"|"receipts"}` |
| *extra* `engine.resync` | `{"reason":"notification_overflow"}` |

Example (WhatsApp QR):

```
-> {"jsonrpc":"2.0","id":7,"method":"login.start","params":{"network":"whatsapp","flow":"qr"}}
<- {"jsonrpc":"2.0","id":7,"result":{"loginID":"lgn_8H2Q","step":{"type":"display_and_wait","step_id":"fi.mau.whatsapp.login.qr","instructions":"Scan the QR code with the WhatsApp mobile app to log in","display_and_wait":{"type":"qr","data":"<opaque pairing payload>"}}}}
<- {"jsonrpc":"2.0","method":"login.step","params":{"loginID":"lgn_8H2Q","network":"whatsapp","status":"step","step":{...rotated QR...}}}
<- {"jsonrpc":"2.0","method":"login.step","params":{"loginID":"lgn_8H2Q","network":"whatsapp","status":"completed","account":{...}}}
```

## 14. Process lifecycle

- **Spawn** (host, as a Tauri `externalBin` sidecar):
  `cappy-engine --data-dir <abs path> [--log-level info] [--memory-limit 256MiB]`.
  The host clears the environment, then sets `HOME`, `TMPDIR` and
  `GOMEMLIMIT` (when the flag is absent). Secrets never go on argv or into
  env. Telegram credentials travel in `hello`.
- **Engine start:**
  1. Parse flags. Exit 2 on bad usage.
  2. Capture stdout for RPC; `os.Stdout` becomes stderr.
  3. Set up the redacting logger.
  4. Create the data dir with mode 0700.
  5. Take `flock(engine.lock, LOCK_EX|LOCK_NB)`. Exit 3 if busy.
  6. Open and migrate inbox.db, and open bridge.db. Exit 4 if the DB owner
     differs or is a newer version (no downgrade).
  7. Wipe `tmp/`. Mark `pending` outgoing rows from earlier processes:
     `sent` if bridge.db has a `message` with that MXID, otherwise
     `failed:"interrupted"`.
  8. Serve RPC. Only `hello`, `ping`, `engine.stats` and `shutdown` work
     until `hello` finishes.
  9. On `hello`, build and start the bridges (section 6), then reply.
     Notifications flow from then on.
- **Graceful stop** happens on `shutdown`, stdin EOF (which is how parent
  death is detected) or SIGTERM/SIGINT:
  1. Refuse new requests with 1000.
  2. Cancel login sessions.
  3. Run `StopWithTimeout(5 s)` on every bridge in parallel. This
     disconnects clients and **does not log out**.
  4. Drain notifications.
  5. Run `PRAGMA wal_checkpoint(TRUNCATE)`, close both DBs and release the
     lock.
  6. Exit 0. A hard deadline of 10 s ends with exit 1.
- **Host quit sequence:** send `shutdown`, wait 10 s, send SIGTERM, wait
  3 s, then SIGKILL.
- **Crash restart by the host:**
  - On a non-zero exit, a signal or unexpected EOF, restart with backoff
    1, 2, 4 … 60 s. The backoff resets after 10 min of uptime.
  - **Circuit breaker:** after 5 crashes in 15 min, stop restarting, mark the
    engine failed, and fall back to the web views.
  - Exit 2 and 4 are never retried. Exit 3 is retried once after 2 s,
    because the old process may still be exiting.
  - Hang detection: a `ping` every 30 s with a 10 s timeout; 3 misses means
    kill and restart.
  - After a restart, the host re-sends `hello` and reloads `accounts.list`
    and the visible chats. In-flight requests fail on the host side with
    `engine_restarted`. Logins in progress are lost and the UI offers
    "try again".
  - Stored sessions reconnect automatically through `StartLogins`.

## 15. Data folder layout

Default: `~/Library/Application Support/app.cappycomm.rust/engine.noindex/`.
`.noindex` keeps message attachments out of Spotlight. Tests use
`Cappy_Engine/tmp.noindex/<run>/`.

```
engine.noindex/            0700
  engine.lock              flock; holds the pid
  bridge.db(-wal,-shm)     0600  mautrix-owned: bridgev2 core (bridge_id whatsapp|telegram), whatsmeow_* (device keys = credentials), whatsapp_*, telegram_*
  inbox.db(-wal,-shm)      0600  Cappy-owned: section 8 schema + FTS5 + install key
  media/aa/bb/<sha256>     0600  attachments and avatars, deduplicated
  tmp/                           staging, wiped at start
```

`bridge.db` holds live session keys. Restoring it on another Mac would clone
a WhatsApp device. Whether to exclude it from Time Machine is an owner
decision, and is not done in the spike. Encryption at rest relies on
FileVault for now.

## 16. Memory cap

- `GOMEMLIMIT` is a soft limit: the GC works harder near it, but it is not a
  hard cap. The default is 256 MiB, from the env var or `--memory-limit`
  (`debug.SetMemoryLimit`). GOGC stays at the default.
- Fixed bounds:
  - SQLite cache 4 MiB per inbox connection (at most 4) and a bridge pool of
    5 connections.
  - Notification queue 1024, RPC concurrency 16.
  - List limits 200 (messages and chats) and 100 (search).
  - Media cap 50 MiB, because WhatsApp downloads attachments into memory.
- Known peaks: WhatsApp history-sync blobs (protobuf decode during the first
  sync) and media downloads.
- **Measurement.** `engine.stats` (runtime/metrics plus RSS from
  `getrusage`) is sampled by the host at idle, after a WhatsApp login, at
  the first-sync peak, and daily during the 2-week soak.
- **Proposed targets, unverified:**
  - Idle with no accounts: 80 MB or less.
  - Steady with one WhatsApp account: 150 MB or less.
  - First-sync peak: 400 MB or less.
- The host restarts the engine gracefully if RSS stays above 1 GiB for
  2 minutes.

## 17. Logging and privacy

- All logs are zerolog JSON on stderr. They pass through
  `logredact.Writer`, as does Go's std `log`. whatsmeow and gotd/zap log at
  warn; bridgev2 and the engine at info. The host stores stderr in its own
  local rotating log (3 × 5 MB) and never uploads it.
- **The writer parses each line.** Deny-listed keys get
  `"redacted:<8 hex of HMAC(installKey, value)>"`, so values can still be
  correlated. The deny-list:
  `body`, `text`, `caption`, `formatted_body`, `name`, `displayname`,
  `remote_name`, `push_name`, `business_name`, `phone`, `phone_number`,
  `user_login_id`, `login_id`, `remote_id`, `jid`, `chat_jid`, `sender`,
  `sender_id`, `sender_jid`, `ghost_id`, `user_id`, `portal_id`,
  `portal_receiver`, `event_data`, `qr`, `code`, `url`, `topic`, `title`,
  `query`, `input`.
  Every string value, including `message`, is then scrubbed with regexes for
  WhatsApp JIDs (`@s.whatsapp.net|@lid|@g.us|@newsletter|@broadcast`),
  phone-like runs of 7 or more digits, and ghost MXIDs
  (`@(whatsapp|telegram)_...`).
- RPC params and results are never logged. Only method, id, duration and
  error code are.
- **Residual risk.** An unrecovered goroutine panic prints the Go runtime's
  own stack to stderr, which bypasses the writer. bridgev2 recovers panics in
  event handlers and the RPC layer recovers per request, so this path is
  rare. The host treats engine stderr as sensitive.

## 18. Security notes

- The engine never calls `net.Listen`. The binary links `appservice`,
  `federation` and `mediaproxy` code transitively, but nothing starts them.
  Tests check `lsof -nP -a -p <pid> -iTCP -sTCP:LISTEN` and `-iUDP`.
- The only outbound peers are WhatsApp, which includes the unauthenticated
  web-version check at first login, and, once configured, Telegram data
  centres.
- All params are validated and size-bounded. The engine accepts no file paths
  from the host in the spike.
- The Telegram `api_hash` lives only in memory.

## 19. Spike test plan (go/no-go)

Agent-run tests contact no account and no Telegram server:

| ID | Test | Pass |
| --- | --- | --- |
| T0 | `go build -tags sqlite_fts5 ./cmd/cappy-engine` and `go vet ./...` | One binary with both connectors; size recorded |
| T1 | Unit tests: localmx against a fake `NetworkConnector`. Covers portal create, live message, edit, reaction, redaction, receipts, a BatchSend repeated twice giving one row, FTS search, and the send path ending in `sent`. | All pass; no duplicate rows |
| T2 | Protocol test spawning the binary: `hello`, then `networks.list` (whatsapp ready, telegram `needs_configuration`), empty lists, Telegram `login.start` returning 1002, an unknown method returning -32601, a malformed line returning -32700 with the engine surviving, `shutdown` exiting 0 within 10 s, stdin EOF exiting 0, and a SIGKILL followed by a restart on the same dir. | All pass |
| T3 | No listening sockets (lsof during T2 and T4) | Empty |
| T4 | **Opt-in** `CAPPY_WA_QR_SMOKE=1`, the WhatsApp QR shape check. Uses a temp dir: `login.start whatsapp/qr` must yield a `display_and_wait` step of type `qr` with non-empty data in the expected comma-separated form. **Only length and shape are asserted; the data is never printed, rendered or saved.** Then `login.cancel` (status `cancelled`), `accounts.list` empty, `shutdown`, and delete the dir. This is the only WhatsApp contact (unauthenticated). | Pass |
| T5 | Telegram registration: `networks.list` shows Telegram with its 4 flows, and no Telegram connection exists | Pass |
| T6 | `engine.stats` at idle and after T4 | Recorded |
| T7 | Log scan with synthetic canaries (fake numbers such as `+1555...` and bodies `CAPPY-CANARY-*` from the fake network) | No canary in stderr |

Owner-run later, never by an agent: **T8**, the second WhatsApp account. This
covers QR login by the owner, the first sync, live receive within 5 s, a text
send, restart without re-login, and a 14-day soak with daily `engine.stats`
and an `account.state` log. **T9**, Telegram, once the owner supplies an
`api_id`/`api_hash`.

**GO** if T0–T7 pass, and T8 shows: the first sync completes, sends succeed,
there is no unexpected logout in 14 days, memory stays within target, and
there are no crash loops. **NO-GO** if any of these fail and no
configuration fix exists.

## 20. Monthly upgrade procedure

1. Check that the new `mautrix-whatsapp` and `mautrix-telegram` tags require
   the same `maunium.net/go/mautrix` version. If not, use the nearest
   compatible commit and record it here.
2. Run `go get` on all three, then `go mod tidy` and `go mod verify`.
3. Diff `bridgev2/matrixinterface.go`, `networkinterface.go`, `login.go`,
   `bridge.go` and `bridgeconfig/config.go` between the two mautrix-go tags.
4. Run `go test -tags sqlite_fts5 ./...`, then T1–T7.

## 21. Out of scope for the spike

- Sending media, voice, stickers, reactions, edits, deletes, read receipts
  or typing from Cappy. Only text send is in.
- Loading older history from the network (backfill queue) and lazy or direct
  media.
- Media garbage collection and pruning of `raw_events`.
- Telegram login, which needs the owner's `api_id`.
- WhatsApp passkey (`webauthn`) steps and `client_http` steps.
- New chats, contact search, communities and spaces UI.
- OS notifications from the engine.
- Signal, Messenger, Instagram, Slack and LinkedIn.
- A Windows build.
- Signing and notarizing the sidecar. This is part of roadmap step 3, after
  the code exists.
- Encryption at rest beyond FileVault, and backups or export.
- Matrix and Beeper compatibility, and bridge `!commands`.

## 22. Risks and open questions

1. **Shared bridge.db write contention.** Two bridges plus whatsmeow share
   one SQLite writer. Mitigations: `_txlock=immediate` and a 5 s busy
   timeout. Fallback: one `bridge-<network>.db` per network. This is a small
   change because the bridge DB is per-bridge in code.
2. **Process-global connector state** (whatsmeow `DeviceProps`/version).
   Mitigation: exactly one bridge per network.
3. **Matrix semantics bridgev2 assumes but local storage only approximates.**
   Stream-order receipts, cross-room replies and some state echoes degrade
   silently. `GetEvent` is implemented to cover the important ones.
4. **Two-file crash window.** A message can land in inbox.db without a
   bridge.db row. A later edit or reaction to it is then dropped by bridgev2.
   This is rare and accepted.
5. **Ban or unlink risk** of unofficial clients is an owner decision. Test
   only the second WhatsApp account first.
6. **AGPL.** Distributing the engine binary requires offering its source.
   Whether the Rust host is affected is a legal question (not legal advice).
7. **Upstream churn** happens monthly (section 20).
8. **Disk growth** from eager media downloads. The 50 MiB cap applies; lazy
   media is the next step.
9. **Signing and notarizing** a cgo Go binary as a Tauri sidecar is
   expected to work but has not been tried.
