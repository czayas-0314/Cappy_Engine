# Notices

Cappy Engine is licensed under the GNU Affero General Public License v3.0 only
(AGPL-3.0-only). See `LICENSE`, which is the AGPL-3.0 text copied from mautrix-whatsapp.
It links the following components. Their license texts are in
`third_party/licenses/`.

| Component | License | File |
| --- | --- | --- |
| mautrix-whatsapp (go.mau.fi/mautrix-whatsapp) v0.2610.0 | AGPL-3.0-or-later | `mautrix-whatsapp.LICENSE.txt` |
| mautrix-telegram (go.mau.fi/mautrix-telegram) v0.2610.0 | AGPL-3.0-or-later | `mautrix-telegram.LICENSE.txt` |
| gotd/td fork inside mautrix-telegram (`pkg/gotd`) | MIT | `gotd-td.LICENSE.txt` |
| mautrix-go (maunium.net/go/mautrix) v0.32.0 | MPL-2.0 | `mautrix-go.LICENSE.txt` |
| whatsmeow (go.mau.fi/whatsmeow) | MPL-2.0 | `whatsmeow.LICENSE.txt` |
| go.mau.fi/util v0.11.0 | MPL-2.0 | `mau-util.LICENSE.txt` |
| The Go standard library and golang.org/x modules (crypto, exp, image, net, sync, sys, text), all from The Go Authors | BSD-3-Clause | `go.LICENSE.txt` |
| mattn/go-sqlite3 v1.14.52, which bundles SQLite (public domain) | MIT | `go-sqlite3.LICENSE.txt` |
| bottled-bridge-core (cloud-in-a-bottle, commit `e801a224`). Code adapted from it in `internal/localmx/*.go` and `internal/fakenet/fakenet.go` says so in each file header. | AGPL-3.0 | `bottled-bridge-core.LICENSE.txt` |

Other transitive Go modules keep their own licenses, which are in the module
cache. A release must generate a complete list, for example with
`go-licenses`, before any binary is distributed.

The special exceptions in `mautrix-whatsapp.LICENSE.exceptions.txt` and
`mautrix-telegram.LICENSE.exceptions.txt` are granted to Beeper and Element
only. **They do not apply to Cappy Engine.** They are kept here as upstream
notices.

Anyone who receives an engine binary must be offered its complete
corresponding source under the AGPL. This is not legal advice.
