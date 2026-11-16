// Package pincheck exists only to prove that the pinned WhatsApp and Telegram
// bridgev2 connectors resolve to ONE maunium.net/go/mautrix version and link
// into one binary. It is not part of the engine.
package pincheck

import (
	"maunium.net/go/mautrix/bridgev2"

	telegram "go.mau.fi/mautrix-telegram/pkg/connector"
	whatsapp "go.mau.fi/mautrix-whatsapp/pkg/connector"
)

var (
	_ bridgev2.NetworkConnector = (*whatsapp.WhatsAppConnector)(nil)
	_ bridgev2.NetworkConnector = (*telegram.TelegramConnector)(nil)
)
