package pincheck

import (
	"testing"

	"gopkg.in/yaml.v3"

	telegram "go.mau.fi/mautrix-telegram/pkg/connector"
	whatsapp "go.mau.fi/mautrix-whatsapp/pkg/connector"
)

// TestExampleConfigsDecode proves the engine can take each connector's
// defaults from its embedded example config without mxmain or a YAML file.
// No network access: nothing is initialised or started.
func TestExampleConfigsDecode(t *testing.T) {
	wa := &whatsapp.WhatsAppConnector{}
	example, data, _ := wa.GetConfig()
	if err := yaml.Unmarshal([]byte(example), data); err != nil {
		t.Fatalf("whatsapp example config: %v", err)
	}
	if wa.Config.HistorySync.MaxInitialConversations == 0 && wa.Config.OSName == "" {
		t.Fatal("whatsapp defaults not applied")
	}

	tg := &telegram.TelegramConnector{}
	example, data, _ = tg.GetConfig()
	if err := yaml.Unmarshal([]byte(example), data); err != nil {
		t.Fatalf("telegram example config: %v", err)
	}
	// The example ships placeholder API credentials, which ValidateConfig rejects:
	// Telegram must stay unconfigured until the owner supplies an api_id/api_hash.
	if err := tg.ValidateConfig(); err == nil {
		t.Fatal("expected telegram ValidateConfig to reject placeholder api_hash")
	}
}
