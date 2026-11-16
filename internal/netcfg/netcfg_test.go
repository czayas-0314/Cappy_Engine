package netcfg

import (
	"testing"

	telegram "go.mau.fi/mautrix-telegram/pkg/connector"
	whatsapp "go.mau.fi/mautrix-whatsapp/pkg/connector"
)

func TestWhatsAppOverrides(t *testing.T) {
	wa := &whatsapp.WhatsAppConnector{}
	if err := Apply(wa, WhatsApp()); err != nil {
		t.Fatal(err)
	}
	c := wa.Config
	if c.OSName != "Cappy Comm" || c.EnableStatusBroadcast || c.URLPreviews || c.LazyAvatars ||
		c.HistorySync.MediaRequests.AutoRequestMedia || c.HistorySync.MaxInitialConversations != -1 ||
		c.AnimatedSticker.Target != "disable" || string(c.ArchiveTag) != "m.lowpriority" {
		t.Fatalf("overrides not applied: %+v", c)
	}
	// Untouched defaults survive the merge.
	if c.HistorySync.DispatchWait == 0 || c.PinnedTag == "" {
		t.Fatal("defaults lost")
	}
}

func TestTelegramUnconfiguredAndConfigured(t *testing.T) {
	tg := &telegram.TelegramConnector{}
	if err := Apply(tg, Telegram("0.1.0", nil)); err != nil {
		t.Fatal(err)
	}
	if err := tg.ValidateConfig(); err == nil {
		t.Fatal("telegram without credentials must not validate")
	}
	if tg.Config.DeviceInfo.DeviceModel != "Cappy Comm" || tg.Config.SavedMessagesAvatar != "" {
		t.Fatal("device info override not applied")
	}
	tg = &telegram.TelegramConnector{}
	// Synthetic values: this test never contacts Telegram.
	if err := Apply(tg, Telegram("0.1.0", &TelegramCredentials{APIID: 12345, APIHash: "00000000000000000000000000000000"})); err != nil {
		t.Fatal(err)
	}
	if err := tg.ValidateConfig(); err != nil {
		t.Fatalf("telegram with credentials should validate: %v", err)
	}
}
