// Package netcfg builds each network connector's config from its embedded example YAML plus Cappy's overrides.
//
// The connectors compile templates (displayname_template) inside their YAML unmarshalers, so overrides are merged into
// the YAML node tree first and the result is decoded exactly once.
package netcfg

import (
	"fmt"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
	"maunium.net/go/mautrix/bridgev2"
)

// TelegramCredentials come from the host's hello request only. They are kept in memory and never written to disk or
// logs.
type TelegramCredentials struct {
	APIID   int
	APIHash string
}

// WhatsApp returns Cappy's overrides for mautrix-whatsapp (DESIGN.md section 6).
func WhatsApp() map[string]any {
	return map[string]any{
		"os_name":                 "Cappy Comm",
		"displayname_template":    `{{or .FullName .BusinessName .PushName .Phone .RedactedPhone "Unknown user"}}`,
		"enable_status_broadcast": false,
		"archive_tag":             "m.lowpriority",
		"animated_sticker.target": "disable",
		"url_previews":            false,
		"lazy_avatars":            false,
		"history_sync.media_requests.auto_request_media": false,
		"history_sync.max_initial_conversations":         -1,
	}
}

// Telegram returns Cappy's overrides for mautrix-telegram. Without credentials the placeholder api_id/api_hash stay,
// which the connector's ValidateConfig rejects, so the network reports needs_configuration.
func Telegram(engineVersion string, creds *TelegramCredentials) map[string]any {
	o := map[string]any{
		"device_info.device_model":   "Cappy Comm",
		"device_info.system_version": "macOS",
		"device_info.app_version":    engineVersion,
		"animated_sticker.target":    "disable",
		"saved_message_avatar":       "",
	}
	if creds != nil && creds.APIID != 0 && creds.APIHash != "" {
		o["api_id"] = creds.APIID
		o["api_hash"] = creds.APIHash
	}
	return o
}

// Apply decodes conn's embedded example config with overrides merged in. Keys are dotted paths.
func Apply(conn bridgev2.NetworkConnector, overrides map[string]any) error {
	example, data, _ := conn.GetConfig()
	if data == nil {
		return nil
	}
	var root yaml.Node
	if example != "" {
		if err := yaml.Unmarshal([]byte(example), &root); err != nil {
			return fmt.Errorf("example config: %w", err)
		}
	}
	var doc *yaml.Node
	if root.Kind == yaml.DocumentNode && len(root.Content) == 1 {
		doc = root.Content[0]
	} else {
		doc = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	}
	keys := make([]string, 0, len(overrides))
	for k := range overrides {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := setPath(doc, strings.Split(k, "."), overrides[k]); err != nil {
			return fmt.Errorf("override %s: %w", k, err)
		}
	}
	if err := doc.Decode(data); err != nil {
		return fmt.Errorf("decode config: %w", err)
	}
	return nil
}

func setPath(node *yaml.Node, path []string, value any) error {
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("not a mapping")
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value != path[0] {
			continue
		}
		if len(path) == 1 {
			return encodeInto(node.Content[i+1], value)
		}
		child := node.Content[i+1]
		if child.Kind != yaml.MappingNode {
			*child = yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		}
		return setPath(child, path[1:], value)
	}
	key := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: path[0]}
	val := &yaml.Node{}
	if len(path) == 1 {
		if err := encodeInto(val, value); err != nil {
			return err
		}
	} else {
		*val = yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		if err := setPath(val, path[1:], value); err != nil {
			return err
		}
	}
	node.Content = append(node.Content, key, val)
	return nil
}

func encodeInto(dst *yaml.Node, value any) error {
	var n yaml.Node
	if err := n.Encode(value); err != nil {
		return err
	}
	*dst = n
	return nil
}
