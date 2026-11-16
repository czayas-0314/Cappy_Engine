// Copyright (C) 2026 Cappy Comm contributors
// Portions adapted from cloud-in-a-bottle/bottled-bridge-core (localmx/ids.go, commit e801a224), AGPL-3.0.
//
// This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General
// Public License as published by the Free Software Foundation, version 3 of the License (AGPL-3.0-only). See LICENSE.

package localmx

import (
	"crypto/hmac"
	"crypto/sha256"
	"strings"

	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"

	"cappy.local/engine/internal/inbox"
	"cappy.local/engine/internal/media"
)

// ServerName is the made-up homeserver every ID lives on. Nothing resolves it.
const ServerName = inbox.ServerName

// OwnerMXID is the one bridgev2 user on every network, and its double puppet.
const OwnerMXID = id.UserID("@owner:" + ServerName)

const mxcPrefix = "mxc://" + ServerName + "/"

// BotMXID is the bridge bot of a network.
func BotMXID(network string) id.UserID {
	return id.UserID("@" + network + "bot:" + ServerName)
}

func (c *Connector) ghostPrefix() string {
	return "@" + c.network + "_"
}

// FormatGhostMXID is reversible and stateless: "@<network>_<encoded remote ID>:cappy.local". Ghost IDs can contain
// phone numbers, so logs redact them.
func (c *Connector) FormatGhostMXID(userID networkid.UserID) id.UserID {
	return id.UserID(c.ghostPrefix() + id.EncodeUserLocalpart(string(userID)) + ":" + ServerName)
}

func (c *Connector) ParseGhostMXID(userID id.UserID) (networkid.UserID, bool) {
	s, ok := strings.CutPrefix(string(userID), c.ghostPrefix())
	if !ok {
		return "", false
	}
	s, ok = strings.CutSuffix(s, ":"+ServerName)
	if !ok {
		return "", false
	}
	decoded, err := id.DecodeUserLocalpart(s)
	if err != nil {
		return "", false
	}
	return networkid.UserID(decoded), true
}

// mac is HMAC-SHA256 under the install key over NUL-separated parts, base32-encoded.
func (c *Connector) mac(parts ...string) string {
	h := hmac.New(sha256.New, c.installKey)
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return inbox.B32.EncodeToString(h.Sum(nil))
}

// RoomIDFor is deterministic, so a chat the bridge deletes and recreates maps back to the same chat.
func (c *Connector) RoomIDFor(key networkid.PortalKey) id.RoomID {
	return id.RoomID("!" + c.mac("room", c.network, string(key.ID), string(key.Receiver))[:26] + ":" + ServerName)
}

// MessageEventID is deterministic, so a message delivered twice (live and by backfill, or again after a restart) is
// stored once.
func (c *Connector) MessageEventID(key networkid.PortalKey, messageID networkid.MessageID, partID networkid.PartID) id.EventID {
	return id.EventID("$m" + c.mac("msg", c.network, string(key.ID), string(key.Receiver), string(messageID), string(partID))[:43])
}

func (c *Connector) ReactionEventID(key networkid.PortalKey, messageID networkid.MessageID, partID networkid.PartID, sender networkid.UserID, emojiID networkid.EmojiID) id.EventID {
	return id.EventID("$r" + c.mac("reaction", c.network, string(key.ID), string(key.Receiver), string(messageID), string(partID), string(sender), string(emojiID))[:43])
}

// RandomEventID is used for state changes, edits, redactions and notices.
func RandomEventID() id.EventID {
	return id.EventID("$x" + inbox.RandomB32(26))
}

// RandomRoomID is used for rooms without a deterministic ID (management rooms).
func RandomRoomID() id.RoomID {
	return id.RoomID("!" + inbox.RandomB32(26) + ":" + ServerName)
}

func ContentURI(sha string) id.ContentURIString {
	return id.ContentURIString(mxcPrefix + sha)
}

// SHAFromContentURI returns "" for URIs that don't point into the local media store.
func SHAFromContentURI(uri id.ContentURIString) string {
	sha, ok := strings.CutPrefix(string(uri), mxcPrefix)
	if !ok || !media.ValidSHA(sha) {
		return ""
	}
	return sha
}
