// Copyright (C) 2026 Cappy Comm contributors
// Portions adapted from cloud-in-a-bottle/bottled-bridge-core (localmx/state.go, commit e801a224), AGPL-3.0.
//
// This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General
// Public License as published by the Free Software Foundation, version 3 of the License (AGPL-3.0-only). See LICENSE.

package localmx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"cappy.local/engine/internal/inbox"
)

// senderFor maps a Matrix user bridgev2 acts as to a contact row. The owner (and a ghost that is one of the owner's
// own logins) is "from me" with no contact; the bridge bot has no contact either.
func (c *Connector) senderFor(tx *inbox.Tx, userID id.UserID) (contactID int64, fromMe bool, err error) {
	switch userID {
	case OwnerMXID:
		return 0, true, nil
	case c.bot.mxid:
		return 0, false, nil
	}
	ghostID, ok := c.ParseGhostMXID(userID)
	if !ok {
		return 0, false, fmt.Errorf("unknown sender on network %s", c.network)
	}
	contactID, err = tx.EnsureContact(c.network, string(ghostID), string(userID))
	if err != nil {
		return 0, false, err
	}
	return contactID, c.isSelf(ghostID), nil
}

func (c *Connector) isSelf(ghostID networkid.UserID) bool {
	return c.br != nil && c.br.GetCachedUserLoginByID(networkid.UserLoginID(ghostID)) != nil
}

func bridgeInfo(evt *event.Event) *event.BridgeEventContent {
	if evt.Type.Type != event.StateBridge.Type && evt.Type.Type != event.StateHalfShotBridge.Type {
		return nil
	}
	if info, ok := evt.Content.Parsed.(*event.BridgeEventContent); ok {
		return info
	}
	var info event.BridgeEventContent
	if len(evt.Content.VeryRaw) > 0 && json.Unmarshal(evt.Content.VeryRaw, &info) == nil {
		return &info
	}
	if evt.Content.Raw != nil {
		if data, err := json.Marshal(evt.Content.Raw); err == nil && json.Unmarshal(data, &info) == nil {
			return &info
		}
	}
	return nil
}

func (c *Connector) createRoom(ctx context.Context, req *mautrix.ReqCreateRoom) (id.RoomID, error) {
	roomID := req.BeeperLocalRoomID
	var info *event.BridgeEventContent
	for _, evt := range req.InitialState {
		if bi := bridgeInfo(evt); bi != nil && bi.Channel.ID != "" {
			info = bi
			break
		}
	}
	chat := inbox.NewChat{Network: c.network, Kind: "group"}
	if roomID == "" || info == nil {
		// A management room or space without a portal: keep it as a hidden system chat rather than failing.
		if roomID == "" {
			roomID = RandomRoomID()
		}
		chat.Kind = "system"
		chat.PortalID = string(roomID)
	} else {
		chat.PortalID = info.Channel.ID
		chat.PortalReceiver = info.Channel.Receiver
		chat.MessageRequest = info.Channel.MessageRequest
		switch {
		case req.IsDirect || info.BeeperRoomTypeV2 == "dm":
			chat.Kind = "dm"
		case info.BeeperRoomTypeV2 == "group_dm":
			chat.Kind = "group_dm"
		case info.BeeperRoomTypeV2 == "space":
			chat.Kind = "space"
		}
	}
	if roomType, _ := req.CreationContent["type"].(event.RoomType); roomType == event.RoomTypeSpace {
		chat.Kind = "space"
	} else if roomTypeStr, _ := req.CreationContent["type"].(string); roomTypeStr == string(event.RoomTypeSpace) {
		chat.Kind = "space"
	}
	chat.RoomID = string(roomID)
	err := c.write(ctx, func(tx *inbox.Tx) error {
		if chat.PortalReceiver != "" {
			accountID, err := tx.EnsureAccount(c.network, chat.PortalReceiver)
			if err != nil {
				return err
			}
			chat.AccountID = accountID
		}
		if err := tx.UpsertChat(chat); err != nil {
			return err
		}
		if req.Name != "" {
			if err := c.applyState(ctx, tx, roomID, c.bot.mxid, event.StateRoomName, "", &event.Content{Parsed: &event.RoomNameEventContent{Name: req.Name}}, 0); err != nil {
				return err
			}
		}
		if req.Topic != "" {
			if err := c.applyState(ctx, tx, roomID, c.bot.mxid, event.StateTopic, "", &event.Content{Parsed: &event.TopicEventContent{Topic: req.Topic}}, 0); err != nil {
				return err
			}
		}
		if req.PowerLevelOverride != nil {
			if err := c.applyState(ctx, tx, roomID, c.bot.mxid, event.StatePowerLevels, "", &event.Content{Parsed: req.PowerLevelOverride}, 0); err != nil {
				return err
			}
		}
		for _, evt := range req.InitialState {
			evtType := evt.Type
			evtType.Class = event.StateEventType
			if err := c.applyState(ctx, tx, roomID, c.bot.mxid, evtType, evt.GetStateKey(), &evt.Content, 0); err != nil {
				return err
			}
		}
		for _, invitee := range req.Invite {
			if err := c.applyState(ctx, tx, roomID, c.bot.mxid, event.StateMember, string(invitee),
				&event.Content{Parsed: &event.MemberEventContent{Membership: event.MembershipInvite, IsDirect: req.IsDirect}}, 0); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return roomID, nil
}

func (c *Connector) sendState(ctx context.Context, sender id.UserID, roomID id.RoomID, evtType event.Type, stateKey string, content *event.Content, ts time.Time) (id.EventID, error) {
	evtType.Class = event.StateEventType
	err := c.write(ctx, func(tx *inbox.Tx) error {
		if _, err := tx.ChatByRoom(string(roomID)); err != nil {
			return fmt.Errorf("state for unknown room: %w", err)
		}
		return c.applyState(ctx, tx, roomID, sender, evtType, stateKey, content, timestampMS(ts))
	})
	return RandomEventID(), err
}

// applyState records a state event and updates the chat columns derived from it. ts is zero for state set while
// syncing, and the remote change's time otherwise; only the latter appears in the timeline (as a kind "update" row
// whose update_json the host localizes).
func (c *Connector) applyState(ctx context.Context, tx *inbox.Tx, roomID id.RoomID, sender id.UserID, evtType event.Type, stateKey string, content *event.Content, ts int64) error {
	if content.Parsed == nil {
		if err := content.ParseRaw(evtType); err != nil && !errors.Is(err, event.ErrUnsupportedContentType) && !errors.Is(err, event.ErrContentAlreadyParsed) {
			return fmt.Errorf("failed to parse %s content: %w", evtType.Type, err)
		}
	}
	raw, err := json.Marshal(content)
	if err != nil {
		return err
	}
	room := string(roomID)
	if err = tx.SetRoomState(room, evtType.Type, stateKey, string(sender), string(raw)); err != nil {
		return err
	}
	inTimeline := ts != 0
	if exclude, _ := content.Raw["com.beeper.exclude_from_timeline"].(bool); exclude {
		inTimeline = false
	}
	update := func(author id.UserID, data map[string]any) error {
		if !inTimeline || author == c.bot.mxid {
			return nil
		}
		contactID, fromMe, err := c.senderFor(tx, author)
		if err != nil {
			return err
		}
		updateJSON, err := json.Marshal(data)
		if err != nil {
			return err
		}
		_, err = tx.PutMessage(inbox.NewMessage{EventID: string(RandomEventID()), RoomID: room, SenderContactID: contactID,
			FromMe: fromMe, TS: ts, Kind: "update", UpdateJSON: string(updateJSON), Source: inbox.SourceLive})
		return err
	}

	switch parsed := content.Parsed.(type) {
	case *event.RoomNameEventContent:
		if err = tx.SetChatField(room, "title", nullable(parsed.Name), "info"); err != nil {
			return err
		}
		return update(sender, map[string]any{"type": "rename", "title": parsed.Name})
	case *event.TopicEventContent:
		return tx.SetChatField(room, "topic", nullable(parsed.Topic), "info")
	case *event.RoomAvatarEventContent:
		if err = tx.SetChatField(room, "avatar_sha", nullable(SHAFromContentURI(parsed.URL)), "info"); err != nil {
			return err
		}
		return update(sender, map[string]any{"type": "avatar"})
	case *event.BeeperDisappearingTimer:
		if err = tx.SetChatField(room, "disappear_type", nullable(string(parsed.Type)), "info"); err != nil {
			return err
		}
		if err = tx.SetChatField(room, "disappear_ms", parsed.Timer.Milliseconds(), "info"); err != nil {
			return err
		}
		return update(sender, map[string]any{"type": "disappearing", "mode": string(parsed.Type), "timerMs": parsed.Timer.Milliseconds()})
	case *event.BridgeEventContent:
		if parsed.Channel.ID != "" {
			return tx.SetChatField(room, "message_request", boolInt(parsed.Channel.MessageRequest), "info")
		}
		return nil
	case *event.SpaceParentEventContent:
		if _, err = tx.ChatByRoom(stateKey); errors.Is(err, inbox.ErrNotFound) {
			return nil
		} else if err != nil {
			return err
		}
		return tx.SetChatField(room, "parent_room_id", stateKey, "info")
	case *event.SpaceChildEventContent:
		if _, err = tx.ChatByRoom(stateKey); errors.Is(err, inbox.ErrNotFound) {
			return nil
		} else if err != nil {
			return err
		}
		var parent any
		if len(parsed.Via) > 0 {
			parent = room
		}
		return tx.SetChatField(stateKey, "parent_room_id", parent, "info")
	case *event.MemberEventContent:
		return c.applyMembership(tx, roomID, sender, id.UserID(stateKey), parsed, update)
	case *event.TombstoneEventContent:
		// bridgev2 merged this room into another one (eg a WhatsApp phone-number DM into its LID chat) and then calls
		// DeleteRoom with puppetsOnly, which keeps the room. Hide the old chat so the host does not list it twice.
		return tx.SetChatField(room, "deleted", 1, "deleted")
	}
	return nil
}

func (c *Connector) applyMembership(tx *inbox.Tx, roomID id.RoomID, sender, target id.UserID, content *event.MemberEventContent, update func(id.UserID, map[string]any) error) error {
	if target == c.bot.mxid {
		return nil
	}
	room := string(roomID)
	contactID, isOwner, err := c.senderFor(tx, target)
	if err != nil {
		return nil // a member we can't map (eg another network's ghost): keep only the raw state
	}
	if target == OwnerMXID {
		isOwner = true
	}
	previous, err := tx.SetMembership(room, contactID, isOwner && target == OwnerMXID, string(content.Membership))
	if err != nil {
		return err
	}
	chat, err := tx.ChatByRoom(room)
	if err != nil {
		return err
	}
	if chat.Kind == "dm" && !isOwner && (content.Membership == event.MembershipJoin || content.Membership == event.MembershipInvite) {
		if err = tx.SetChatField(room, "dm_contact_id", contactID, "info"); err != nil {
			return err
		}
	}
	if chat.Kind != "group" && chat.Kind != "group_dm" {
		return nil
	}
	wasIn := previous == string(event.MembershipJoin)
	isIn := content.Membership == event.MembershipJoin
	targetRef := map[string]any{}
	if contactID != 0 {
		targetRef["target"] = inbox.ContactIDString(contactID)
	}
	switch {
	case wasIn == isIn:
		return nil
	case isIn:
		return update(target, merge(targetRef, "type", "join"))
	case sender != target && sender != c.bot.mxid:
		return update(sender, merge(targetRef, "type", "remove"))
	default:
		return update(target, merge(targetRef, "type", "leave"))
	}
}

func merge(m map[string]any, k string, v any) map[string]any {
	m[k] = v
	return m
}

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}
