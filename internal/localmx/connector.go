// Copyright (C) 2026 Cappy Comm contributors
// Portions adapted from cloud-in-a-bottle/bottled-bridge-core (localmx/connector.go, commit e801a224), AGPL-3.0.
//
// This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General
// Public License as published by the Free Software Foundation, version 3 of the License (AGPL-3.0-only). See LICENSE.

// Package localmx is the Matrix side of a bridgev2 bridge, without Matrix: it implements bridgev2's MatrixConnector
// and MatrixAPI by writing what the bridge would send to a homeserver into the local inbox.db. Rooms are chats, ghost
// users are contacts, and the one bridge user (@owner:cappy.local) is the device owner. There is one Connector per
// network (bridge); all of them share the inbox, the media store and the notifier.
package localmx

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"cappy.local/engine/internal/inbox"
	"cappy.local/engine/internal/media"
)

// Notifier receives what changed after each committed inbox write, and bridge-wide states.
type Notifier interface {
	Changed(ctx context.Context, ch *inbox.Changes)
	NetworkState(network string, state status.BridgeState)
}

type Connector struct {
	network    string
	installKey []byte
	inbox      *inbox.DB
	media      *media.Store
	notifier   Notifier
	log        zerolog.Logger

	br    *bridgev2.Bridge
	bot   *intent
	owner *intent

	stateMu     sync.Mutex
	globalState status.BridgeState
}

var (
	_ bridgev2.MatrixConnector                       = (*Connector)(nil)
	_ bridgev2.MatrixConnectorWithArbitraryRoomState = (*Connector)(nil)
	_ bridgev2.MatrixConnectorWithBridgeIdentifier   = (*Connector)(nil)
)

// New makes the connector for one network. The bot and owner intents exist straight away, because NewBridge reads
// BotIntent() right after Init and before Start.
func New(network string, ib *inbox.DB, ms *media.Store, notifier Notifier, log zerolog.Logger) *Connector {
	c := &Connector{network: network, installKey: ib.InstallKey(), inbox: ib, media: ms, notifier: notifier, log: log}
	c.bot = &intent{c: c, mxid: BotMXID(network)}
	c.owner = &intent{c: c, mxid: OwnerMXID}
	return c
}

func (c *Connector) Network() string { return c.network }

func (c *Connector) Init(br *bridgev2.Bridge) { c.br = br }

func (c *Connector) Start(ctx context.Context) error { return nil }
func (c *Connector) PreStop()                        {}
func (c *Connector) Stop()                           {}

func (c *Connector) GetCapabilities() *bridgev2.MatrixCapabilities {
	// Batch sending turns on backfill: history is inserted with its original timestamps in one transaction.
	return &bridgev2.MatrixCapabilities{BatchSending: true, ExtraProfileMeta: true}
}

func (c *Connector) GetUniqueBridgeID() string {
	return "cappy-" + c.network
}

func (c *Connector) GhostIntent(userID networkid.UserID) bridgev2.MatrixAPI {
	return &intent{c: c, mxid: c.FormatGhostMXID(userID), ghost: userID}
}

// NewUserIntent is how bridgev2 gets the owner's double puppet, used for "from me" events made on other devices.
func (c *Connector) NewUserIntent(ctx context.Context, userID id.UserID, accessToken string) (bridgev2.MatrixAPI, string, error) {
	if userID != OwnerMXID {
		return nil, "", errors.New("only the owner has a user intent")
	}
	return c.owner, accessToken, nil
}

func (c *Connector) BotIntent() bridgev2.MatrixAPI { return c.bot }

// GlobalState is the latest bridge-wide (not per-login) state.
func (c *Connector) GlobalState() status.BridgeState {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.globalState
}

// SendBridgeStatus never fails: bridgev2 retries a global state until it returns nil.
func (c *Connector) SendBridgeStatus(ctx context.Context, state *status.BridgeState) error {
	if state == nil {
		return nil
	}
	if state.RemoteID == "" {
		c.stateMu.Lock()
		c.globalState = *state
		c.stateMu.Unlock()
		if c.notifier != nil {
			c.notifier.NetworkState(c.network, *state)
		}
		return nil
	}
	_ = c.write(ctx, func(tx *inbox.Tx) error {
		accountID, err := tx.EnsureAccount(c.network, string(state.RemoteID))
		if err != nil {
			return err
		}
		if state.RemoteName != "" {
			if err = tx.SetAccountName(accountID, state.RemoteName); err != nil {
				return err
			}
		}
		ts := int64(0)
		if !state.Timestamp.IsZero() {
			ts = state.Timestamp.UnixMilli()
		}
		return tx.SetAccountState(accountID, inbox.AccountState{
			State:      string(state.StateEvent),
			Error:      string(state.Error),
			Message:    state.Message,
			UserAction: string(state.UserAction),
			TS:         ts,
		})
	})
	return nil
}

func (c *Connector) SendMessageStatus(ctx context.Context, ms *bridgev2.MessageStatus, info *bridgev2.MessageStatusEventInfo) {
	if ms == nil || info == nil || info.SourceEventID == "" {
		return
	}
	if info.EventType != (event.Type{}) && info.EventType != event.EventMessage && info.EventType != event.EventSticker {
		return
	}
	var newStatus, errText string
	switch ms.Status {
	case event.MessageStatusSuccess:
		newStatus = "sent"
		if len(ms.DeliveredTo) > 0 {
			newStatus = "delivered"
		}
	case event.MessageStatusPending:
		return
	default:
		newStatus = "failed"
		errText = string(ms.ErrorReason)
		if errText == "" {
			errText = "unknown"
		}
	}
	_ = c.write(ctx, func(tx *inbox.Tx) error {
		err := tx.SetMessageStatus(string(info.SourceEventID), newStatus, errText)
		if errors.Is(err, inbox.ErrNotFound) {
			return nil // a status for something that isn't an outgoing message row
		}
		return err
	})
}

func (c *Connector) GenerateContentURI(ctx context.Context, mediaID networkid.MediaID) (id.ContentURIString, error) {
	return "", bridgev2.ErrDirectMediaNotEnabled
}

func (c *Connector) ParseContentURI(ctx context.Context, contentURI id.ContentURIString) (networkid.MediaID, error) {
	return nil, bridgev2.ErrDirectMediaNotEnabled
}

func (c *Connector) GetPowerLevels(ctx context.Context, roomID id.RoomID) (*event.PowerLevelsEventContent, error) {
	state, err := c.inbox.RoomState(ctx, string(roomID), event.StatePowerLevels.Type)
	if err != nil {
		return nil, err
	}
	pl := &event.PowerLevelsEventContent{}
	if entry, ok := state[""]; ok {
		if err = json.Unmarshal([]byte(entry.Content), pl); err != nil {
			return nil, err
		}
	}
	return pl, nil
}

func (c *Connector) GetMembers(ctx context.Context, roomID id.RoomID) (map[id.UserID]*event.MemberEventContent, error) {
	state, err := c.inbox.RoomState(ctx, string(roomID), event.StateMember.Type)
	if err != nil {
		return nil, err
	}
	members := make(map[id.UserID]*event.MemberEventContent, len(state))
	for key, entry := range state {
		var content event.MemberEventContent
		if err = json.Unmarshal([]byte(entry.Content), &content); err != nil {
			return nil, err
		}
		members[id.UserID(key)] = &content
	}
	return members, nil
}

func (c *Connector) GetMemberInfo(ctx context.Context, roomID id.RoomID, userID id.UserID) (*event.MemberEventContent, error) {
	members, err := c.GetMembers(ctx, roomID)
	if err != nil {
		return nil, err
	}
	if m, ok := members[userID]; ok {
		return m, nil
	}
	return &event.MemberEventContent{Membership: event.MembershipLeave}, nil
}

func (c *Connector) GetStateEvent(ctx context.Context, roomID id.RoomID, eventType event.Type, stateKey string) (*event.Event, error) {
	state, err := c.inbox.RoomState(ctx, string(roomID), eventType.Type)
	if err != nil {
		return nil, err
	}
	entry, ok := state[stateKey]
	if !ok {
		return nil, nil
	}
	evt := &event.Event{Type: eventType, StateKey: &stateKey, Sender: id.UserID(entry.Sender), RoomID: roomID}
	evt.Type.Class = event.StateEventType
	if err = json.Unmarshal([]byte(entry.Content), &evt.Content); err != nil {
		return nil, err
	}
	if err = evt.Content.ParseRaw(evt.Type); err != nil && !errors.Is(err, event.ErrContentAlreadyParsed) && !errors.Is(err, event.ErrUnsupportedContentType) {
		return nil, err
	}
	return evt, nil
}

// BatchSend stores a backfill batch in one transaction, with the IDs bridgev2 assigned (deterministic, so a batch
// delivered twice is stored once). Backfill never produces message.new notifications.
func (c *Connector) BatchSend(ctx context.Context, roomID id.RoomID, req *mautrix.ReqBeeperBatchSend, extras []*bridgev2.MatrixSendExtra) (*mautrix.RespBeeperBatchSend, error) {
	resp := &mautrix.RespBeeperBatchSend{EventIDs: make([]id.EventID, len(req.Events))}
	err := c.write(ctx, func(tx *inbox.Tx) error {
		var lastTS int64
		for i, evt := range req.Events {
			te := timelineEvent{roomID: roomID, eventID: evt.ID, evtType: evt.Type, sender: evt.Sender, ts: evt.Timestamp,
				content: &evt.Content, source: inbox.SourceBackfill}
			if i < len(extras) && extras[i] != nil {
				te.meta = extras[i].MessageMeta
			}
			if te.eventID == "" {
				te.eventID = RandomEventID()
			}
			if err := c.applyTimelineEvent(ctx, tx, te); err != nil {
				return err
			}
			resp.EventIDs[i] = te.eventID
			lastTS = max(lastTS, evt.Timestamp)
		}
		if req.MarkReadBy == OwnerMXID && lastTS > 0 {
			return tx.SetLastRead(string(roomID), lastTS)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Connector) GenerateDeterministicRoomID(portalKey networkid.PortalKey) id.RoomID {
	return c.RoomIDFor(portalKey)
}

func (c *Connector) GenerateDeterministicEventID(roomID id.RoomID, portalKey networkid.PortalKey, messageID networkid.MessageID, partID networkid.PartID) id.EventID {
	return c.MessageEventID(portalKey, messageID, partID)
}

func (c *Connector) GenerateReactionEventID(roomID id.RoomID, targetMessage *database.Message, sender networkid.UserID, emojiID networkid.EmojiID) id.EventID {
	if targetMessage == nil {
		return RandomEventID()
	}
	return c.ReactionEventID(targetMessage.Room, targetMessage.ID, targetMessage.PartID, sender, emojiID)
}

func (c *Connector) ServerName() string { return ServerName }

// write runs fn in one inbox transaction, then tells the notifier what changed (after the commit).
func (c *Connector) write(ctx context.Context, fn func(tx *inbox.Tx) error) error {
	ch, err := c.inbox.Tx(ctx, fn)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Str("network", c.network).Msg("Failed to write to inbox")
		return err
	}
	if c.notifier != nil && !ch.Empty() {
		c.notifier.Changed(ctx, ch)
	}
	return nil
}
