// Copyright (C) 2026 Cappy Comm contributors
// Portions adapted from cloud-in-a-bottle/bottled-bridge-core (localmx/timeline.go, commit e801a224), AGPL-3.0.
//
// This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General
// Public License as published by the Free Software Foundation, version 3 of the License (AGPL-3.0-only). See LICENSE.

package localmx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/rs/zerolog"
	"go.mau.fi/util/variationselector"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"cappy.local/engine/internal/inbox"
)

// timelineEvent is a non-state event bridgev2 sends into a room, live or in a backfill batch.
type timelineEvent struct {
	roomID  id.RoomID
	eventID id.EventID
	evtType event.Type
	sender  id.UserID
	ts      int64
	content *event.Content
	meta    *database.Message
	source  string
}

func (c *Connector) applyTimelineEvent(ctx context.Context, tx *inbox.Tx, te timelineEvent) error {
	if te.content.Parsed == nil {
		if err := te.content.ParseRaw(te.evtType); err != nil && !errors.Is(err, event.ErrUnsupportedContentType) && !errors.Is(err, event.ErrContentAlreadyParsed) {
			return fmt.Errorf("failed to parse %s content: %w", te.evtType.Type, err)
		}
	}
	raw, err := json.Marshal(te.content)
	if err != nil {
		return err
	}
	room := string(te.roomID)
	if _, err = tx.ChatByRoom(room); err != nil {
		return fmt.Errorf("event for unknown room: %w", err)
	}
	replaces := ""
	if msg, ok := te.content.Parsed.(*event.MessageEventContent); ok {
		replaces = string(msg.RelatesTo.GetReplaceID())
	}
	if err = tx.PutRawEvent(string(te.eventID), room, te.evtType.Type, string(te.sender), te.ts, string(raw), replaces); err != nil {
		return err
	}
	contactID, fromMe, err := c.senderFor(tx, te.sender)
	if err != nil {
		return err
	}
	log := zerolog.Ctx(ctx)

	if action, _ := te.content.Raw["com.beeper.action_message"].(map[string]any); action != nil && action["type"] == "disappearing_timer" {
		// Announced alongside the timer change itself, which is already in the timeline.
		return nil
	}
	switch te.evtType {
	case event.EventMessage, event.EventSticker:
		msg, ok := te.content.Parsed.(*event.MessageEventContent)
		if !ok {
			return fmt.Errorf("unexpected content %T for %s", te.content.Parsed, te.evtType.Type)
		}
		if target := msg.RelatesTo.GetReplaceID(); target != "" {
			newContent := msg.NewContent
			if newContent == nil {
				newContent = msg
			}
			formatted := ""
			if newContent.Format == event.FormatHTML {
				formatted = newContent.FormattedBody
			}
			err = tx.EditMessage(string(target), newContent.Body, formatted, te.ts)
			if errors.Is(err, inbox.ErrNotFound) {
				log.Debug().Msg("Edit target not stored; ignoring edit")
				return nil
			}
			return err
		}
		_, err = tx.PutMessage(c.convertMessage(room, contactID, fromMe, te, msg))
		return err
	case event.EventReaction:
		reaction, ok := te.content.Parsed.(*event.ReactionEventContent)
		if !ok {
			return fmt.Errorf("unexpected content %T for reaction", te.content.Parsed)
		}
		err = tx.PutReaction(inbox.NewReaction{
			EventID:         string(te.eventID),
			TargetEventID:   string(reaction.RelatesTo.EventID),
			SenderContactID: contactID,
			FromMe:          fromMe,
			Key:             variationselector.Remove(reaction.RelatesTo.Key),
			TS:              te.ts,
		})
		if errors.Is(err, inbox.ErrNotFound) {
			log.Debug().Msg("Reaction target not stored; ignoring reaction")
			return nil
		}
		return err
	case event.EventRedaction:
		redaction, ok := te.content.Parsed.(*event.RedactionEventContent)
		if !ok {
			return fmt.Errorf("unexpected content %T for redaction", te.content.Parsed)
		}
		mode := "unsend"
		if te.sender == c.bot.mxid && redaction.Reason == "Message disappeared" {
			mode = "disappear"
			if viewLimited, _ := te.content.Raw["com.beeper.view_limited"].(bool); viewLimited {
				mode = "viewed"
			}
		}
		err = tx.Redact(string(redaction.Redacts), te.ts, mode)
		if errors.Is(err, inbox.ErrNotFound) {
			return nil
		}
		return err
	default:
		// Other event types (polls, calls, ...) are kept only as raw events for GetEvent.
		return nil
	}
}

func (c *Connector) convertMessage(room string, contactID int64, fromMe bool, te timelineEvent, msg *event.MessageEventContent) inbox.NewMessage {
	msg.RemoveReplyFallback()
	m := inbox.NewMessage{
		EventID:         string(te.eventID),
		RoomID:          room,
		SenderContactID: contactID,
		FromMe:          fromMe,
		TS:              te.ts,
		Kind:            "message",
		MsgType:         string(msg.MsgType),
		Body:            msg.Body,
		ReplyTo:         string(msg.RelatesTo.GetNonFallbackReplyTo()),
		ThreadRoot:      string(msg.RelatesTo.GetThreadParent()),
		Source:          te.source,
	}
	if msg.Format == event.FormatHTML {
		m.FormattedBody = msg.FormattedBody
	}
	if te.meta != nil {
		m.RemoteID = string(te.meta.ID)
		m.PartID = string(te.meta.PartID)
	}
	if te.sender == c.bot.mxid || msg.MsgType == event.MsgNotice {
		// The bridge's own notices, eg "failed to bridge this message".
		m.Kind = "notice"
	}
	if len(msg.BeeperLinkPreviews) > 0 {
		if previews, err := json.Marshal(msg.BeeperLinkPreviews); err == nil {
			m.LinkPreviews = string(previews)
		}
	}
	if msg.MsgType == event.MsgLocation && msg.GeoURI != "" && !strings.Contains(msg.Body, msg.GeoURI) {
		m.Body = strings.TrimSpace(msg.Body + "\n" + msg.GeoURI)
	}
	isSticker := te.evtType == event.EventSticker
	if isSticker || msg.MsgType.IsMedia() {
		m.Attachment = convertAttachment(msg, isSticker)
		if m.Attachment.Caption != "" {
			m.Body = m.Attachment.Caption
		} else {
			m.Body = ""
		}
	}
	return m
}

func convertAttachment(msg *event.MessageEventContent, isSticker bool) *inbox.StoredAttachment {
	a := &inbox.StoredAttachment{
		MsgType:   string(msg.MsgType),
		Name:      msg.FileName,
		IsVoice:   msg.MSC3245Voice != nil,
		IsSticker: isSticker,
		SHA256:    SHAFromContentURI(msg.URL),
	}
	if isSticker {
		a.MsgType = "m.sticker"
	}
	if a.Name == "" {
		a.Name = msg.Body
	} else if msg.Body != msg.FileName {
		a.Caption = msg.Body
	}
	if info := msg.Info; info != nil {
		a.Mime = info.MimeType
		a.Size = int64(info.Size)
		a.Width = info.Width
		a.Height = info.Height
		a.DurationMS = info.Duration
		a.Blurhash = info.Blurhash
		if a.Blurhash == "" {
			a.Blurhash = info.AnoaBlurhash
		}
	}
	return a
}

// sendTimelineEvent stores an event bridgev2 sends live and returns its event ID. Messages and reactions get the same
// deterministic IDs backfill would give them, so a message seen both live and in backfill is stored once.
func (c *Connector) sendTimelineEvent(ctx context.Context, sender id.UserID, roomID id.RoomID, evtType event.Type, content *event.Content, extra *bridgev2.MatrixSendExtra) (id.EventID, error) {
	te := timelineEvent{roomID: roomID, evtType: evtType, sender: sender, ts: nowMS(), content: content, source: inbox.SourceLive}
	if extra != nil {
		te.meta = extra.MessageMeta
		if ts := timestampMS(extra.Timestamp); ts != 0 {
			te.ts = ts
		}
	}
	te.eventID = RandomEventID()
	if content.Parsed == nil {
		_ = content.ParseRaw(evtType)
	}
	switch evtType {
	case event.EventMessage, event.EventSticker:
		msg, ok := content.Parsed.(*event.MessageEventContent)
		if ok && msg.RelatesTo.GetReplaceID() == "" && te.meta != nil {
			te.eventID = c.MessageEventID(te.meta.Room, te.meta.ID, te.meta.PartID)
		}
	case event.EventReaction:
		if extra != nil && extra.ReactionMeta != nil {
			r := extra.ReactionMeta
			te.eventID = c.ReactionEventID(r.Room, r.MessageID, r.MessagePartID, r.SenderID, r.EmojiID)
		}
	}
	err := c.write(ctx, func(tx *inbox.Tx) error {
		return c.applyTimelineEvent(ctx, tx, te)
	})
	return te.eventID, err
}
