// Copyright (C) 2026 Cappy Comm contributors
// Portions adapted from cloud-in-a-bottle/bottled-bridge-core (localmx/intent.go, commit e801a224), AGPL-3.0.
//
// This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General
// Public License as published by the Free Software Foundation, version 3 of the License (AGPL-3.0-only). See LICENSE.

package localmx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"cappy.local/engine/internal/inbox"
)

// intent acts as one Matrix user: the bridge bot, the owner (double puppet) or a ghost.
type intent struct {
	c     *Connector
	mxid  id.UserID
	ghost networkid.UserID
}

var _ bridgev2.MatrixAPI = (*intent)(nil)

func (in *intent) GetMXID() id.UserID   { return in.mxid }
func (in *intent) IsDoublePuppet() bool { return in.mxid == OwnerMXID }

func (in *intent) SendMessage(ctx context.Context, roomID id.RoomID, eventType event.Type, content *event.Content, extra *bridgev2.MatrixSendExtra) (*mautrix.RespSendEvent, error) {
	eventID, err := in.c.sendTimelineEvent(ctx, in.mxid, roomID, eventType, content, extra)
	if err != nil {
		return nil, err
	}
	return &mautrix.RespSendEvent{EventID: eventID}, nil
}

func (in *intent) SendState(ctx context.Context, roomID id.RoomID, eventType event.Type, stateKey string, content *event.Content, ts time.Time) (*mautrix.RespSendEvent, error) {
	eventID, err := in.c.sendState(ctx, in.mxid, roomID, eventType, stateKey, content, ts)
	if err != nil {
		return nil, err
	}
	return &mautrix.RespSendEvent{EventID: eventID}, nil
}

// MarkRead is the owner reading on another device, or another person's read receipt. An unknown target is ignored.
func (in *intent) MarkRead(ctx context.Context, roomID id.RoomID, eventID id.EventID, ts time.Time) error {
	if in.mxid == in.c.bot.mxid {
		return nil
	}
	return in.c.write(ctx, func(tx *inbox.Tx) error {
		ref, err := tx.MessageRef(string(eventID))
		if errors.Is(err, inbox.ErrNotFound) {
			return nil
		} else if err != nil {
			return err
		}
		if in.mxid == OwnerMXID {
			return tx.SetLastRead(ref.RoomID, ref.TS)
		}
		contactID, fromMe, err := in.c.senderFor(tx, in.mxid)
		if err != nil || fromMe {
			return err
		}
		at := timestampMS(ts)
		if at == 0 {
			at = nowMS()
		}
		return tx.SetReceipt(ref.RoomID, contactID, string(eventID), ref.TS, at)
	})
}

func (in *intent) MarkUnread(ctx context.Context, roomID id.RoomID, unread bool) error {
	return in.c.write(ctx, func(tx *inbox.Tx) error {
		return tx.SetChatField(string(roomID), "marked_unread", boolInt(unread), "read")
	})
}

// MarkTyping is a no-op: typing indicators are out of scope for the spike.
func (in *intent) MarkTyping(ctx context.Context, roomID id.RoomID, typingType bridgev2.TypingType, timeout time.Duration) error {
	return nil
}

func (in *intent) DownloadMedia(ctx context.Context, uri id.ContentURIString, file *event.EncryptedFileInfo) ([]byte, error) {
	sha := SHAFromContentURI(uri)
	if sha == "" {
		return nil, mautrix.MNotFound
	}
	return in.c.media.Read(sha)
}

func (in *intent) DownloadMediaToFile(ctx context.Context, uri id.ContentURIString, file *event.EncryptedFileInfo, writable bool, callback func(*os.File) error) error {
	sha := SHAFromContentURI(uri)
	if sha == "" {
		return mautrix.MNotFound
	}
	src, err := in.c.media.Open(sha)
	if err != nil {
		return err
	}
	defer src.Close()
	if !writable {
		return callback(src)
	}
	tmp, err := os.CreateTemp(in.c.media.TmpDir(), "dl-")
	if err != nil {
		return err
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()
	if _, err = io.Copy(tmp, src); err != nil {
		return err
	}
	if _, err = tmp.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return callback(tmp)
}

func (in *intent) UploadMedia(ctx context.Context, roomID id.RoomID, data []byte, fileName, mimeType string) (id.ContentURIString, *event.EncryptedFileInfo, error) {
	sha, err := in.c.media.Put(data)
	if err != nil {
		return "", nil, err
	}
	return ContentURI(sha), nil, nil
}

func (in *intent) UploadMediaStream(ctx context.Context, roomID id.RoomID, size int64, requireFile bool, cb bridgev2.FileStreamCallback) (id.ContentURIString, *event.EncryptedFileInfo, error) {
	tmp, err := os.CreateTemp(in.c.media.TmpDir(), "up-")
	if err != nil {
		return "", nil, err
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()
	res, err := cb(tmp)
	if err != nil {
		return "", nil, bridgev2.CallbackError{Type: "write", Wrapped: err}
	}
	path := tmp.Name()
	if res != nil && res.ReplacementFile != "" {
		path = res.ReplacementFile
		defer os.Remove(res.ReplacementFile)
	}
	src, err := os.Open(path)
	if err != nil {
		return "", nil, err
	}
	defer src.Close()
	sha, err := in.c.media.PutStream(func(w io.Writer) error {
		_, err := io.Copy(w, src)
		return err
	})
	if err != nil {
		return "", nil, err
	}
	return ContentURI(sha), nil, nil
}

func (in *intent) SetDisplayName(ctx context.Context, name string) error {
	if in.ghost == "" {
		return nil
	}
	return in.c.write(ctx, func(tx *inbox.Tx) error {
		contactID, err := tx.EnsureContact(in.c.network, string(in.ghost), string(in.mxid))
		if err != nil {
			return err
		}
		return tx.SetContactName(contactID, name)
	})
}

func (in *intent) SetAvatarURL(ctx context.Context, avatarURL id.ContentURIString) error {
	if in.ghost == "" {
		return nil
	}
	return in.c.write(ctx, func(tx *inbox.Tx) error {
		contactID, err := tx.EnsureContact(in.c.network, string(in.ghost), string(in.mxid))
		if err != nil {
			return err
		}
		return tx.SetContactAvatar(contactID, SHAFromContentURI(avatarURL))
	})
}

func (in *intent) SetExtraProfileMeta(ctx context.Context, data any) error {
	if in.ghost == "" || data == nil {
		return nil
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}
	var extra event.BeeperProfileExtra
	if err = json.Unmarshal(raw, &extra); err != nil {
		return err
	}
	return in.c.write(ctx, func(tx *inbox.Tx) error {
		contactID, err := tx.EnsureContact(in.c.network, string(in.ghost), string(in.mxid))
		if err != nil {
			return err
		}
		return tx.SetContactExtra(contactID, extra.Identifiers, extra.IsNetworkBot)
	})
}

// SetProfile is only used with ReplaceEntireProfile, which this connector doesn't offer.
func (in *intent) SetProfile(ctx context.Context, data any) error { return nil }

func (in *intent) CreateRoom(ctx context.Context, req *mautrix.ReqCreateRoom) (id.RoomID, error) {
	return in.c.createRoom(ctx, req)
}

// DeleteRoom hides the chat but keeps its history: bridgev2 also deletes rooms it replaces.
func (in *intent) DeleteRoom(ctx context.Context, roomID id.RoomID, puppetsOnly bool) error {
	if puppetsOnly {
		return nil
	}
	return in.c.write(ctx, func(tx *inbox.Tx) error {
		return tx.SetChatField(string(roomID), "deleted", 1, "deleted")
	})
}

func (in *intent) EnsureJoined(ctx context.Context, roomID id.RoomID, params ...bridgev2.EnsureJoinedParams) error {
	if in.mxid == in.c.bot.mxid {
		return nil
	}
	members, err := in.c.GetMembers(ctx, roomID)
	if err != nil {
		return err
	}
	if current, ok := members[in.mxid]; ok && current.Membership == event.MembershipJoin {
		return nil
	}
	_, err = in.SendState(ctx, roomID, event.StateMember, string(in.mxid), &event.Content{
		Parsed: &event.MemberEventContent{Membership: event.MembershipJoin},
	}, time.Time{})
	return err
}

func (in *intent) EnsureInvited(ctx context.Context, roomID id.RoomID, userID id.UserID) error {
	if userID == in.c.bot.mxid {
		return nil
	}
	members, err := in.c.GetMembers(ctx, roomID)
	if err != nil {
		return err
	}
	if current, ok := members[userID]; ok && (current.Membership == event.MembershipJoin || current.Membership == event.MembershipInvite) {
		return nil
	}
	_, err = in.SendState(ctx, roomID, event.StateMember, string(userID), &event.Content{
		Parsed: &event.MemberEventContent{Membership: event.MembershipInvite},
	}, time.Time{})
	return err
}

func (in *intent) TagRoom(ctx context.Context, roomID id.RoomID, tag event.RoomTag, isTagged bool) error {
	var column string
	switch tag {
	case event.RoomTagFavourite:
		column = "pinned"
	case event.RoomTagLowPriority:
		column = "archived"
	default:
		return nil
	}
	return in.c.write(ctx, func(tx *inbox.Tx) error {
		return tx.SetChatField(string(roomID), column, boolInt(isTagged), "info")
	})
}

func (in *intent) MuteRoom(ctx context.Context, roomID id.RoomID, until time.Time) error {
	var value any
	if until.Equal(event.MutedForever) {
		value = int64(-1)
	} else if until.After(time.Now()) {
		value = until.UnixMilli()
	}
	return in.c.write(ctx, func(tx *inbox.Tx) error {
		return tx.SetChatField(string(roomID), "muted_until", value, "info")
	})
}

// GetEvent serves an event bridgev2 stored earlier (WhatsApp uses it for quoted replies and caption edits).
func (in *intent) GetEvent(ctx context.Context, roomID id.RoomID, eventID id.EventID) (*event.Event, error) {
	raw, err := in.c.inbox.RawEvent(ctx, string(eventID))
	if errors.Is(err, inbox.ErrNotFound) {
		return nil, mautrix.MNotFound
	} else if err != nil {
		return nil, err
	}
	if raw.RoomID != string(roomID) {
		return nil, mautrix.MNotFound
	}
	evt := &event.Event{
		ID:        id.EventID(raw.EventID),
		RoomID:    id.RoomID(raw.RoomID),
		Sender:    id.UserID(raw.Sender),
		Type:      event.Type{Type: raw.Type, Class: event.MessageEventType},
		Timestamp: raw.TS,
	}
	if err = json.Unmarshal([]byte(raw.Content), &evt.Content); err != nil {
		return nil, fmt.Errorf("stored event content: %w", err)
	}
	if raw.RedactedTS != 0 {
		evt.Content = event.Content{}
		evt.Unsigned.RedactedBecause = &event.Event{Type: event.EventRedaction, Timestamp: raw.RedactedTS, Redacts: evt.ID}
		return evt, nil
	}
	if err = evt.Content.ParseRaw(evt.Type); err != nil && !errors.Is(err, event.ErrUnsupportedContentType) && !errors.Is(err, event.ErrContentAlreadyParsed) {
		return nil, err
	}
	return evt, nil
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func nowMS() int64 { return time.Now().UnixMilli() }

func timestampMS(ts time.Time) int64 {
	if ts.IsZero() {
		return 0
	}
	return ts.UnixMilli()
}
