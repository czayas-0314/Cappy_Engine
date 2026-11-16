// Copyright (C) 2026 Cappy Comm contributors
// Portions adapted from cloud-in-a-bottle/bottled-bridge-core (bridgetest/network.go, commit e801a224), AGPL-3.0.
//
// This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General
// Public License as published by the Free Software Foundation, version 3 of the License (AGPL-3.0-only). See LICENSE.

// Package fakenet is an in-process bridgev2 network connector for tests. It never touches a real network: tests feed
// remote events to the real bridgev2 portal logic, which talks to the real local Matrix connector and inbox. The owner
// is "me"; "alice" and "bob" are other people.
package fakenet

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.mau.fi/util/configupgrade"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"
)

const LoginID = networkid.UserLoginID("me")

type Network struct {
	br *bridgev2.Bridge

	mu       sync.Mutex
	sent     []*bridgev2.MatrixMessage
	failSend bool
	receipts []*bridgev2.MatrixReadReceipt
	history  map[networkid.PortalID][]*bridgev2.BackfillMessage
}

func New() *Network {
	return &Network{history: map[networkid.PortalID][]*bridgev2.BackfillMessage{}}
}

// SetHistory sets the messages a chat brings when it first appears (forward backfill).
func (n *Network) SetHistory(portal networkid.PortalID, msgs []*bridgev2.BackfillMessage) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.history[portal] = msgs
}

func (n *Network) SetFailSend(fail bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.failSend = fail
}

// Sent is what the bridge handed to the network to send.
func (n *Network) Sent() []*bridgev2.MatrixMessage {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]*bridgev2.MatrixMessage(nil), n.sent...)
}

// Receipts are the read receipts the bridge handed to the network.
func (n *Network) Receipts() []*bridgev2.MatrixReadReceipt {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]*bridgev2.MatrixReadReceipt(nil), n.receipts...)
}

var _ bridgev2.NetworkConnector = (*Network)(nil)

func (n *Network) Init(br *bridgev2.Bridge)         { n.br = br }
func (n *Network) Start(context.Context) error      { return nil }
func (n *Network) GetBridgeInfoVersion() (int, int) { return 1, 1 }
func (n *Network) GetName() bridgev2.BridgeName {
	return bridgev2.BridgeName{DisplayName: "Fake", NetworkID: "fake", BeeperBridgeType: "fake"}
}
func (n *Network) GetDBMetaTypes() database.MetaTypes { return database.MetaTypes{} }
func (n *Network) GetCapabilities() *bridgev2.NetworkGeneralCapabilities {
	return &bridgev2.NetworkGeneralCapabilities{}
}
func (n *Network) GetConfig() (string, any, configupgrade.Upgrader) { return "", nil, nil }

func (n *Network) GetLoginFlows() []bridgev2.LoginFlow {
	return []bridgev2.LoginFlow{
		{ID: "instant", Name: "Instant", Description: "Logs in as \"me\" immediately"},
		{ID: "qr", Name: "QR", Description: "Shows a fake QR code that nobody scans"},
	}
}

func (n *Network) CreateLogin(_ context.Context, user *bridgev2.User, flowID string) (bridgev2.LoginProcess, error) {
	switch flowID {
	case "instant":
		return &instantLogin{user: user}, nil
	case "qr":
		return &qrLogin{cancelled: make(chan struct{})}, nil
	}
	return nil, fmt.Errorf("no such flow")
}

type instantLogin struct {
	user *bridgev2.User
}

func (l *instantLogin) Start(ctx context.Context) (*bridgev2.LoginStep, error) {
	ul, err := l.user.NewLogin(ctx, &database.UserLogin{ID: LoginID, RemoteName: "Me Myself"}, nil)
	if err != nil {
		return nil, err
	}
	ul.Client.Connect(ul.Log.WithContext(context.Background()))
	return &bridgev2.LoginStep{
		Type: bridgev2.LoginStepTypeComplete, StepID: "complete",
		CompleteParams: &bridgev2.LoginCompleteParams{UserLoginID: ul.ID, UserLogin: ul},
	}, nil
}

func (l *instantLogin) Cancel() {}

// qrLogin shows a code and waits until it is cancelled, like a QR code nobody scans.
type qrLogin struct {
	once      sync.Once
	cancelled chan struct{}
}

func (l *qrLogin) Start(context.Context) (*bridgev2.LoginStep, error) {
	return &bridgev2.LoginStep{
		Type: bridgev2.LoginStepTypeDisplayAndWait, StepID: "qr", Instructions: "Scan the code",
		DisplayAndWaitParams: &bridgev2.LoginDisplayAndWaitParams{Type: bridgev2.LoginDisplayTypeQR, Data: "fake,qr,data"},
	}, nil
}

func (l *qrLogin) Wait(ctx context.Context) (*bridgev2.LoginStep, error) {
	select {
	case <-l.cancelled:
		return nil, errors.New("login cancelled")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (l *qrLogin) Cancel() { l.once.Do(func() { close(l.cancelled) }) }

func (n *Network) LoadUserLogin(_ context.Context, login *bridgev2.UserLogin) error {
	login.Client = &client{n: n, login: login}
	return nil
}

type client struct {
	n     *Network
	login *bridgev2.UserLogin
}

var (
	_ bridgev2.NetworkAPI                    = (*client)(nil)
	_ bridgev2.BackfillingNetworkAPI         = (*client)(nil)
	_ bridgev2.ReadReceiptHandlingNetworkAPI = (*client)(nil)
)

func (c *client) HandleMatrixReadReceipt(_ context.Context, receipt *bridgev2.MatrixReadReceipt) error {
	c.n.mu.Lock()
	defer c.n.mu.Unlock()
	c.n.receipts = append(c.n.receipts, receipt)
	return nil
}

func (c *client) Connect(ctx context.Context) {
	c.login.BridgeState.Send(statusConnected())
}
func (c *client) Disconnect()                  {}
func (c *client) IsLoggedIn() bool             { return true }
func (c *client) LogoutRemote(context.Context) {}
func (c *client) IsThisUser(_ context.Context, userID networkid.UserID) bool {
	return userID == "me"
}

func members(ids ...networkid.UserID) *bridgev2.ChatMemberList {
	list := &bridgev2.ChatMemberList{IsFull: true, MemberMap: bridgev2.ChatMemberMap{}}
	for _, userID := range ids {
		list.MemberMap.Set(bridgev2.ChatMember{EventSender: bridgev2.EventSender{Sender: userID, IsFromMe: userID == "me"}})
	}
	return list
}

func (c *client) GetChatInfo(_ context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
	if portal.ID == GroupKey().ID {
		name := "Synthetic group"
		roomType := database.RoomTypeDefault
		return &bridgev2.ChatInfo{Name: &name, Type: &roomType, Members: members("me", "alice", "bob"), CanBackfill: true}, nil
	}
	roomType := database.RoomTypeDM
	info := &bridgev2.ChatInfo{Type: &roomType, Members: members("me", "alice"), CanBackfill: true}
	info.Members.OtherUserID = "alice"
	return info, nil
}

func (c *client) GetUserInfo(_ context.Context, ghost *bridgev2.Ghost) (*bridgev2.UserInfo, error) {
	names := map[networkid.UserID]string{"me": "Me Myself", "alice": "Alice Synthetic", "bob": "Bob Synthetic"}
	name := names[ghost.ID]
	return &bridgev2.UserInfo{Name: &name}, nil
}

func (c *client) GetCapabilities(context.Context, *bridgev2.Portal) *event.RoomFeatures {
	return &event.RoomFeatures{Reply: event.CapLevelFullySupported, Edit: event.CapLevelFullySupported,
		Delete: event.CapLevelFullySupported, Reaction: event.CapLevelFullySupported}
}

func (c *client) HandleMatrixMessage(_ context.Context, msg *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error) {
	c.n.mu.Lock()
	defer c.n.mu.Unlock()
	if c.n.failSend {
		return nil, errors.New("the fake network refused the message")
	}
	c.n.sent = append(c.n.sent, msg)
	return &bridgev2.MatrixMessageResponse{DB: &database.Message{
		ID: networkid.MessageID(fmt.Sprintf("sent-%d", len(c.n.sent))), SenderID: "me", Timestamp: time.Now(),
	}}, nil
}

func (c *client) FetchMessages(_ context.Context, params bridgev2.FetchMessagesParams) (*bridgev2.FetchMessagesResponse, error) {
	c.n.mu.Lock()
	defer c.n.mu.Unlock()
	if params.Forward {
		return &bridgev2.FetchMessagesResponse{Messages: c.n.history[params.Portal.ID], Forward: true}, nil
	}
	return &bridgev2.FetchMessagesResponse{HasMore: false}, nil
}

// ---- helpers for building remote events ----

func DMKey() networkid.PortalKey    { return networkid.PortalKey{ID: "alice", Receiver: LoginID} }
func GroupKey() networkid.PortalKey { return networkid.PortalKey{ID: "group", Receiver: LoginID} }

func Sender(userID networkid.UserID) bridgev2.EventSender {
	return bridgev2.EventSender{Sender: userID, IsFromMe: userID == "me"}
}

func TextMessage(key networkid.PortalKey, sender networkid.UserID, msgID networkid.MessageID, body string, ts time.Time) *simplevent.Message[string] {
	return &simplevent.Message[string]{
		EventMeta: simplevent.EventMeta{
			Type: bridgev2.RemoteEventMessage, PortalKey: key, Sender: Sender(sender), CreatePortal: true, Timestamp: ts,
		},
		ID:   msgID,
		Data: body,
		ConvertMessageFunc: func(_ context.Context, _ *bridgev2.Portal, _ bridgev2.MatrixAPI, data string) (*bridgev2.ConvertedMessage, error) {
			return &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{{
				Type: event.EventMessage, Content: &event.MessageEventContent{MsgType: event.MsgText, Body: data},
			}}}, nil
		},
	}
}

func Edit(key networkid.PortalKey, sender networkid.UserID, target networkid.MessageID, body string, ts time.Time) *simplevent.Message[string] {
	return &simplevent.Message[string]{
		EventMeta: simplevent.EventMeta{
			Type: bridgev2.RemoteEventEdit, PortalKey: key, Sender: Sender(sender), Timestamp: ts,
		},
		TargetMessage: target,
		Data:          body,
		ConvertEditFunc: func(_ context.Context, _ *bridgev2.Portal, _ bridgev2.MatrixAPI, existing []*database.Message, data string) (*bridgev2.ConvertedEdit, error) {
			return &bridgev2.ConvertedEdit{ModifiedParts: []*bridgev2.ConvertedEditPart{{
				Part: existing[0], Type: event.EventMessage,
				Content: &event.MessageEventContent{MsgType: event.MsgText, Body: data},
			}}}, nil
		},
	}
}

func Reaction(key networkid.PortalKey, sender networkid.UserID, target networkid.MessageID, emoji string) *simplevent.Reaction {
	return &simplevent.Reaction{
		EventMeta:     simplevent.EventMeta{Type: bridgev2.RemoteEventReaction, PortalKey: key, Sender: Sender(sender), Timestamp: time.Now()},
		TargetMessage: target,
		Emoji:         emoji,
	}
}

func Remove(key networkid.PortalKey, sender networkid.UserID, target networkid.MessageID) *simplevent.MessageRemove {
	return &simplevent.MessageRemove{
		EventMeta:     simplevent.EventMeta{Type: bridgev2.RemoteEventMessageRemove, PortalKey: key, Sender: Sender(sender), Timestamp: time.Now()},
		TargetMessage: target,
	}
}

func BackfillText(sender networkid.UserID, msgID networkid.MessageID, body string, ts time.Time) *bridgev2.BackfillMessage {
	return &bridgev2.BackfillMessage{
		ConvertedMessage: &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{{
			Type: event.EventMessage, Content: &event.MessageEventContent{MsgType: event.MsgText, Body: body},
		}}},
		Sender: Sender(sender), ID: msgID, Timestamp: ts,
	}
}

func ChatResync(key networkid.PortalKey) *simplevent.ChatResync {
	return &simplevent.ChatResync{
		EventMeta: simplevent.EventMeta{Type: bridgev2.RemoteEventChatResync, PortalKey: key, CreatePortal: true},
	}
}

func statusConnected() status.BridgeState {
	return status.BridgeState{StateEvent: status.StateConnected}
}
