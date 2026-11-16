package engine

import (
	"context"
	"sync"
	"time"

	"maunium.net/go/mautrix/bridgev2/status"

	"cappy.local/engine/internal/inbox"
)

// ChatDebounce is how long chat.updated notifications for one chat are coalesced.
const ChatDebounce = 250 * time.Millisecond

// notifier turns committed inbox changes into JSON-RPC notifications. message.new and message.updated go out right
// away (after the commit, in commit order); chat.updated is debounced per chat, so it always follows the message
// notifications of the same write.
type notifier struct {
	e *Engine

	mu      sync.Mutex
	pending map[string]string
	timers  map[string]*time.Timer
}

func newNotifier(e *Engine) *notifier {
	return &notifier{e: e, pending: make(map[string]string), timers: make(map[string]*time.Timer)}
}

func (n *notifier) NetworkState(network string, st status.BridgeState) {
	n.e.networkStateChanged(network, st)
}

func (n *notifier) Changed(ctx context.Context, ch *inbox.Changes) {
	if ch == nil {
		return
	}
	ctx = context.WithoutCancel(ctx)
	for _, eventID := range ch.NewMessages {
		msg, err := n.e.inbox.Message(ctx, eventID)
		if err != nil {
			continue
		}
		summary, err := n.e.inbox.ChatSummary(ctx, msg.RoomID)
		if err != nil {
			continue
		}
		n.e.srv.Notify("message.new", map[string]any{"message": msg, "chat": summary})
	}
	for _, upd := range ch.Updated {
		msg, err := n.e.inbox.Message(ctx, upd.EventID)
		if err != nil {
			continue
		}
		n.e.srv.Notify("message.updated", map[string]any{"message": msg, "change": upd.Change})
	}
	for _, accountID := range ch.Accounts {
		n.e.notifyAccount(ctx, accountID)
	}
	for _, roomID := range ch.ChatOrder {
		n.chatChanged(roomID, ch.Chats[roomID])
	}
}

func (n *notifier) chatChanged(roomID, reason string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if prev, ok := n.pending[roomID]; ok {
		if reason == "created" || reason == "deleted" || (prev != "created" && prev != "deleted") {
			n.pending[roomID] = reason
		}
		return
	}
	n.pending[roomID] = reason
	n.timers[roomID] = time.AfterFunc(ChatDebounce, func() { n.send(roomID) })
}

func (n *notifier) send(roomID string) {
	n.mu.Lock()
	reason, ok := n.pending[roomID]
	delete(n.pending, roomID)
	delete(n.timers, roomID)
	n.mu.Unlock()
	if !ok {
		return
	}
	chat, err := n.e.inbox.Chat(context.Background(), roomID)
	if err != nil || chat.Kind == "system" {
		return
	}
	n.e.srv.Notify("chat.updated", map[string]any{"chat": chat, "reason": reason})
}

// flush sends every pending chat.updated now (during shutdown).
func (n *notifier) flush() {
	n.mu.Lock()
	rooms := make([]string, 0, len(n.pending))
	for roomID, t := range n.timers {
		t.Stop()
		rooms = append(rooms, roomID)
	}
	n.mu.Unlock()
	for _, roomID := range rooms {
		n.send(roomID)
	}
}

func (e *Engine) notifyAccount(ctx context.Context, accountID string) {
	acc, err := e.inbox.Account(ctx, accountID)
	if err != nil {
		return
	}
	e.decorateAccount(acc)
	e.srv.NotifyPriority("account.state", map[string]any{"account": acc})
}
