package inbox

import (
	"encoding/json"

	"cappy.local/engine/internal/media"
)

// The types in this file are the JSON shapes the host sees (DESIGN.md section 13). Fields are camelCase; IDs are the
// host-facing forms (acc_, con_, cht_, msg_), never Matrix IDs.

type Media struct {
	Media string `json:"media"`
	Path  string `json:"path"`
	Mime  string `json:"mime,omitempty"`
	Size  int64  `json:"size,omitempty"`
}

func mediaRef(sha, mime string, size int64) *Media {
	if !media.ValidSHA(sha) {
		return nil
	}
	return &Media{Media: "sha256:" + sha, Path: media.RelPath(sha), Mime: mime, Size: size}
}

type Account struct {
	Account    string  `json:"account"`
	Network    string  `json:"network"`
	Name       *string `json:"name"`
	LoggedIn   bool    `json:"loggedIn"`
	State      string  `json:"state"`
	Error      *string `json:"error"`
	Message    *string `json:"message"`
	UserAction *string `json:"userAction"`
	StateTs    *int64  `json:"stateTs"`
	CreatedAt  int64   `json:"createdAt"`

	LoginID     string `json:"-"`
	LoggedOutAt *int64 `json:"-"`
}

type Contact struct {
	Contact string  `json:"contact"`
	Network string  `json:"network"`
	Name    *string `json:"name"`
	Avatar  *Media  `json:"avatar"`
	IsBot   bool    `json:"isBot"`
}

type LastMessage struct {
	Message string  `json:"message"`
	FromMe  bool    `json:"fromMe"`
	Sender  *string `json:"sender"`
	Ts      int64   `json:"ts"`
	Kind    string  `json:"kind"`
	Msgtype *string `json:"msgtype"`
	Preview string  `json:"preview"`
}

type Chat struct {
	Chat           string       `json:"chat"`
	Account        *string      `json:"account"`
	Network        string       `json:"network"`
	Kind           string       `json:"kind"`
	Title          *string      `json:"title"`
	Topic          *string      `json:"topic,omitempty"`
	Avatar         *Media       `json:"avatar"`
	DMContact      *Contact     `json:"dmContact"`
	MessageRequest bool         `json:"messageRequest"`
	LastActivity   *int64       `json:"lastActivity"`
	LastMessage    *LastMessage `json:"lastMessage"`
	Unread         int          `json:"unread"`
	MarkedUnread   bool         `json:"markedUnread"`
	MutedUntil     *int64       `json:"mutedUntil"`
	Pinned         bool         `json:"pinned"`
	Archived       bool         `json:"archived"`
	Deleted        bool         `json:"deleted"`

	RoomID string `json:"-"`
}

// ChatSummary is the small chat part of a message.new notification.
type ChatSummary struct {
	Chat         string `json:"chat"`
	Unread       int    `json:"unread"`
	LastActivity *int64 `json:"lastActivity"`
}

type Attachment struct {
	Msgtype    string  `json:"msgtype"`
	Name       string  `json:"name"`
	Caption    *string `json:"caption"`
	Width      *int    `json:"width"`
	Height     *int    `json:"height"`
	DurationMs *int    `json:"durationMs"`
	IsVoice    bool    `json:"isVoice"`
	IsSticker  bool    `json:"isSticker"`
	Blurhash   *string `json:"blurhash"`
	File       *Media  `json:"file"`
}

type Reaction struct {
	Key    string `json:"key"`
	Count  int    `json:"count"`
	FromMe bool   `json:"fromMe"`
}

type Message struct {
	Message     string          `json:"message"`
	Chat        string          `json:"chat"`
	Account     *string         `json:"account"`
	Sender      *Contact        `json:"sender"`
	FromMe      bool            `json:"fromMe"`
	Ts          int64           `json:"ts"`
	EditedTs    *int64          `json:"editedTs"`
	Deleted     bool            `json:"deleted"`
	Kind        string          `json:"kind"`
	Msgtype     *string         `json:"msgtype"`
	Text        *string         `json:"text"`
	HTML        *string         `json:"html"`
	Update      json.RawMessage `json:"update"`
	ReplyTo     *string         `json:"replyTo"`
	ThreadRoot  *string         `json:"threadRoot"`
	Attachment  *Attachment     `json:"attachment"`
	Reactions   []Reaction      `json:"reactions"`
	Status      *string         `json:"status"`
	StatusError *string         `json:"statusError"`

	EventID string `json:"-"`
	RoomID  string `json:"-"`
	rowID   int64
}

// StoredAttachment is how an attachment is kept in messages.attachment (JSON).
type StoredAttachment struct {
	MsgType    string `json:"msgtype"`
	Name       string `json:"name,omitempty"`
	Caption    string `json:"caption,omitempty"`
	Mime       string `json:"mime,omitempty"`
	Size       int64  `json:"size,omitempty"`
	Width      int    `json:"width,omitempty"`
	Height     int    `json:"height,omitempty"`
	DurationMS int    `json:"duration_ms,omitempty"`
	IsVoice    bool   `json:"is_voice,omitempty"`
	IsSticker  bool   `json:"is_sticker,omitempty"`
	Blurhash   string `json:"blurhash,omitempty"`
	SHA256     string `json:"sha256,omitempty"`
}

func (a *StoredAttachment) view() *Attachment {
	if a == nil {
		return nil
	}
	v := &Attachment{
		Msgtype:   a.MsgType,
		Name:      a.Name,
		IsVoice:   a.IsVoice,
		IsSticker: a.IsSticker,
		File:      mediaRef(a.SHA256, a.Mime, a.Size),
	}
	if a.Caption != "" {
		v.Caption = &a.Caption
	}
	if a.Width > 0 {
		v.Width = &a.Width
	}
	if a.Height > 0 {
		v.Height = &a.Height
	}
	if a.DurationMS > 0 {
		v.DurationMs = &a.DurationMS
	}
	if a.Blurhash != "" {
		v.Blurhash = &a.Blurhash
	}
	return v
}

type SearchResult struct {
	Message Message `json:"message"`
	Snippet string  `json:"snippet"`
}
