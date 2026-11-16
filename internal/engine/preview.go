package engine

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"image"
	"image/color"
	"image/jpeg"
	"io"
	"strings"

	// Decoders for the photo formats WhatsApp and Telegram deliver.
	_ "image/gif"
	_ "image/png"

	"golang.org/x/image/draw"
	_ "golang.org/x/image/webp"

	"cappy.local/engine/internal/inbox"
	"cappy.local/engine/internal/media"
	"cappy.local/engine/internal/rpc"
)

const (
	// CodeMediaUnavailable: the message has no stored photo, or it cannot be shown.
	CodeMediaUnavailable = 1014

	previewMaxEdge   = 1600
	previewMaxPixels = 40_000_000 // refuse larger images before decoding them
	previewMaxInput  = 50 << 20   // the store's attachment cap
	previewQuality   = 85
)

type mediaPreviewParams struct {
	Chat    string `json:"chat"`
	Message string `json:"message"`
}

type photoPreview struct {
	Mime   string `json:"mime"`
	Width  int    `json:"width"`
	Height int    `json:"height"`
	Data   string `json:"data"`
}

// handleMediaPreview returns a stored photo as a freshly encoded JPEG: decoded with Go's
// memory-safe decoders, scaled to fit 1600 px and re-encoded, so the app never renders the
// sender's original file and its metadata (such as location) is dropped.
func (e *Engine) handleMediaPreview(ctx context.Context, params json.RawMessage) (any, error) {
	var p mediaPreviewParams
	if err := rpc.Decode(params, &p); err != nil {
		return nil, err
	}
	row, rerr := e.chatRoom(ctx, p.Chat)
	if rerr != nil {
		return nil, rerr
	}
	eventID := inbox.EventFromMessageID(p.Message)
	msg, err := e.inbox.Message(ctx, eventID)
	if eventID == "" || err != nil || msg.RoomID != row.RoomID {
		return nil, rpc.InvalidParams("message is not in this chat")
	}
	unavailable := rpc.NewError(CodeMediaUnavailable, "media_unavailable", "this message has no photo stored on this computer")
	att := msg.Attachment
	if msg.Deleted || att == nil || att.File == nil || (att.Msgtype != "m.image" && !att.IsSticker) {
		return nil, unavailable
	}
	sha := strings.TrimPrefix(att.File.Media, "sha256:")
	if !media.ValidSHA(sha) {
		return nil, unavailable
	}
	f, err := e.media.Open(sha)
	if err != nil {
		return nil, unavailable
	}
	defer f.Close()
	out, err := previewJPEG(io.LimitReader(f, previewMaxInput+1))
	if err != nil {
		return nil, unavailable
	}
	return out, nil
}

// previewJPEG decodes a GIF (first frame), JPEG, PNG or still WebP image and re-encodes it as a JPEG
// no larger than previewMaxEdge on its long side. Transparent areas become white.
func previewJPEG(r io.Reader) (*photoPreview, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if len(data) > previewMaxInput {
		return nil, errors.New("image too large")
	}
	cfg, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	if cfg.Width <= 0 || cfg.Height <= 0 || cfg.Width*cfg.Height > previewMaxPixels {
		return nil, errors.New("image dimensions not supported")
	}
	src, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	b := src.Bounds()
	w, h := b.Dx(), b.Dy()
	if w > previewMaxEdge || h > previewMaxEdge {
		if w >= h {
			h, w = max(1, h*previewMaxEdge/w), previewMaxEdge
		} else {
			w, h = max(1, w*previewMaxEdge/h), previewMaxEdge
		}
	}
	dst := image.NewRGBA(image.Rect(0, 0, w, h))
	draw.Draw(dst, dst.Bounds(), &image.Uniform{C: color.White}, image.Point{}, draw.Src)
	draw.CatmullRom.Scale(dst, dst.Bounds(), src, b, draw.Over, nil)
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, dst, &jpeg.Options{Quality: previewQuality}); err != nil {
		return nil, err
	}
	return &photoPreview{Mime: "image/jpeg", Width: w, Height: h, Data: base64.StdEncoding.EncodeToString(buf.Bytes())}, nil
}
