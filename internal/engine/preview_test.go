package engine

import (
	"bytes"
	"encoding/base64"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"testing"
)

func encodePNG(t *testing.T, w, h int, fill color.Color) []byte {
	t.Helper()
	img := image.NewNRGBA(image.Rect(0, 0, w, h))
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			img.Set(x, y, fill)
		}
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestPreviewScalesAndReencodes(t *testing.T) {
	out, err := previewJPEG(bytes.NewReader(encodePNG(t, 3200, 800, color.NRGBA{R: 200, A: 255})))
	if err != nil {
		t.Fatal(err)
	}
	if out.Mime != "image/jpeg" || out.Width != 1600 || out.Height != 400 {
		t.Fatalf("unexpected preview: %s %dx%d", out.Mime, out.Width, out.Height)
	}
	raw, err := base64.StdEncoding.DecodeString(out.Data)
	if err != nil {
		t.Fatal(err)
	}
	img, err := jpeg.Decode(bytes.NewReader(raw))
	if err != nil || img.Bounds().Dx() != 1600 {
		t.Fatalf("the preview is not a 1600 px JPEG: %v", err)
	}
}

func TestPreviewKeepsSmallImagesAndFlattensTransparency(t *testing.T) {
	out, err := previewJPEG(bytes.NewReader(encodePNG(t, 40, 60, color.NRGBA{})))
	if err != nil {
		t.Fatal(err)
	}
	if out.Width != 40 || out.Height != 60 {
		t.Fatalf("small image was resized: %dx%d", out.Width, out.Height)
	}
	raw, _ := base64.StdEncoding.DecodeString(out.Data)
	img, _ := jpeg.Decode(bytes.NewReader(raw))
	if r, g, b, _ := img.At(20, 30).RGBA(); r < 0xf000 || g < 0xf000 || b < 0xf000 {
		t.Fatal("transparent pixels should become white")
	}
}

func TestPreviewRefusesBadInput(t *testing.T) {
	// A tiny PNG header that claims 100000 x 100000 pixels is refused before decoding.
	header := encodePNG(t, 1, 1, color.White)
	bomb := append([]byte(nil), header...)
	// IHDR width and height are bytes 16..23.
	copy(bomb[16:24], []byte{0, 1, 0x86, 0xa0, 0, 1, 0x86, 0xa0})
	for name, data := range map[string][]byte{"not an image": []byte("hello"), "too many pixels": bomb, "empty": nil} {
		if _, err := previewJPEG(bytes.NewReader(data)); err == nil {
			t.Fatalf("%s: expected an error", name)
		}
	}
}
