package inbox

import (
	"crypto/rand"
	"encoding/base32"
	"encoding/binary"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ServerName is the made-up homeserver every Matrix-style ID lives on. Nothing ever resolves it.
const ServerName = "cappy.local"

// B32 is unpadded upper-case RFC 4648 base32 (A-Z, 2-7).
var B32 = base32.StdEncoding.WithPadding(base32.NoPadding)

var plainIDPart = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// RandomB32 returns n random base32 characters.
func RandomB32(n int) string {
	buf := make([]byte, (n*5+7)/8)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	return B32.EncodeToString(buf)[:n]
}

// NewULID returns a 26-character, time-sortable base32 ID (48-bit millisecond time + 80 random bits).
func NewULID() string {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[:8], uint64(time.Now().UnixMilli())<<16)
	if _, err := rand.Read(buf[6:]); err != nil {
		panic(err)
	}
	return B32.EncodeToString(buf[:])[:26]
}

// ChatIDFromRoom maps "!ABC:cappy.local" to "cht_ABC". It is a pure string map.
func ChatIDFromRoom(roomID string) string {
	local, ok := strings.CutPrefix(roomID, "!")
	if !ok {
		return ""
	}
	local, ok = strings.CutSuffix(local, ":"+ServerName)
	if !ok {
		return ""
	}
	return "cht_" + local
}

// RoomFromChatID is the inverse of ChatIDFromRoom. It returns "" for a malformed ID.
func RoomFromChatID(chatID string) string {
	local, ok := strings.CutPrefix(chatID, "cht_")
	if !ok || !plainIDPart.MatchString(local) {
		return ""
	}
	return "!" + local + ":" + ServerName
}

// MessageIDFromEvent maps "$mXYZ" to "msg_mXYZ".
func MessageIDFromEvent(eventID string) string {
	local, ok := strings.CutPrefix(eventID, "$")
	if !ok || local == "" {
		return ""
	}
	return "msg_" + local
}

// EventFromMessageID is the inverse of MessageIDFromEvent. It returns "" for a malformed ID.
func EventFromMessageID(messageID string) string {
	local, ok := strings.CutPrefix(messageID, "msg_")
	if !ok || !plainIDPart.MatchString(local) {
		return ""
	}
	return "$" + local
}

// ContactIDString formats a contact row ID for the host.
func ContactIDString(id int64) string {
	return "con_" + strings.ToUpper(strconv.FormatInt(id, 32))
}

// NewAccountID returns "acc_" + 16 random base32 characters.
func NewAccountID() string {
	return "acc_" + RandomB32(16)
}
