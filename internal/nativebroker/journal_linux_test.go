//go:build linux

package nativebroker

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestJournalPagesKeepFirstUnreadCursorAndBoundPackets(t *testing.T) {
	var input strings.Builder
	for i := 0; i < 100; i++ {
		b, _ := json.Marshal(map[string]any{"MESSAGE": strings.Repeat("\x00", 9000), "__CURSOR": string(rune('a' + i)), "__REALTIME_TIMESTAMP": "1000000"})
		input.Write(b)
		input.WriteByte('\n')
	}
	page, err := decodeJournal(strings.NewReader(input.String()), "before", 100)
	if err != nil || len(page.Events) == 0 || len(page.Events) >= 100 {
		t.Fatalf("bounded page=%+v err=%v", page, err)
	}
	encoded, _ := json.Marshal(Response{Journal: &page})
	if len(encoded) > MaxMessage {
		t.Fatalf("packet=%d exceeds %d", len(encoded), MaxMessage)
	}
	if page.NextCursor != string(rune('a'+len(page.Events)-1)) {
		t.Fatal("cursor skipped an unread event")
	}
	if !strings.HasSuffix(page.Events[0].Message, " [truncated]") {
		t.Fatal("oversized message not truncated")
	}
}

func TestJournalDecodeBinaryMessagesAndEmptyCursor(t *testing.T) {
	page, err := decodeJournal(strings.NewReader(`{"MESSAGE":[65,66],"__CURSOR":"next","__REALTIME_TIMESTAMP":"1000000"}`), "old", 1)
	if err != nil || len(page.Events) != 1 || page.Events[0].Message != "AB" || page.Events[0].Timestamp.Unix() != 1 {
		t.Fatalf("page=%+v err=%v", page, err)
	}
	page, err = decodeJournal(strings.NewReader(""), "old", 1)
	if err != nil || page.NextCursor != "old" {
		t.Fatalf("empty=%+v err=%v", page, err)
	}
	for _, bad := range []string{`{"MESSAGE":"x","__CURSOR":"","__REALTIME_TIMESTAMP":"1"}`, `{"MESSAGE":{},"__CURSOR":"x","__REALTIME_TIMESTAMP":"1"}`, `{"MESSAGE":"x","__CURSOR":"x","__REALTIME_TIMESTAMP":"bad"}`} {
		if _, err := decodeJournal(strings.NewReader(bad), "", 10); err == nil {
			t.Fatal("invalid journal accepted")
		}
	}
}
