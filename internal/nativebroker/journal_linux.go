//go:build linux

package nativebroker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

func readJournal(ctx context.Context, unit, cursor string, limit int) (JournalPage, error) {
	if !unitPattern.MatchString(unit) || len(cursor) > 4096 || strings.ContainsAny(cursor, "\x00\r\n") || limit < 1 || limit > 100 {
		return JournalPage{}, errors.New("invalid journal request")
	}
	args := []string{"--quiet", "--no-pager", "--output=json", "--output-fields=MESSAGE,__CURSOR,__REALTIME_TIMESTAMP", "--unit=" + unit}
	if cursor != "" {
		args = append(args, "--after-cursor="+cursor, "--no-tail")
	} else {
		args = append(args, "--lines="+strconv.Itoa(limit))
	}
	command := exec.CommandContext(ctx, "/usr/bin/journalctl", args...)
	command.Env = []string{"PATH=/usr/sbin:/usr/bin:/sbin:/bin", "LC_ALL=C"}
	output, err := command.StdoutPipe()
	if err != nil {
		return JournalPage{}, err
	}
	if err := command.Start(); err != nil {
		return JournalPage{}, err
	}
	page, readErr := decodeJournal(io.LimitReader(output, 2<<20), cursor, limit)
	// Stop after a bounded page; --after-cursor must read the first events,
	// not journalctl's last N events (which would silently skip backlog).
	if len(page.Events) > 0 || readErr != nil {
		_ = command.Process.Kill()
	}
	waitErr := command.Wait()
	if ctx.Err() != nil {
		return JournalPage{}, ctx.Err()
	}
	if readErr != nil {
		return JournalPage{}, errors.New("cannot decode worker journal")
	}
	if len(page.Events) == 0 && waitErr != nil {
		return JournalPage{}, errors.New("cannot read worker journal")
	}
	return page, nil
}

func decodeJournal(reader io.Reader, cursor string, limit int) (JournalPage, error) {
	page := JournalPage{Events: []JournalEvent{}, NextCursor: cursor}
	decoder := json.NewDecoder(reader)
	bytesUsed := 0
	for len(page.Events) < limit {
		var entry struct {
			Message   json.RawMessage `json:"MESSAGE"`
			Cursor    string          `json:"__CURSOR"`
			Timestamp string          `json:"__REALTIME_TIMESTAMP"`
		}
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				return page, nil
			}
			return page, err
		}
		var message string
		if err := json.Unmarshal(entry.Message, &message); err != nil {
			var raw []byte
			if err := json.Unmarshal(entry.Message, &raw); err != nil {
				return page, errors.New("invalid journal message")
			}
			message = string(raw)
		}
		microseconds, err := strconv.ParseInt(entry.Timestamp, 10, 64)
		if err != nil || entry.Cursor == "" || len(entry.Cursor) > 4096 {
			return page, errors.New("invalid journal metadata")
		}
		if len(message) > 8192 {
			message = strings.ToValidUTF8(message[:8192], "�") + " [truncated]"
		}
		// JSON escaping can expand each byte up to sixfold. Bound the full packet,
		// including the opaque cursor, without consuming the next event.
		encoded, _ := json.Marshal(JournalEvent{Message: message, Timestamp: time.UnixMicro(microseconds).UTC()})
		if bytesUsed+len(encoded)+len(entry.Cursor)+8192 > MaxMessage {
			return page, nil
		}
		bytesUsed += len(encoded)
		page.Events = append(page.Events, JournalEvent{Message: message, Timestamp: time.UnixMicro(microseconds).UTC()})
		page.NextCursor = entry.Cursor
	}
	return page, nil
}
