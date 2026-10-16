package localrun

import (
	"io"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/charmbracelet/x/ansi"
)

// Suppress only runtime startup address announcements for this child. Errors,
// request logs, and app messages containing URLs remain useful diagnostics.
var runtimeAddressAnnouncement = regexp.MustCompile(`(?i)^(?:INFO:\s*)?(?:\*\s*)?(?:Listening on|Uvicorn running on|Running on|Dash is running on|Local URL:|Network URL:|External URL:|URL:)\s+(https?://\S+?)(?:\s+\(Press CTRL\+C to quit\))?\s*$`)

type runtimeLogWriter struct {
	mu          sync.Mutex
	out         io.Writer
	port        int
	fragment    string
	passthrough bool // an overlong line must be preserved in full
}

func (w *runtimeLogWriter) suppress(line string) bool {
	match := runtimeAddressAnnouncement.FindStringSubmatch(strings.TrimSpace(ansi.Strip(line)))
	if match == nil {
		return false
	}
	u, err := url.Parse(match[1])
	return err == nil && u.Port() == strconv.Itoa(w.port)
}

func (w *runtimeLogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.fragment += string(p)
	for len(w.fragment) > 0 {
		end := strings.IndexByte(w.fragment, '\n')
		complete := end >= 0 && end < 8192
		if !complete {
			if len(w.fragment) < 8192 {
				break
			}
			end = 8191
		}
		line := w.fragment[:end+1]
		w.fragment = w.fragment[end+1:]
		if !complete || w.passthrough || !w.suppress(line) {
			if n, err := io.WriteString(w.out, line); err != nil {
				return 0, err
			} else if n != len(line) {
				return 0, io.ErrShortWrite
			}
		}
		w.passthrough = !complete
	}
	return len(p), nil
}

func (w *runtimeLogWriter) flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.fragment != "" && (w.passthrough || !w.suppress(w.fragment)) {
		_, _ = io.WriteString(w.out, w.fragment)
	}
	w.fragment = ""
	flushEventWriter(w.out)
}
