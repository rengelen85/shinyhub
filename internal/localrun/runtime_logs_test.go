package localrun

import (
	"bytes"
	"strings"
	"testing"
)

func TestRuntimeLogsHideStartupAddressesAndPreserveDiagnostics(t *testing.T) {
	for _, observed := range []bool{false, true} {
		for _, tc := range []struct {
			name, input string
			hidden      bool
		}{
			{"R Shiny", "Listening on http://127.0.0.1:20001\n", true},
			{"uvicorn", "INFO:     Uvicorn running on http://127.0.0.1:20001 (Press CTRL+C to quit)\n", true},
			{"colored uvicorn", "\x1b[32mINFO\x1b[0m: Uvicorn running on \x1b[1mhttp://127.0.0.1:20001\x1b[0m (Press CTRL+C to quit)\r\n", true},
			{"Dash", "Dash is running on http://127.0.0.1:20001/\n", true},
			{"Werkzeug", " * Running on http://127.0.0.1:20001\n", true},
			{"Streamlit local", "  Local URL: http://localhost:20001\n", true},
			{"Streamlit network", "  Network URL: http://192.168.1.10:20001\n", true},
			{"Streamlit headless", "  URL: http://127.0.0.1:20001\n", true},
			{"partial final announcement", "Listening on http://127.0.0.1:20001", true},
			{"other port", "Listening on http://127.0.0.1:20002\n", false},
			{"port prefix", "Listening on http://127.0.0.1:200010\n", false},
			{"startup error", "ERROR: Failed to listen on http://127.0.0.1:20001\n", false},
			{"URL in diagnostic", "Could not fetch http://127.0.0.1:20001/api\n", false},
			{"request log", "127.0.0.1 - GET /index.html HTTP/1.1 200\n", false},
			{"ordinary partial line", "app initialized", false},
			{"long line", strings.Repeat("x", 20000) + "Listening on http://127.0.0.1:20001\n", false},
		} {
			mode := "plain/"
			if observed {
				mode = "events/"
			}
			t.Run(mode+tc.name, func(t *testing.T) {
				var output bytes.Buffer
				var events []Event
				observer := &runEvents{}
				if observed {
					observer.callback = func(e Event) { events = append(events, e) }
				}
				w := &runtimeLogWriter{out: observer.writer(&output, "app", "stderr", 1), port: 20001}
				// Exercise fragmented subprocess writes, including split URLs/ANSI.
				for i := range len(tc.input) {
					if _, err := w.Write([]byte(tc.input[i : i+1])); err != nil {
						t.Fatal(err)
					}
				}
				flushEventWriter(w)
				want := tc.input
				if tc.hidden {
					want = ""
				}
				if output.String() != want {
					t.Fatalf("output = %q, want %q", output.String(), want)
				}
				if observed && tc.hidden && len(events) != 0 {
					t.Fatalf("startup address leaked to TUI/NDJSON: %+v", events)
				}
				if observed && !tc.hidden && len(events) == 0 {
					t.Fatal("diagnostic missing from TUI/NDJSON")
				}
			})
		}
	}
}
