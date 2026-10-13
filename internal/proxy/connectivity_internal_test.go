package proxy

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// seedFailedWSAt back-dates a slug's first failed upgrade so the wsWarnGrace window is
// already exceeded without the test sleeping.
func seedFailedWSAt(p *Proxy, slug string, ago time.Duration) {
	p.seenMu.Lock()
	p.firstFailedWSAt[slug] = time.Now().Add(-ago)
	p.seenMu.Unlock()
}

func TestConnectivityHealth_States(t *testing.T) {
	p := New()

	// Never served: nothing to report.
	if ever, serving := p.ConnectivityHealth("app"); ever || serving {
		t.Fatalf("never-served: got (ever=%v, serving=%v), want (false,false)", ever, serving)
	}

	// Served, still inside the grace window: not yet a warning.
	p.RecordActivity("app")
	if ever, serving := p.ConnectivityHealth("app"); ever || serving {
		t.Fatalf("within grace: got (ever=%v, serving=%v), want (false,false)", ever, serving)
	}

	// A failed upgrade beyond the grace window without a success: warn.
	seedFailedWSAt(p, "app", 2*wsWarnGrace)
	if ever, serving := p.ConnectivityHealth("app"); ever || !serving {
		t.Fatalf("past grace, no WS: got (ever=%v, serving=%v), want (false,true)", ever, serving)
	}

	// A completed WebSocket clears the warning and reports healthy.
	p.MarkWSReady("app")
	if ever, serving := p.ConnectivityHealth("app"); !ever || serving {
		t.Fatalf("after WS: got (ever=%v, serving=%v), want (true,false)", ever, serving)
	}
}

func TestConnectivityHealth_ResetsOnClear(t *testing.T) {
	p := New()
	seedFailedWSAt(p, "app", 2*wsWarnGrace)
	if _, serving := p.ConnectivityHealth("app"); !serving {
		t.Fatalf("precondition: expected serving-without-ws before clear")
	}
	// Deregister/hibernate clears WS readiness AND the detection window, so a
	// re-woken pool starts fresh rather than warning immediately.
	p.clearWSReady("app")
	if ever, serving := p.ConnectivityHealth("app"); ever || serving {
		t.Fatalf("after clear: got (ever=%v, serving=%v), want (false,false)", ever, serving)
	}
}

func TestConnectivityHealth_WSReadySuppressesWarning(t *testing.T) {
	p := New()
	// Once a WebSocket has connected, further HTTP traffic must never re-arm the
	// warning for this lifecycle (RecordActivity skips firstFailedWSAt when ready).
	p.MarkWSReady("app")
	seedFailedWSAt(p, "app", 2*wsWarnGrace) // even if a stale served-at leaks in
	p.RecordActivity("app")
	if ever, serving := p.ConnectivityHealth("app"); !ever || serving {
		t.Fatalf("ws-ready then served: got (ever=%v, serving=%v), want (true,false)", ever, serving)
	}
}

func TestRecordActivity_LogsOnceWhenServingWithoutWS(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError})))
	defer slog.SetDefault(prev)

	p := New()
	// Back-date a failed upgrade so the next HTTP activity trips the warning.
	seedFailedWSAt(p, "app", 2*wsWarnGrace)
	p.RecordActivity("app")
	p.RecordActivity("app") // must NOT log a second time

	got := strings.Count(buf.String(), "no WebSocket has connected")
	if got != 1 {
		t.Fatalf("serving-without-ws ERROR logged %d times, want exactly 1\nlog:\n%s", got, buf.String())
	}
	if !strings.Contains(buf.String(), "docs/reverse-proxy/caddy.md") {
		t.Errorf("log message should point at the reverse-proxy docs; got:\n%s", buf.String())
	}
}

func TestConnectivityHealthHTTPOnlyNeverWarns(t *testing.T) {
	p := New()
	p.RecordActivity("probe")
	p.RecordActivity("probe")
	if _, armed := p.firstFailedWSAt["probe"]; armed {
		t.Fatal("HTTP-only traffic armed WebSocket warning")
	}
	if ever, warning := p.ConnectivityHealth("probe"); ever || warning {
		t.Fatalf("HTTP-only health=%t,%t", ever, warning)
	}
	p.recordWSFailure("probe")
	seedFailedWSAt(p, "probe", 2*wsWarnGrace)
	p.RecordActivity("probe")
	if _, warning := p.ConnectivityHealth("probe"); !warning {
		t.Fatal("failed upgrade did not warn")
	}
	p.MarkWSReady("probe")
	p.recordWSFailure("probe")
	if ever, warning := p.ConnectivityHealth("probe"); !ever || warning {
		t.Fatal("successful upgrade did not clear warning")
	}
}

func TestConnectivityWarningOnlyArmsForFailedForwardedUpgrade(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusBadRequest) }))
	defer upstream.Close()
	p := New()
	p.SetPoolSize("probe", 1)
	if err := p.RegisterReplica("probe", 0, upstream.URL, nil, 1); err != nil {
		t.Fatal(err)
	}
	for _, upgrade := range []bool{false, true} {
		req := httptest.NewRequest(http.MethodGet, "/app/probe/websocket", nil)
		if upgrade {
			req.Header.Set("Connection", "Upgrade")
			req.Header.Set("Upgrade", "websocket")
		}
		rec := httptest.NewRecorder()
		p.ServeHTTP(rec, req)
		p.seenMu.RLock()
		_, armed := p.firstFailedWSAt["probe"]
		p.seenMu.RUnlock()
		if armed != upgrade {
			t.Fatalf("upgrade=%t armed=%t status=%d", upgrade, armed, rec.Code)
		}
	}
}
