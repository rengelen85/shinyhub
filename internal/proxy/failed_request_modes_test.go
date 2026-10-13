package proxy_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/config"
	"github.com/rvben/shinyhub/internal/proxy"
)

func TestFailedAppResponsesAcrossIsolationAndRequestTypes(t *testing.T) {
	for _, mode := range []config.WorkerIsolationMode{"multiplex", "grouped", "per_session"} {
		for _, kind := range []string{"GET", "HEAD", "POST", "websocket"} {
			t.Run(string(mode)+"/"+kind, func(t *testing.T) {
				p := proxy.New()
				p.SetPoolSize("failed", 1)
				p.SetPoolMode("failed", mode, 4, 2)
				p.SetAppStatusLookup(func(string) (string, string) { return "failed", "private upstream credentials <secret>" })
				var wakes atomic.Int64
				p.SetWakeTrigger(func(context.Context, string) { wakes.Add(1) })
				p.SetOnMissSync(func(string) { t.Error("failed app tried to restore backends") })
				logged := make(chan proxy.AccessLogEntry, 1)
				p.SetAccessLogger(func(e proxy.AccessLogEntry) { logged <- e })
				server := httptest.NewServer(p)
				t.Cleanup(server.Close)
				method := kind
				if kind == "websocket" {
					method = http.MethodGet
				}
				req, err := http.NewRequest(method, server.URL+"/app/failed/", nil)
				if err != nil {
					t.Fatal(err)
				}
				if kind == "websocket" {
					req.Header.Set("Connection", "Upgrade")
					req.Header.Set("Upgrade", "websocket")
					req.Header.Set("Sec-WebSocket-Version", "13")
					req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
				}
				client := server.Client()
				client.Timeout = 2 * time.Second
				response, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				body, err := io.ReadAll(response.Body)
				if err != nil {
					t.Fatal(err)
				}
				if response.StatusCode != http.StatusServiceUnavailable || wakes.Load() != 0 {
					t.Fatalf("failed app response: status=%d wakes=%d body=%s", response.StatusCode, wakes.Load(), body)
				}
				if strings.Contains(string(body), "secret") || strings.Contains(string(body), proxy.LoadingPageSentinel) {
					t.Fatalf("failed app leaked diagnostics or offered loading: %s", body)
				}
				if method == http.MethodHead {
					if len(body) != 0 {
						t.Fatalf("HEAD returned a body: %s", body)
					}
				} else if !strings.Contains(string(body), "This app is unavailable") {
					t.Fatalf("missing unavailable message: %s", body)
				}
				var entry proxy.AccessLogEntry
				select {
				case entry = <-logged:
				case <-time.After(2 * time.Second):
					t.Fatal("failed request was not recorded")
				}
				if entry.Status != http.StatusServiceUnavailable || !entry.Fallback || entry.FallbackReason != "app-failed" {
					t.Fatalf("misclassified failure: %+v", entry)
				}
			})
		}
	}
}
