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
	"github.com/rvben/shinyhub/internal/db"
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

func TestFailedAppRejectsTrafficToStillRegisteredBackend(t *testing.T) {
	var upstreamCalls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		w.Write([]byte("incompatible producer data"))
	}))
	defer backend.Close()
	p := proxy.New()
	if err := p.Register("failed", backend.URL); err != nil {
		t.Fatal(err)
	}
	var failed atomic.Bool
	p.SetAppStatusLookup(func(string) (string, string) {
		if failed.Load() {
			return "failed", "private producer failure"
		}
		return "running", ""
	})
	server := httptest.NewServer(p)
	defer server.Close()
	client := server.Client()
	client.Timeout = 2 * time.Second
	response, err := client.Get(server.URL + "/app/failed/")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || upstreamCalls.Load() != 1 {
		t.Fatal("healthy registered backend did not serve")
	}
	failed.Store(true)
	for _, kind := range []string{"GET", "HEAD", "POST", "websocket"} {
		t.Run(kind, func(t *testing.T) {
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
			}
			response, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != http.StatusServiceUnavailable || upstreamCalls.Load() != 1 || strings.Contains(string(body), "private producer") {
				t.Fatalf("failed app reached registered backend: status=%d upstream requests=%d body=%s", response.StatusCode, upstreamCalls.Load(), body)
			}
		})
	}
	failed.Store(false)
	response, err = client.Get(server.URL + "/app/failed/")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || upstreamCalls.Load() != 2 {
		t.Fatal("successful repair did not allow healthy serving again")
	}
}

func TestFailedAppReadinessOverridesEarlierSuccess(t *testing.T) {
	for _, customReadiness := range []bool{false, true} {
		t.Run(map[bool]string{false: "websocket readiness", true: "runtime readiness"}[customReadiness], func(t *testing.T) {
			p := proxy.New()
			p.MarkWSReady("failed")
			if customReadiness {
				p.SetAppReadyFunc(func(string) bool { return true })
			}
			var failed atomic.Bool
			p.SetAppStatusLookup(func(string) (string, string) {
				if failed.Load() {
					return "failed", "private producer failure"
				}
				return "running", ""
			})
			server := httptest.NewServer(p)
			defer server.Close()
			client := server.Client()
			client.Timeout = 2 * time.Second
			url := server.URL + "/app/failed/.shinyhub/ready"
			response, err := client.Get(url)
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Fatal("healthy app was not ready")
			}
			failed.Store(true)
			for _, method := range []string{http.MethodGet, http.MethodHead} {
				req, _ := http.NewRequest(method, url, nil)
				response, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(response.Body)
				response.Body.Close()
				if err != nil {
					t.Fatal(err)
				}
				if response.StatusCode != http.StatusServiceUnavailable || (method == http.MethodGet && string(body) != `{"ready":false}`) || (method == http.MethodHead && len(body) != 0) {
					t.Fatalf("failed app retained ready verdict: method=%s status=%d body=%s", method, response.StatusCode, body)
				}
			}
		})
	}
}

func TestFailedAppRejectsRegisteredBackendDuringCorrectiveDeployment(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("incompatible producer data"))
	}))
	defer backend.Close()
	p := proxy.New()
	if err := p.Register("failed", backend.URL); err != nil {
		t.Fatal(err)
	}
	app := &db.App{Status: "failed", LastDeploymentStatus: db.DeploymentPending, LastError: "private failure"}
	p.SetAppStatusLookup(func(string) (string, string) { return app.MissStatus(true) })
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/app/failed/", nil))
	if rec.Code != http.StatusServiceUnavailable || strings.Contains(rec.Body.String(), "incompatible producer data") || strings.Contains(rec.Body.String(), "private failure") {
		t.Fatalf("corrective deploy masked failed consumer: status=%d body=%s", rec.Code, rec.Body.String())
	}
}
