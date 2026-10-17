package cli

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/deployevent"
)

func TestPrefetchedServerInfoOverlapsPreparation(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		io.WriteString(w, `{"version":"dev","capabilities":{"bundle_env":true},"runtimes":{"r":false}}`)
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	probe := prefetchServerInfo(ctx, &cliConfig{Host: srv.URL})
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("probe did not start asynchronously")
	}
	// Build a real bundle while the server is still withholding its answer.
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "app.py"), "# shiny\n")
	plan, _, err := prepareDeploymentForFlags(dir, &deployFlags{})
	if err != nil || plan.CompressedBytes == 0 {
		t.Fatalf("bundle failed during probe: %v", err)
	}
	select {
	case <-probe.done:
		t.Fatal("probe finished before server replied")
	default:
	}
	close(release)
	info, err := probe.wait()
	if err != nil || !info.Capabilities.BundleEnv || info.Runtimes["r"] {
		t.Fatalf("response=%+v err=%v", info, err)
	}
}

func TestPrefetchedServerInfoCancellation(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
		close(canceled)
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	probe := prefetchServerInfo(ctx, &cliConfig{Host: srv.URL})
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("probe never started")
	}
	cancel()
	select {
	case <-probe.done:
	case <-time.After(5 * time.Second):
		t.Fatal("probe did not stop")
	}
	_, err := probe.wait()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	select {
	case <-canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("server request was not canceled")
	}
}

func BenchmarkDeployPreparation(b *testing.B) {
	dir := b.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "app.py"), []byte("# shiny\n"), 0600); err != nil {
		b.Fatal(err)
	}
	payload := make([]byte, 8<<20)
	_, _ = rand.New(rand.NewSource(1)).Read(payload)
	if err := os.WriteFile(filepath.Join(dir, "payload.bin"), payload, 0600); err != nil {
		b.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		io.WriteString(w, `{"version":"dev","runtimes":{"r":true}}`)
	}))
	defer srv.Close()
	cfg := &cliConfig{Host: srv.URL}
	for _, parallel := range []bool{false, true} {
		name := "sequential-baseline"
		if parallel {
			name = "overlapped"
		}
		b.Run(name, func(b *testing.B) {
			for b.Loop() {
				var probe *serverInfoProbe
				if parallel {
					probe = prefetchServerInfo(context.Background(), cfg)
				}
				if _, _, err := prepareDeploymentForFlags(dir, &deployFlags{}); err != nil {
					b.Fatal(err)
				}
				var err error
				if parallel {
					_, err = probe.wait()
				} else {
					_, err = probeServer(cfg)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// A writer observing the beginning of bundle preparation lets the fixture
// require the probe to be in flight before the CLI continues local work.
type bundlePhaseWriter struct {
	t       *testing.T
	started <-chan struct{}
	seen    bool
}

func (w *bundlePhaseWriter) Write(p []byte) (int, error) {
	if strings.Contains(string(p), "Bundling ") {
		w.seen = true
		select {
		case <-w.started:
		case <-time.After(5 * time.Second):
			w.t.Error("server probe did not overlap bundle phase")
		}
	}
	return len(p), nil
}

func TestDeployPreflightReusesServerInfo(t *testing.T) {
	resetFormatState(t)
	quietFlag, outputFlagValue, resolvedFormat = false, "table", ""
	started := make(chan struct{})
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/server-info":
			if calls.Add(1) == 1 {
				close(started)
			}
			io.WriteString(w, `{"version":"dev","capabilities":{"bundle_env":true,"schedule_inputs":true}}`)
		case "/api/apps/demo":
			io.WriteString(w, `{"app":{"slug":"demo","status":"running"}}`)
		case "/api/apps/demo/deploy":
			if err := r.ParseMultipartForm(1 << 20); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			defer r.MultipartForm.RemoveAll()
			io.WriteString(w, `{"slug":"demo","status":"running"}`)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(500)
		}
	}))
	defer srv.Close()
	writeTestCLIConfig(t, srv.URL)
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "app.py"), "# shiny\n")
	mustWrite(t, filepath.Join(dir, "shinyhub.toml"), `[app.env]
MODE="production"
[[schedule]]
name="refresh"
cron="0 * * * *"
cmd="python refresh.py"
inputs=["warehouse"]
`)
	cmd := newDeployCmd()
	cmd.SetOut(io.Discard)
	phaseWriter := &bundlePhaseWriter{t: t, started: started}
	cmd.SetErr(phaseWriter)
	cmd.SetArgs([]string{dir, "--slug", "demo"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !phaseWriter.seen {
		t.Fatal("overlap assertion never observed the bundle phase")
	}
	if calls.Load() != 1 {
		t.Fatalf("server-info calls=%d", calls.Load())
	}
}

// Capability gates require a recognizable server envelope, not merely JSON.
func TestDeployCapabilityGatesRequireServerSignature(t *testing.T) {
	for _, tc := range []struct{ name, manifest string }{
		{"env", "[app.env]\nMODE='production'\n"},
		{"producer", `[[schedule]]
name="refresh"
cron="0 * * * *"
cmd="python refresh.py"
inputs=["warehouse"]
`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetFormatState(t)
			quietFlag, outputFlagValue, resolvedFormat = false, "json", ""
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/api/server-info" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(500)
					return
				}
				calls.Add(1)
				io.WriteString(w, `{"hello":"world"}`)
			}))
			defer srv.Close()
			writeTestCLIConfig(t, srv.URL)
			dir := t.TempDir()
			mustWrite(t, filepath.Join(dir, "app.py"), "# shiny\n")
			mustWrite(t, filepath.Join(dir, "shinyhub.toml"), tc.manifest)
			cmd := newDeployCmd()
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			cmd.SetArgs([]string{dir, "--slug", "demo"})
			err := cmd.Execute()
			var nr *serverNotReadyError
			if !errors.As(err, &nr) || nr.status != 0 {
				t.Fatalf("expected server-not-ready classification, got %v", err)
			}
			if calls.Load() != 1 {
				t.Fatalf("server-info calls=%d", calls.Load())
			}
		})
	}
}

func TestDeployBundleFailureCancelsPrefetch(t *testing.T) {
	resetFormatState(t)
	quietFlag, outputFlagValue, resolvedFormat = false, "table", ""
	started := make(chan struct{})
	canceled := make(chan struct{})
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/server-info" {
			t.Errorf("mutated server after invalid bundle: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(500)
			return
		}
		if calls.Add(1) != 1 {
			t.Error("duplicate server-info request")
			w.WriteHeader(500)
			return
		}
		close(started)
		<-r.Context().Done()
		close(canceled)
	}))
	defer srv.Close()
	writeTestCLIConfig(t, srv.URL)
	dir := t.TempDir()
	// The env requires a capability check, but there is no runnable entrypoint.
	mustWrite(t, filepath.Join(dir, "shinyhub.toml"), "[app.env]\nMODE='production'\n")
	cmd := newDeployCmd()
	cmd.SetOut(io.Discard)
	phaseWriter := &bundlePhaseWriter{t: t, started: started}
	cmd.SetErr(phaseWriter)
	cmd.SetArgs([]string{dir, "--slug", "demo"})
	start := time.Now()
	err := cmd.Execute()
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("failed bundle waited %s for probe cancellation", elapsed)
	}
	if err == nil || !strings.Contains(err.Error(), "no app entrypoint found") {
		t.Fatalf("error=%v", err)
	}
	if !phaseWriter.seen {
		t.Fatal("overlap assertion never observed the bundle phase")
	}
	select {
	case <-canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("failed bundle left its probe running")
	}
}

func TestDeployRRuntimeWarningUsesSingleServerInfoRequest(t *testing.T) {
	for _, tc := range []struct {
		name, body       string
		status           int
		disconnect, warn bool
	}{
		{name: "R unavailable", body: `{"version":"dev","runtimes":{"r":false}}`, warn: true},
		{name: "legacy runtimes only", body: `{"runtimes":{"r":false}}`, warn: true},
		{name: "R available", body: `{"version":"dev","runtimes":{"r":true}}`},
		{name: "old server omits runtimes", body: `{"version":"dev"}`},
		{name: "non-200", status: http.StatusNotFound},
		{name: "transport failure", disconnect: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api/server-info":
					calls.Add(1)
					if tc.disconnect {
						conn, _, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Error(err)
							return
						}
						conn.Close()
						return
					}
					if tc.status != 0 {
						w.WriteHeader(tc.status)
					}
					io.WriteString(w, tc.body)
				case "/api/apps/demo":
					io.WriteString(w, `{"app":{"slug":"demo","status":"running"}}`)
				case "/api/apps/demo/deploy":
					_, _ = io.Copy(io.Discard, r.Body)
					io.WriteString(w, `{"slug":"demo","status":"running"}`)
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(500)
				}
			}))
			defer srv.Close()
			writeTestCLIConfig(t, srv.URL)
			dir := t.TempDir()
			mustWrite(t, filepath.Join(dir, "app.R"), "# shiny\n")
			_, stderr, err := execCLISplit(t, "deploy", dir, "--slug", "demo", "-o", "json")
			if err != nil {
				t.Fatalf("deploy failed: %v; stderr=%s", err, stderr)
			}
			warned := strings.Contains(stderr, "server reports no R runtime")
			if warned != tc.warn {
				t.Fatalf("warning=%v want=%v; stderr=%s", warned, tc.warn, stderr)
			}
			if calls.Load() != 1 {
				t.Fatalf("server-info calls=%d", calls.Load())
			}
		})
	}
}

func TestUnchangedRWatchBundleSkipsRuntimeProbe(t *testing.T) {
	resetFormatState(t)
	quietFlag, outputFlagValue, resolvedFormat = false, "json", ""
	transport := &serverInfoCountingTransport{}
	previous := httpClient
	httpClient = &apiClient{&http.Client{Transport: transport, Timeout: 30 * time.Second}}
	t.Cleanup(func() { httpClient = previous })
	writeTestCLIConfig(t, "http://fixture.invalid")
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "app.R"), "# shiny\n")
	f := &deployFlags{slug: "demo", watchMode: true, format: formatJSON}
	plan, _, err := prepareDeploymentForFlags(dir, f)
	if err != nil {
		t.Fatal(err)
	}
	f.watchLastDigest = plan.Digest
	cmd := newDeployCmd()
	cmd.SetContext(context.Background())
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	err = runDeploy(cmd, []string{dir}, f)
	if !errors.Is(err, errWatchBundleUnchanged) {
		t.Fatalf("error=%v", err)
	}
	if transport.calls.Load() != 0 {
		t.Fatalf("requests=%d", transport.calls.Load())
	}
	// Positive control: this counter observes the actual production client.
	if _, err := readServerInfo(context.Background(), &cliConfig{Host: "http://fixture.invalid"}); err != nil {
		t.Fatal(err)
	}
	if transport.calls.Load() != 1 {
		t.Fatalf("positive control requests=%d", transport.calls.Load())
	}
}

type serverInfoCountingTransport struct{ calls atomic.Int32 }

func (t *serverInfoCountingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	t.calls.Add(1)
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header),
		Body: io.NopCloser(strings.NewReader(`{"version":"dev"}`)), Request: r}, nil
}

func TestDeployNDJSONUploadBytesMatchWireLength(t *testing.T) {
	var wireLength atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/apps/demo":
			io.WriteString(w, `{"app":{"slug":"demo","status":"running"}}`)
		case "/api/apps/demo/deploy":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
			}
			if r.ContentLength != int64(len(body)) {
				t.Errorf("declared length=%d received=%d", r.ContentLength, len(body))
			}
			wireLength.Store(int64(len(body)))
			io.WriteString(w, `{"slug":"demo","status":"running"}`)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(500)
		}
	}))
	defer srv.Close()
	writeTestCLIConfig(t, srv.URL)
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "app.py"), "# shiny\n")
	stdout, stderr, err := execCLISplit(t, "deploy", dir, "--slug", "demo", "-o", "ndjson")
	if err != nil {
		t.Fatalf("deploy failed: %v; stderr=%s", err, stderr)
	}
	decoder := json.NewDecoder(strings.NewReader(stdout))
	var statuses []string
	for {
		var event deployevent.Event
		err := decoder.Decode(&event)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if event.Phase == "upload" {
			if event.Bytes <= 0 || event.Bytes != wireLength.Load() {
				t.Fatalf("reported bytes=%d received=%d", event.Bytes, wireLength.Load())
			}
			statuses = append(statuses, event.Status)
		}
	}
	if strings.Join(statuses, ",") != "started,completed" {
		t.Fatalf("upload events=%v", statuses)
	}
}
