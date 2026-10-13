package cli

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestProducerFailedStatusHasNoVersionMismatchHint(t *testing.T) {
	if hint := unknownStatusHint("failed"); hint != "" {
		t.Fatalf("same-version producer failure produces false upgrade advice: %s", hint)
	}
}

func TestWaitForHealthy_ProducerFailureFailsFast(t *testing.T) {
	polls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/logs") {
			return
		}
		polls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"app":{"status":"failed","last_replica_error":"producer run 1413 failed after a data write"}}`))
	}))
	defer srv.Close()
	var out bytes.Buffer
	err := waitForHealthyWithOutput(&cliConfig{Host: srv.URL, Token: "tok"}, "demo", 30*time.Second, &out)
	if err == nil || !strings.Contains(err.Error(), "failed") || !strings.Contains(err.Error(), "producer run 1413") || strings.Contains(err.Error(), "timed out") || polls != 1 {
		t.Fatalf("producer failure did not surface immediately: err=%v polls=%d", err, polls)
	}
	if strings.Contains(out.String(), "upgrade") {
		t.Fatalf("false upgrade advice: %s", out.String())
	}
}

func TestFleetHealthLoop_ProducerFailureFailsFast(t *testing.T) {
	var out bytes.Buffer
	calls := 0
	err := waitForFleetHealthLoop("demo", 2*time.Minute, time.Second, 30*time.Second,
		func() (bool, string, error) { calls++; return false, "failed", nil },
		stepClock(time.Second), func(time.Duration) { t.Fatal("failed app must not wait") }, &out)
	if err == nil || !strings.Contains(err.Error(), "failed") || strings.Contains(err.Error(), "timed out") || calls != 1 {
		t.Fatalf("failed app was not terminal on first poll: err=%v calls=%d", err, calls)
	}
	if strings.Contains(out.String(), "upgrade") {
		t.Fatalf("false upgrade advice: %s", out.String())
	}
}
