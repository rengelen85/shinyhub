package lifecycle_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/dbtest"
	"github.com/rvben/shinyhub/internal/proxy"
	"github.com/rvben/shinyhub/internal/schedulespec"
)

type directRecoveryHTTPTransport struct{}

func (directRecoveryHTTPTransport) TransportForReplica(*db.Replica) (http.RoundTripper, error) {
	return http.DefaultTransport, nil
}

func TestProducerRepairNeverReadoptsInvalidatedConsumer(t *testing.T) {
	store := dbtest.New(t)
	app := mustCreateApp(t, store, "repair-routing")
	dep, err := store.BeginDeployment(app.ID, "v1", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = store.PromoteDeployment(dep.ID); err != nil {
		t.Fatal(err)
	}
	if err = store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: app.Slug, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	var oldRequests atomic.Int64
	old := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		oldRequests.Add(1)
		_, _ = io.WriteString(w, "old consumer data")
	}))
	t.Cleanup(old.Close)
	if err = store.UpsertReplica(db.UpsertReplicaParams{AppID: app.ID, Index: 0, Status: "running", Provider: "docker", EndpointURL: old.URL, WorkerID: "old-container", DeploymentID: &dep.ID}); err != nil {
		t.Fatal(err)
	}
	p := proxy.New()
	p.SetAppStatusLookup(func(slug string) (string, string) {
		app, err := store.GetAppBySlug(slug)
		if err != nil {
			return "", ""
		}
		return app.Status, app.LastError
	})
	syncer := proxy.NewPoolSyncer(p, store, directRecoveryHTTPTransport{}, slog.Default(), false)
	server := httptest.NewServer(p)
	t.Cleanup(server.Close)
	client := server.Client()
	client.Timeout = 2 * time.Second
	client.Jar, err = cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	visit := func() (int, string) {
		t.Helper()
		resp, err := client.Get(server.URL + "/app/" + app.Slug + "/")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, string(body)
	}
	syncer.RunOnce(context.Background())
	if status, body := visit(); status != http.StatusOK || body != "old consumer data" {
		t.Fatalf("fixture did not serve original consumer: %d %s", status, body)
	}
	command, fingerprint, err := schedulespec.ProducerIdentity(`["refresh"]`)
	if err != nil {
		t.Fatal(err)
	}
	schedule, err := store.CreateSchedule(db.CreateScheduleParams{AppID: app.ID, Name: "refresh", CronExpr: "0 5 * * *", CommandJSON: command, Enabled: true, DeployTrigger: "bundle_change", OnSuccess: "none", RollFallback: "defer"})
	if err != nil {
		t.Fatal(err)
	}
	complete := func(status string) {
		t.Helper()
		run, err := store.InsertScheduleRun(db.InsertScheduleRunParams{ScheduleID: schedule, Status: "running", Trigger: "manual", StartedAt: time.Now().UTC(), DeploymentID: &dep.ID, AppVersion: dep.Version, ContentDigest: dep.ContentDigest, ProducerCommandJSON: command, ProducerFingerprint: fingerprint, PublishesData: true, OnSuccess: "none", RollFallback: "defer"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = store.CompleteScheduleRunAndEnqueueActivation(db.CompleteScheduleRunParams{RunID: run, Status: status, FinishedAt: time.Now().UTC(), DataWriteAttempted: true}); err != nil {
			t.Fatal(err)
		}
	}
	complete("failed")
	for range 3 {
		syncer.RunOnce(context.Background())
		status, body := visit()
		if status != http.StatusServiceUnavailable || !strings.Contains(body, "This app is unavailable") {
			t.Fatalf("quarantine response: %d %s", status, body)
		}
	}
	complete("succeeded")
	if err = store.EnforceCompatibilityQuarantines(); err != nil {
		t.Fatal(err)
	}
	// The old process is deliberately alive and still reachable. Clearing
	// the app's fence is not proof that this process reloaded repaired data.
	for range 3 {
		syncer.RunOnce(context.Background())
		_, body := visit()
		if body == "old consumer data" || strings.Contains(body, "This app is unavailable") {
			t.Fatalf("repair either revived unsafe data or stayed failed: %s", body)
		}
	}
	if oldRequests.Load() != 1 {
		t.Fatalf("invalidated consumer received traffic after failure: %d requests", oldRequests.Load())
	}
	fresh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "repaired consumer data") }))
	t.Cleanup(fresh.Close)
	if err = store.UpsertReplica(db.UpsertReplicaParams{AppID: app.ID, Index: 0, Status: "running", Provider: "docker", EndpointURL: fresh.URL, WorkerID: "new-container", DeploymentID: &dep.ID, ConsumerBooted: true}); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		syncer.RunOnce(context.Background())
		if status, body := visit(); status != http.StatusOK || body != "repaired consumer data" {
			t.Fatalf("replacement did not serve sticky client: %d %s", status, body)
		}
	}
	if oldRequests.Load() != 1 {
		t.Fatalf("sticky client returned to retired consumer: %d requests", oldRequests.Load())
	}
}
