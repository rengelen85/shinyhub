package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/schedulespec"
)

// Exercise actual producer execution and the API's unsafe-publication callback,
// then repair the failed deploy obligation through the public retry endpoint.
func TestProducerRecovery_EndToEnd(t *testing.T) {
	srv, store, token, jm := buildScheduleE2EServer(t)
	srv.SetJobs(jm, nil)
	slug := "producer-recovery"
	if _, err := store.CreateApp(db.CreateAppParams{Slug: slug, Name: slug, OwnerID: 1}); err != nil {
		t.Fatal(err)
	}
	app, err := store.GetAppBySlug(slug)
	if err != nil {
		t.Fatal(err)
	}
	bundle := t.TempDir()
	dep, err := store.BeginDeployment(app.ID, "v1", bundle)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetDeploymentDigest(dep.ID, "sha256:v1"); err != nil {
		t.Fatal(err)
	}
	if err := store.PromoteDeployment(dep.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: slug, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	schedule, err := store.CreateSchedule(db.CreateScheduleParams{
		AppID: app.ID, Name: "producer", CronExpr: "0 5 * * *", CommandJSON: `["sh","-c","test -f repaired"]`, Enabled: true,
		TimeoutSeconds: 30, OverlapPolicy: "skip", MissedPolicy: "skip", DeployTrigger: schedulespec.DeployTriggerBundleChange,
		OnSuccess: "none", RollFallback: "defer",
	})
	if err != nil {
		t.Fatal(err)
	}
	obligations, err := store.ReconcileDeployObligationsForDeployment(app.ID, dep.ID)
	if err != nil || len(obligations) != 1 {
		t.Fatalf("obligations=%+v err=%v", obligations, err)
	}
	claimed, err := store.ClaimNextDeployObligationFor(app.ID, dep.ID)
	if err != nil {
		t.Fatal(err)
	}
	runID, err := jm.RunDeployObligation(claimed)
	if err != nil {
		t.Fatal(err)
	}
	// Wait for the physical command before draining the remaining callbacks.
	deadline := time.Now().Add(5 * time.Second)
	for {
		run, err := store.GetScheduleRun(runID)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status != "running" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("producer did not finish")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Drain all execution and callbacks, not merely the terminal database row.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := jm.InterruptAndDrain(ctx); err != nil {
		t.Fatal(err)
	}

	run, err := store.GetScheduleRun(runID)
	if err != nil || run.Status != "failed" {
		t.Fatalf("failed producer run=%+v err=%v", run, err)
	}
	app, err = store.GetAppBySlug(slug)
	if err != nil || app.Status != "failed" || !strings.Contains(app.LastError, fmt.Sprint(runID)) {
		t.Fatalf("failure app=%+v err=%v", app, err)
	}
	if err := os.WriteFile(filepath.Join(bundle, "repaired"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	jm.ResumeAdmissions()
	path := fmt.Sprintf("/api/apps/%s/schedules/%d/convergence/retry", slug, schedule)
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, authedRequest(t, http.MethodPost, path, nil, token))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("retry status=%d: %s", rec.Code, rec.Body.String())
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		obligation, err := store.GetDeployObligation(claimed.ID)
		if err != nil {
			t.Fatal(err)
		}
		if obligation.Status == "satisfied" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("repair obligation=%+v", obligation)
		}
		time.Sleep(10 * time.Millisecond)
	}
	rec = httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, authedRequest(t, http.MethodGet, "/api/apps/"+slug, nil, token))
	var response struct {
		App db.App `json:"app"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK || response.App.DesiredStatus != "running" || response.App.LastReplicaError != "" {
		t.Fatalf("repaired response=%+v status=%d", response, rec.Code)
	}
}
