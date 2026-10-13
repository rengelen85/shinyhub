package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/db"
)

func TestProducerFailureAndRepairThroughHTTP(t *testing.T) {
	srv, store, token, runtime := newManifestE2EServerWithJobs(t)
	t.Cleanup(func() { srv.jobs.Stop(context.Background()); _ = srv.manager.StopAll(); srv.Close() })
	if _, err := store.CreateApp(db.CreateAppParams{Slug: "producer-repair", Name: "producer-repair", OwnerID: 1, Access: "private"}); err != nil {
		t.Fatal(err)
	}
	manifest := strings.Replace(groupedProducerManifest, `deploy_trigger = "first_deploy"`, `deploy_trigger = "bundle_change"`, 1)
	body, contentType := buildMultiFileBundleUpload(t, map[string]string{"app.py": "print('app')", "refresh.py": "print('data')", "shinyhub.toml": manifest})
	request := func(method, path string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(method, path, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		srv.Router().ServeHTTP(rec, req)
		return rec
	}
	req := httptest.NewRequest(http.MethodPost, "/api/apps/producer-repair/deploy", body)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("deploy: %d %s", rec.Code, rec.Body.String())
	}
	app, err := store.GetAppBySlug("producer-repair")
	if err != nil {
		t.Fatal(err)
	}
	scheduleID := scheduleIDByName(t, store, app.ID, "refresh-data")
	srv.proxy.SetAppStatusLookup(func(slug string) (string, string) {
		app, err := store.GetAppBySlug(slug)
		if err != nil {
			return "", ""
		}
		return app.Status, app.LastError
	})
	type appResponse struct {
		App            db.App `json:"app"`
		Quarantined    bool   `json:"compatibility_quarantined"`
		RepairRequired bool   `json:"producer_repair_required"`
		Converged      bool   `json:"schedule_convergence_satisfied"`
	}
	show := func() appResponse {
		t.Helper()
		rec := request(http.MethodGet, "/api/apps/producer-repair")
		if rec.Code != http.StatusOK {
			t.Fatalf("show: %d %s", rec.Code, rec.Body.String())
		}
		var response appResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		return response
	}
	run := func(want string) int64 {
		t.Helper()
		rec := request(http.MethodPost, fmt.Sprintf("/api/apps/producer-repair/schedules/%d/run", scheduleID))
		if rec.Code != http.StatusAccepted {
			t.Fatalf("run: %d %s", rec.Code, rec.Body.String())
		}
		var response struct {
			RunID int64 `json:"run_id"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for {
			runRec := request(http.MethodGet, fmt.Sprintf("/api/apps/producer-repair/schedules/%d/runs/%d", scheduleID, response.RunID))
			if runRec.Code != http.StatusOK {
				t.Fatalf("get run: %d %s", runRec.Code, runRec.Body.String())
			}
			var state db.ScheduleRun
			if err := json.Unmarshal(runRec.Body.Bytes(), &state); err != nil {
				t.Fatal(err)
			}
			if state.Status == want {
				return response.RunID
			}
			if time.Now().After(deadline) {
				t.Fatalf("run did not become %s: %s", want, runRec.Body.String())
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	runtime.mu.Lock()
	runtime.producerErr = errors.New("private transient upstream failure")
	runtime.mu.Unlock()
	failedID := run("failed")
	failed := show()
	if failed.App.Status != "failed" || !failed.Quarantined || !failed.RepairRequired || failed.Converged || !strings.Contains(failed.App.LastError, fmt.Sprint(failedID)) {
		t.Fatalf("operator cannot diagnose producer failure: %+v", failed)
	}
	visitor := httptest.NewRecorder()
	srv.proxy.ServeHTTP(visitor, httptest.NewRequest(http.MethodGet, "/app/producer-repair/", nil))
	if visitor.Code != http.StatusServiceUnavailable || !strings.Contains(visitor.Body.String(), "This app is unavailable") || strings.Contains(visitor.Body.String(), "private transient") {
		t.Fatalf("visitor failure response: %d %s", visitor.Code, visitor.Body.String())
	}
	// Wait for the failure callback to finish before repairing; the durable
	// terminal row can be observed before consumer cleanup/auditing finishes.
	deadline := time.Now().Add(5 * time.Second)
	for {
		count, err := store.CountAuditEvents("schedule_run_failed")
		if err != nil {
			t.Fatal(err)
		}
		if count == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("failure callback did not finish")
		}
		time.Sleep(10 * time.Millisecond)
	}
	runtime.mu.Lock()
	runtime.producerErr = nil
	runtime.mu.Unlock()
	repairedID := run("succeeded")
	repaired := show()
	if repaired.App.DesiredStatus != "running" || (repaired.App.Status != "running" && repaired.App.Status != "degraded") || repaired.Quarantined || repaired.RepairRequired || !repaired.Converged || repaired.App.LastError != "" {
		t.Fatalf("successful repair did not restore service intent: %+v", repaired)
	}
	durable, err := store.GetAppByID(app.ID)
	if err != nil || durable.Status != "running" {
		t.Fatalf("repair left durable app unavailable: %+v err=%v", durable, err)
	}
	events, err := store.ListAuditEvents("app_status_changed", 10, 0)
	if err != nil || len(events) != 2 {
		t.Fatalf("transition events=%+v err=%v", events, err)
	}
	for i, want := range []struct {
		run              int64
		from, to, reason string
	}{
		{repairedID, "failed", "running", "producer_repair_succeeded"},
		{failedID, "running", "failed", "attempting a data write"},
	} {
		var detail map[string]any
		if err := json.Unmarshal([]byte(events[i].Detail), &detail); err != nil {
			t.Fatal(err)
		}
		if detail["schedule_run_id"] != float64(want.run) || detail["from"] != want.from || detail["to"] != want.to || !strings.Contains(fmt.Sprint(detail["reason"]), want.reason) {
			t.Fatalf("incomplete transition audit: %s", events[i].Detail)
		}
	}
	woke := make(chan string, 1)
	srv.proxy.SetWakeTrigger(func(_ context.Context, slug string) { woke <- slug })
	visitor = httptest.NewRecorder()
	srv.proxy.ServeHTTP(visitor, httptest.NewRequest(http.MethodGet, "/app/producer-repair/", nil))
	if strings.Contains(visitor.Body.String(), "This app is unavailable") {
		t.Fatalf("repair left unavailable page: %s", visitor.Body.String())
	}
	select {
	case slug := <-woke:
		if slug != app.Slug {
			t.Fatalf("woke wrong app: %s", slug)
		}
	case <-time.After(time.Second):
		t.Fatal("repair left the app unable to wake on traffic")
	}
}
