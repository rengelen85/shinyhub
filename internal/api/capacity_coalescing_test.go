package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/activation"
	"github.com/rvben/shinyhub/internal/config"
	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/jobs"
	"github.com/rvben/shinyhub/internal/lifecycle/scheduler"
	"github.com/rvben/shinyhub/internal/process"
)

func TestProducerPublicationRetainsCapacityWindowInAppStatus(t *testing.T) {
	srv, store, token, manager, appsDir, _ := buildManifestE2EServer(t, config.RuntimeConfig{Mode: "native"})
	defer srv.Close()
	jobManager, err := jobs.NewManager(manager, nil, process.DefaultTier, store, nil, appsDir, appsDir)
	if err != nil {
		t.Fatal(err)
	}
	defer jobManager.Stop(context.Background())
	srv.SetJobs(jobManager, scheduler.New(jobManager, store, time.UTC))
	app := createGenerationTestApp(t, store, "coalesced-capacity", 1, 16)
	if rec := deployBareGeneration(t, srv, token, app.Slug, "print('serving')", false); rec.Code != http.StatusOK {
		t.Fatal(rec.Body.String())
	}
	request := func(method, path, body string, status int) []byte {
		t.Helper()
		req := httptest.NewRequest(method, "/api/apps/"+app.Slug+path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		srv.Router().ServeHTTP(rec, req)
		if rec.Code != status {
			t.Fatalf("%s %s: %d %s", method, path, rec.Code, rec.Body.String())
		}
		return rec.Body.Bytes()
	}
	var schedule struct {
		ID int64 `json:"id"`
	}
	if err := json.Unmarshal(request("POST", "/schedules", `{"name":"refresh","cron_expr":"0 0 * * *","command":["true"],"enabled":false,"timeout_seconds":30,"overlap_policy":"skip","missed_policy":"skip","deploy_trigger":"first_deploy","on_success":"roll","max_defer_age_seconds":3600}`, 201), &schedule); err != nil || schedule.ID == 0 {
		t.Fatalf("schedule: %+v %v", schedule, err)
	}
	state := func() *db.ScheduleActivation {
		t.Helper()
		var result struct {
			Activations []*db.ScheduleActivation `json:"latest_schedule_activations"`
		}
		if err := json.Unmarshal(request("GET", "", "", 200), &result); err != nil {
			t.Fatal(err)
		}
		if len(result.Activations) == 0 {
			return nil
		}
		return result.Activations[0]
	}
	run := func() *db.ScheduleActivation {
		t.Helper()
		var run struct {
			ID int64 `json:"run_id"`
		}
		if err := json.Unmarshal(request("POST", fmt.Sprintf("/schedules/%d/run", schedule.ID), "{}", 202), &run); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if current := state(); current != nil && current.ScheduleRunID != nil && *current.ScheduleRunID == run.ID {
				return current
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("producer did not publish a roll request")
		return nil
	}
	first := run()
	srv.SetAvailableCPUForTest(func() (float64, error) { return 0, nil })
	coordinator := activation.New(store, srv, time.Second)
	if worked, err := coordinator.ProcessNext(context.Background()); !worked || err != nil {
		t.Fatalf("defer: %v %v", worked, err)
	}
	deferred := state()
	if deferred.Status != "deferred_capacity" || deferred.CapacityDeferrals != 1 || deferred.CapacityDeferredAt == nil {
		t.Fatalf("first defer: %+v", deferred)
	}
	next := run()
	if next.ID == first.ID || next.CapacityDeferrals != 1 || next.CapacityDeferredAt == nil || !next.CapacityDeferredAt.Equal(*deferred.CapacityDeferredAt) {
		t.Fatalf("successful data publication reset the capacity window: first=%+v next=%+v", deferred, next)
	}
}
