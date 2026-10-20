package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rvben/shinyhub/internal/activation"
	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/deploy"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/config"
	"github.com/rvben/shinyhub/internal/process"
)

func TestDeployCPUExhaustionKeepsServingGeneration(t *testing.T) {
	srv, store, token, mgr, _, runtime := buildManifestE2EServer(t, config.RuntimeConfig{})
	defer srv.Close()
	runtime.serveTraffic = true
	srv.SetAvailableCPUForTest(func() (float64, error) { return 0, nil })
	app := createGenerationTestApp(t, store, "cpu-full", 1, 16)
	if rec := deployBareGeneration(t, srv, token, app.Slug, "print('old')", false); rec.Code != http.StatusOK {
		t.Fatalf("initial deploy: %d %s", rec.Code, rec.Body.String())
	}
	old, err := store.GetActiveDeploymentGeneration(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	runtime.mu.Lock()
	starts := runtime.nextPID
	runtime.mu.Unlock()
	rec := deployBareGeneration(t, srv, token, app.Slug, "print('new')", false)
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "CPU") {
		t.Fatalf("saturated update = %d %s; want CPU capacity refusal", rec.Code, rec.Body.String())
	}
	active, err := store.GetActiveDeploymentGeneration(app.ID)
	if err != nil || active.DeploymentID != old.DeploymentID {
		t.Fatalf("blocked update changed active generation: %+v %v", active, err)
	}
	runtime.mu.Lock()
	after := runtime.nextPID
	runtime.mu.Unlock()
	if after != starts {
		t.Fatalf("blocked update started competing processes: %d -> %d", starts, after)
	}
	if info, ok := mgr.GetGenerationReplica(app.Slug, old.DeploymentID, 0); !ok || info.Status != "running" {
		t.Fatalf("old worker was stopped: %+v", info)
	}
	if status, body := proxyGenerationBody(srv, app.Slug, ""); status != http.StatusOK || body == "" {
		t.Fatalf("old app no longer serves: %d %s", status, body)
	}
}

func TestDeployCapacityBlockIsVisibleAndRetryClearsIt(t *testing.T) {
	srv, store, token, _, _, _ := buildManifestE2EServer(t, config.RuntimeConfig{})
	defer srv.Close()
	app := createGenerationTestApp(t, store, "capacity-status", 1, 16)
	if rec := deployBareGeneration(t, srv, token, app.Slug, "print('old')", false); rec.Code != http.StatusOK {
		t.Fatal(rec.Body.String())
	}
	available := 0.0
	srv.SetAvailableCPUForTest(func() (float64, error) { return available, nil })
	rec := deployBareGeneration(t, srv, token, app.Slug, "print('new')", false)
	var conflict struct {
		State     string  `json:"update_state"`
		Resource  string  `json:"resource"`
		Required  float64 `json:"required"`
		Automatic bool    `json:"automatic_retry"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &conflict); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusConflict || conflict.State != "blocked_capacity" || conflict.Resource != "cpu" || conflict.Required <= 0 || conflict.Automatic {
		t.Fatalf("capacity refusal has no actionable status: %d %s", rec.Code, rec.Body.String())
	}
	readState := func() string {
		req := httptest.NewRequest("GET", "/api/apps/"+app.Slug, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		out := httptest.NewRecorder()
		srv.Router().ServeHTTP(out, req)
		var detail struct {
			App struct {
				Status string `json:"status"`
			} `json:"app"`
			Update struct {
				State     string `json:"state"`
				Reason    string `json:"reason"`
				Automatic bool   `json:"automatic_retry"`
			} `json:"update_status"`
		}
		if out.Code != http.StatusOK {
			t.Fatal(out.Body.String())
		}
		if err := json.Unmarshal(out.Body.Bytes(), &detail); err != nil {
			t.Fatal(err)
		}
		if detail.App.Status != "running" {
			t.Fatalf("update status replaced app availability: %s", out.Body.String())
		}
		if detail.Update.State == "blocked_capacity" && (detail.Update.Reason == "" || detail.Update.Automatic) {
			t.Fatalf("blocked update is misleading: %s", out.Body.String())
		}
		return detail.Update.State
	}
	if state := readState(); state != "blocked_capacity" {
		t.Fatalf("app details lost capacity blocker: %s", state)
	}
	available = 64
	if rec := deployBareGeneration(t, srv, token, app.Slug, "print('new')", false); rec.Code != http.StatusOK {
		t.Fatalf("retry with capacity: %d %s", rec.Code, rec.Body.String())
	}
	if state := readState(); state == "blocked_capacity" {
		t.Fatal("successful update retained obsolete capacity blocker")
	}
}

func TestDeployCapacityLostDuringPreparationPreservesWorkingVersion(t *testing.T) {
	srv, store, token, mgr, _, runtime := buildManifestE2EServer(t, config.RuntimeConfig{})
	defer srv.Close()
	runtime.serveTraffic = true
	app := createGenerationTestApp(t, store, "capacity-race", 1, 16)
	if rec := deployBareGeneration(t, srv, token, app.Slug, "print('old')", false); rec.Code != http.StatusOK {
		t.Fatal(rec.Body.String())
	}
	old, _ := store.GetActiveDeploymentGeneration(app.ID)
	available := 64.0
	srv.SetAvailableCPUForTest(func() (float64, error) { return available, nil })
	srv.SetDeployRunForTest(func(p deploy.Params) (*deploy.PoolResult, error) {
		p.HealthCheck = func(string, time.Duration, http.RoundTripper) error { return nil }
		result, err := deploy.Run(p)
		if p.PrepareOnly {
			available = 0
		}
		return result, err
	})
	rec := deployBareGeneration(t, srv, token, app.Slug, "print('new')", false)
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), `"update_state":"blocked_capacity"`) {
		t.Fatalf("late capacity loss = %d %s; want actionable capacity conflict", rec.Code, rec.Body.String())
	}
	active, err := store.GetActiveDeploymentGeneration(app.ID)
	if err != nil || active.DeploymentID != old.DeploymentID {
		t.Fatalf("working generation changed: %+v %v", active, err)
	}
	if info, ok := mgr.GetGenerationReplica(app.Slug, old.DeploymentID, 0); !ok || info.Status != "running" {
		t.Fatalf("working worker lost: %+v", info)
	}
	history, err := store.ListDeploymentsBySlug(app.Slug)
	if err != nil || history[0].Status != db.DeploymentBlocked || !strings.HasPrefix(history[0].FailureReason, "capacity-blocked/cpu:") {
		t.Fatalf("late blocker invisible in history: %+v %v", history, err)
	}
}

func TestDeployCPUAdmissionBudgetsWholePoolAndFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name      string
		available float64
		err       error
		replicas  int
	}{
		{"whole-pool", 2, nil, 2},
		{"unknown", 64, errors.New("sample failed"), 1},
		{"nan", math.NaN(), nil, 1},
		{"infinite", math.Inf(1), nil, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, store, token, mgr, _, runtime := buildManifestE2EServer(t, config.RuntimeConfig{})
			defer srv.Close()
			request := 1.0
			srv.cfg.Server.UpdateCapacity.WorkerCPURequestCores = &request
			app := createGenerationTestApp(t, store, "cpu-"+tc.name, tc.replicas, 16)
			if rec := deployBareGeneration(t, srv, token, app.Slug, "print('old')", false); rec.Code != http.StatusOK {
				t.Fatal(rec.Body.String())
			}
			old, _ := store.GetActiveDeploymentGeneration(app.ID)
			srv.SetAvailableCPUForTest(func() (float64, error) { return tc.available, tc.err })
			runtime.mu.Lock()
			starts := runtime.nextPID
			runtime.mu.Unlock()
			rec := deployBareGeneration(t, srv, token, app.Slug, "print('new')", false)
			if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), `"resource":"cpu"`) {
				t.Fatalf("unsafe admission: %d %s", rec.Code, rec.Body.String())
			}
			runtime.mu.Lock()
			after := runtime.nextPID
			runtime.mu.Unlock()
			if after != starts {
				t.Fatal("capacity refusal launched a worker")
			}
			for i := 0; i < tc.replicas; i++ {
				if info, ok := mgr.GetGenerationReplica(app.Slug, old.DeploymentID, i); !ok || info.Status != "running" {
					t.Fatalf("worker %d lost: %+v", i, info)
				}
			}
		})
	}
}

func TestDeployExplicitDowntimeDoesNotRequireParallelCPU(t *testing.T) {
	srv, store, token, _, _, _ := buildManifestE2EServer(t, config.RuntimeConfig{})
	defer srv.Close()
	app := createGenerationTestApp(t, store, "cpu-stop-first", 1, 16)
	if rec := deployBareGeneration(t, srv, token, app.Slug, "print('old')", false); rec.Code != http.StatusOK {
		t.Fatal(rec.Body.String())
	}
	old, _ := store.GetActiveDeploymentGeneration(app.ID)
	srv.SetAvailableCPUForTest(func() (float64, error) { return 0, nil })
	rec := deployBareGeneration(t, srv, token, app.Slug, "print('new')", true)
	if rec.Code != http.StatusOK {
		t.Fatalf("explicit stop-first: %d %s", rec.Code, rec.Body.String())
	}
	active, err := store.GetActiveDeploymentGeneration(app.ID)
	if err != nil || active.DeploymentID == old.DeploymentID {
		t.Fatalf("stop-first failed to replace version: %+v %v", active, err)
	}
}

// Advancing the public claim clock avoids waiting a real minute for the
// persisted retry while keeping the real store and coordinator behavior.
type capacityClaimClock struct {
	*db.Store
	due time.Time
}

func (s *capacityClaimClock) ClaimNextScheduleActivation(now time.Time) (*db.ScheduleActivation, error) {
	if !s.due.IsZero() {
		now = s.due
	}
	return s.Store.ClaimNextScheduleActivation(now)
}

func TestScheduledRollCPUBlockDefersWithoutLosingWorkersAndRetries(t *testing.T) {
	cfg := &config.Config{}
	cfg.Runtime.Mode = "native"
	cfg.Runtime.Docker.DefaultMemoryMB = 16
	cfg.Server.DrainTimeout = 5 * time.Millisecond
	srv, app := newScaleTestServer(t, "cpu-roll", 1, cfg)
	defer srv.Close()
	t.Cleanup(func() { _ = srv.manager.StopAll() })
	srv.SetHostCapacity(1, "config", 65536, "config")
	srv.deployReplica = successfulActivationTestDeployer(t, srv, app, 21700)
	// A stop/retry lifecycle requires a worker owned by this manager, not the
	// generic scale fixture's PID 1000, which may belong to a CI host process.
	current, err := srv.store.GetServingDeployment(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	srv.proxy.SetPoolSize(app.Slug, app.Replicas)
	initial, err := srv.deployReplica(deploy.Params{DeploymentID: current.ID, AppVersion: current.Version, DefaultTier: process.DefaultTier}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.store.UpsertReplica(db.UpsertReplicaParams{
		AppID: app.ID, Index: 0, PID: &initial.PID, Port: &initial.Port, Status: "running", DesiredState: "running",
		Provider: initial.Provider, Tier: initial.Tier, EndpointURL: initial.EndpointURL, WorkerID: initial.WorkerID,
		AppVersion: current.Version, DeploymentID: &current.ID,
	}); err != nil {
		t.Fatal(err)
	}
	a := seedClaimedActivation(t, srv.store, app)
	if _, err := srv.store.RequeueRunningScheduleActivations(time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	available := 0.0
	srv.SetAvailableCPUForTest(func() (float64, error) { return available, nil })
	clock := &capacityClaimClock{Store: srv.store}
	coordinator := activation.New(clock, srv, time.Second)
	if worked, err := coordinator.ProcessNext(context.Background()); err != nil || !worked {
		t.Fatalf("capacity defer: %v %v", worked, err)
	}
	deferred, err := srv.store.GetScheduleActivation(a.ID)
	if err != nil || deferred.Status != "deferred_capacity" || !strings.Contains(deferred.DeferReason, "CPU") || deferred.Attempts != 0 {
		t.Fatalf("durable CPU defer: %+v %v", deferred, err)
	}
	rows, err := srv.store.ListReplicas(app.ID)
	if err != nil || len(rows) != 1 || rows[0].Status != "running" {
		t.Fatalf("working worker changed: %+v %v", rows, err)
	}
	available = 64
	clock.due = deferred.DueAt.Add(time.Second)
	if worked, err := coordinator.ProcessNext(context.Background()); err != nil || !worked {
		t.Fatalf("capacity retry: %v %v", worked, err)
	}
	finished, err := srv.store.GetScheduleActivation(a.ID)
	if err != nil || finished.Status != "succeeded" {
		t.Fatalf("roll did not recover: %+v %v", finished, err)
	}
}

func TestScheduleCreationWarnsWhenRollCPUCannotFit(t *testing.T) {
	srv, store, token, _, _, _ := buildManifestE2EServer(t, config.RuntimeConfig{})
	defer srv.Close()
	app := createGenerationTestApp(t, store, "cpu-roll-warning", 1, 16)
	srv.SetAvailableCPUForTest(func() (float64, error) { return 0, nil })
	body := []byte(`{"name":"refresh","cron_expr":"*/15 * * * *","command":["true"],"timeout_seconds":60,"overlap_policy":"skip","missed_policy":"skip","on_success":"roll"}`)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/apps/"+app.Slug+"/schedules", strings.NewReader(string(body)))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	srv.Router().ServeHTTP(rec, req)
	var dto struct {
		Advisory string `json:"roll_feasibility_advisory"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &dto); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusCreated || !strings.Contains(dto.Advisory, "CPU") || !strings.Contains(dto.Advisory, "defer") {
		t.Fatalf("CPU roll warning missing: %d %s", rec.Code, rec.Body.String())
	}
}

func TestRecoveredFleetCPUWarningUsesAdmissionWorkerCount(t *testing.T) {
	srv, store, token, mgr, _, _ := buildManifestE2EServer(t, config.RuntimeConfig{})
	defer srv.Close()
	srv.SetHostCapacity(1, "config", 65536, "config")
	request := 0.25
	srv.cfg.Server.UpdateCapacity.WorkerCPURequestCores = &request
	srv.SetAvailableCPUForTest(func() (float64, error) { return 1, nil })
	app := createGenerationTestApp(t, store, "warning-budget", 1, 16)
	if rec := deployBareGeneration(t, srv, token, app.Slug, "print('old')", false); rec.Code != http.StatusOK {
		t.Fatal(rec.Body.String())
	}
	if warning := srv.UpdateCPUCapacityWarning(); warning != "" {
		t.Fatalf("one-worker fleet fits: %s", warning)
	}
	// One running generation, one suspended worker and one unconfirmed start.
	// Terminated and remote workers must not consume local CPU budgets.
	for i, info := range []process.ProcessInfo{
		{Status: process.StatusSuspended}, {Status: process.StatusStarting},
		{Status: process.StatusStopped}, {Status: process.StatusCrashed},
		{Status: process.StatusRunning, Provider: "fargate"},
		{Status: process.StatusRunning, Tier: "remote"},
	} {
		info.Slug = fmt.Sprintf("recovered-%d", i)
		mgr.ForceEntry(info.Slug, info)
	}
	srv.SetNodeForTier(func(tier string) string {
		if tier == "remote" {
			return "remote-node"
		}
		return ""
	})
	warning := srv.UpdateCPUCapacityWarning()
	if !strings.Contains(warning, "3 live worker(s)") {
		t.Fatalf("incorrect recovered fleet warning: %s", warning)
	}
	if block := srv.checkUpdateCPU(app, 1); block == nil || !strings.Contains(block.Error(), "CPU budget") {
		t.Fatalf("warning and admission disagree: %v", block)
	}
	// A configured host override cannot conceal the tighter cgroup quota.
	srv.SetHostCapacity(16, "config", 65536, "config")
	setTestCPUQuota(srv, 1)
	if warning := srv.UpdateCPUCapacityWarning(); !strings.Contains(warning, "1.00 effective cores") {
		t.Fatalf("override bypassed quota warning: %s", warning)
	}
}

func TestCapacityConflictDistinguishesTransientPressure(t *testing.T) {
	for _, transient := range []bool{false, true} {
		block := &activation.CapacityError{Resource: "cpu", Reason: "CPU pressure", Transient: transient, RetryAfter: time.Minute}
		rec := httptest.NewRecorder()
		writeCapacityConflict(rec, block)
		var body struct {
			Kind      string `json:"failure_kind"`
			Retryable bool   `json:"capacity_retryable"`
			Delay     int    `json:"retry_after_seconds"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if rec.Header().Get("X-ShinyHub-Conflict") != "generation-handoff-deferred" {
			t.Fatal("capacity refusal lost backward-compatible conflict header")
		}
		if body.Kind != "capacity" || body.Retryable != transient || body.Delay != 60 || rec.Header().Get("Retry-After") != "60" {
			t.Fatalf("response=%s", rec.Body.String())
		}
		if transient && strings.Contains(rec.Body.String(), "allow-downtime") {
			t.Fatal("pressure refusal suggests dropping sessions")
		}
		stream := httptest.NewRecorder()
		responder := newDeployResponder(stream, httptest.NewRequest("POST", "/", nil))
		responder.stream = true
		responder.capacityFailure(block)
		var event struct {
			Kind      string `json:"failure_kind"`
			Retryable bool   `json:"capacity_retryable"`
			Delay     int    `json:"retry_after_seconds"`
		}
		if err := json.Unmarshal(stream.Body.Bytes(), &event); err != nil {
			t.Fatal(err)
		}
		if event.Kind != body.Kind || event.Retryable != body.Retryable || event.Delay != body.Delay {
			t.Fatalf("stream lost retry hints: %s", stream.Body.String())
		}
	}
}

func TestCapacityAfterProducerBarrierIsNotPreservedRefusal(t *testing.T) {
	srv, store, token, _ := newManifestE2EServerWithJobs(t)
	defer srv.Close()
	app := createGenerationTestApp(t, store, "capacity-after-writer", 1, 16)
	srv.SetDeployRunForTest(func(p deploy.Params) (*deploy.PoolResult, error) {
		if !p.PrepareOnly {
			return nil, &activation.CapacityError{Resource: "cpu", Reason: "CPU: busy", Transient: true, RetryAfter: time.Minute}
		}
		return deploy.Run(p)
	})
	body, ct := buildMultiFileBundleUpload(t, map[string]string{"app.py": "print('consumer')", "shinyhub.toml": "[[schedule]]\nname='refresh'\ncron='0 5 * * *'\ncmd='true'\ndeploy_trigger='bundle_change'\n"})
	req := httptest.NewRequest(http.MethodPost, "/api/apps/"+app.Slug+"/deploy", body)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", ct)
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, req)
	if rec.Code != 500 || !strings.Contains(rec.Body.String(), `"failure_kind":"server_error"`) || strings.Contains(rec.Body.String(), `"update_state":"blocked_capacity"`) {
		t.Fatalf("mutating capacity outcome: %d %s", rec.Code, rec.Body)
	}
	latest, _ := store.ListDeploymentsBySlug(app.Slug)
	if len(latest) != 1 || latest[0].Status != db.DeploymentFailed {
		t.Fatalf("mutating capacity failure must remain failed, not blocked or pending: %+v", latest)
	}
	var barrier int
	if err := store.DB().QueryRow("SELECT producer_barrier_entered FROM deployments WHERE id=?", latest[0].ID).Scan(&barrier); err != nil || barrier != 1 {
		t.Fatalf("test did not enter physical producer barrier: %d %v", barrier, err)
	}
	live, _ := store.GetAppByID(app.ID)
	if live.Status == "running" {
		t.Fatal("unsafe consumer restored after writer failure")
	}
}
