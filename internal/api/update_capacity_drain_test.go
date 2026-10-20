package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/activation"
	"github.com/rvben/shinyhub/internal/config"
	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/deploy"
	"github.com/rvben/shinyhub/internal/process"
)

func TestOneCoreFleetReleasesDrainedCPUAndRetriesNextApp(t *testing.T) {
	srv, store, token, mgr, _, runtime := buildManifestE2EServer(t, config.RuntimeConfig{})
	defer srv.Close()
	srv.SetHostCapacity(1, "config", 65536, "config")
	setTestCPUQuota(srv, 1)
	srv.SetAvailableCPUForTest(func() (float64, error) { return 1, nil })
	srv.cfg.Server.DrainTimeout = 10 * time.Minute
	runtime.serveTraffic = true
	runtime.holdStarted = make(chan struct{}, 1)
	runtime.holdRelease = make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(runtime.holdRelease) }) }
	defer release()
	a := createGenerationTestApp(t, store, "fleet-a", 1, 16)
	b := createGenerationTestApp(t, store, "fleet-b", 1, 16)
	for _, app := range []*db.App{a, b} {
		if rec := deployBareGeneration(t, srv, token, app.Slug, "print('old')", false); rec.Code != http.StatusOK {
			t.Fatal(rec.Body.String())
		}
	}
	for i := range 6 {
		slug := fmt.Sprintf("idle-%d", i)
		mgr.ForceEntry(slug, process.ProcessInfo{Slug: slug, Status: process.StatusRunning})
	}
	done := make(chan int, 1)
	go func() {
		status, _ := proxyGenerationBody(srv, a.Slug, "?hold=1")
		done <- status
	}()
	select {
	case <-runtime.holdStarted:
	case <-time.After(time.Second):
		t.Fatal("old request did not reach backend")
	}
	if rec := deployBareGeneration(t, srv, token, a.Slug, "print('new')", false); rec.Code != http.StatusOK {
		t.Fatalf("first surge must fit exactly: %d %s", rec.Code, rec.Body.String())
	}
	if live, draining := srv.localCPUWorkers(); live != 9 || draining != 1 {
		t.Fatalf("retained session accounting = %d live, %d draining", live, draining)
	}
	rec := deployBareGeneration(t, srv, token, b.Slug, "print('new')", false)
	var refusal struct {
		Retryable bool `json:"capacity_retryable"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &refusal); err != nil || rec.Code != 409 || !refusal.Retryable {
		t.Fatalf("draining predecessor must permit fleet wait: %d %s (%v)", rec.Code, rec.Body.String(), err)
	}
	fresh, _ := store.GetAppBySlug(b.Slug)
	if fresh.LastDeploymentStatus != db.DeploymentBlocked || fresh.Status != "running" {
		t.Fatalf("healthy blocked app marked failed: %+v", fresh)
	}
	// A permanent shortage must still fail fast even with a draining worker.
	request := 0.2
	srv.cfg.Server.UpdateCapacity.WorkerCPURequestCores = &request
	var block *activation.CapacityError
	if err := srv.checkUpdateCPU(b, 1); !errors.As(err, &block) || block.Transient {
		t.Fatalf("permanent shortfall was retryable: %v", err)
	}
	srv.cfg.Server.UpdateCapacity.WorkerCPURequestCores = nil
	srv.cfg.Server.UpdateCapacity.MaxSurgeReplicas = 1
	if err := srv.checkUpdateSurge(a, 1); !errors.As(err, &block) || !block.Transient {
		t.Fatalf("retiring predecessor must permit surge wait: %v", err)
	}
	if err := srv.checkUpdateSurge(a, 2); !errors.As(err, &block) || block.Transient {
		t.Fatalf("oversized candidate pool must fail fast: %v", err)
	}
	srv.cfg.Server.UpdateCapacity.MaxSurgeReplicas = 0
	release()
	select {
	case status := <-done:
		if status != http.StatusOK {
			t.Fatalf("old request lost: %d", status)
		}
	case <-time.After(time.Second):
		t.Fatal("old request failed to finish")
	}
	if live := srv.localLiveCPUWorkers(); live != 8 {
		t.Fatalf("session-free predecessor still charged: %d workers", live)
	}
	if rec := deployBareGeneration(t, srv, token, b.Slug, "print('new')", false); rec.Code != http.StatusOK {
		t.Fatalf("second app could not use released slot: %d %s", rec.Code, rec.Body.String())
	}
	fresh, _ = store.GetAppBySlug(b.Slug)
	if fresh.LastDeploymentStatus != db.DeploymentSucceeded || srv.blockedUpdateStatus(b.Slug) != nil {
		t.Fatal("successful retry retained blocked state")
	}
}

func TestZeroCPURequestKeepsPressureAndMemoryAdmission(t *testing.T) {
	srv, store, _, mgr, _, _ := buildManifestE2EServer(t, config.RuntimeConfig{})
	defer srv.Close()
	srv.SetHostCapacity(1, "config", 65536, "config")
	setTestCPUQuota(srv, 1)
	zero := 0.0
	srv.cfg.Server.UpdateCapacity.WorkerCPURequestCores = &zero
	for i := range 20 {
		slug := fmt.Sprintf("idle-%d", i)
		mgr.ForceEntry(slug, process.ProcessInfo{Slug: slug, Status: process.StatusRunning})
	}
	app := createGenerationTestApp(t, store, "no-static-budget", 1, 16)
	srv.SetAvailableCPUForTest(func() (float64, error) { return 1, nil })
	if err := srv.checkUpdateCPU(app, 1); err != nil {
		t.Fatalf("zero did not disable static accounting: %v", err)
	}
	srv.SetAvailableCPUForTest(func() (float64, error) { return 0.19, nil })
	var block *activation.CapacityError
	if err := srv.checkUpdateCPU(app, 1); !errors.As(err, &block) || !block.Transient || block.Required == nil || *block.Required != 0.2 {
		t.Fatalf("zero bypassed sampled startup pressure: %v", err)
	}
	srv.SetAvailableCPUForTest(func() (float64, error) { return 1, nil })
	srv.SetAvailableMemoryForTest(func() (int, error) { return 0, nil })
	if err := srv.generationHandoffCapacityCheck(app); !errors.As(err, &block) || block.Resource != "memory" {
		t.Fatalf("zero bypassed memory guard: %v", err)
	}
}

func TestZeroCPURequestImpossibleStartupFailsWithoutSampling(t *testing.T) {
	for _, tc := range []struct {
		name     string
		replicas int
		reserve  float64
	}{
		{"oversized pool", 10, 0.1},
		{"reserve exhausts node", 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, _, _, _, _, _ := buildManifestE2EServer(t, config.RuntimeConfig{})
			defer srv.Close()
			srv.SetHostCapacity(1, "config", 65536, "config")
			setTestCPUQuota(srv, 1)
			zero := 0.0
			srv.cfg.Server.UpdateCapacity = config.UpdateCapacityConfig{
				WorkerCPURequestCores: &zero, ReservedCPUCores: &tc.reserve,
			}
			srv.SetAvailableCPUForTest(func() (float64, error) {
				t.Fatal("impossible startup must fail before sampling")
				return 1, nil
			})
			var block *activation.CapacityError
			err := srv.checkUpdateCPU(&db.App{}, tc.replicas)
			if !errors.As(err, &block) || block.Transient || !strings.Contains(block.Reason, "even when idle") {
				t.Fatalf("impossible startup treated as transient pressure: %v", err)
			}
		})
	}
}

func TestCapacityBlockPersistenceFailureIsNotAdvertisedAsRetryable(t *testing.T) {
	for _, late := range []bool{false, true} {
		t.Run(fmt.Sprintf("late=%t", late), func(t *testing.T) {
			srv, store, token, _, _, _ := buildManifestE2EServer(t, config.RuntimeConfig{})
			defer srv.Close()
			if store.IsPostgres() {
				t.Skip("SQLite trigger injects the failed status write")
			}
			app := createGenerationTestApp(t, store, "block-write", 1, 16)
			if rec := deployBareGeneration(t, srv, token, app.Slug, "print('old')", false); rec.Code != http.StatusOK {
				t.Fatal(rec.Body.String())
			}
			old, err := store.GetActiveDeploymentGeneration(app.ID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.DB().Exec(`CREATE TRIGGER reject_blocked_status
				BEFORE UPDATE OF status ON deployments WHEN NEW.status = 'blocked'
				BEGIN SELECT RAISE(FAIL, 'injected blocked status failure'); END`); err != nil {
				t.Fatal(err)
			}
			available := 0.0
			if late {
				available = 64
				srv.SetDeployRunForTest(func(p deploy.Params) (*deploy.PoolResult, error) {
					result, err := deploy.Run(p)
					if p.PrepareOnly {
						available = 0
					}
					return result, err
				})
			}
			srv.SetAvailableCPUForTest(func() (float64, error) { return available, nil })
			rec := deployBareGeneration(t, srv, token, app.Slug, "print('new')", false)
			if rec.Code != http.StatusInternalServerError || strings.Contains(rec.Body.String(), `"capacity_retryable":true`) {
				t.Fatalf("failed status write advertised as normal capacity wait: %d %s", rec.Code, rec.Body.String())
			}
			history, err := store.ListDeploymentsBySlug(app.Slug)
			if err != nil || len(history) != 2 || history[0].Status != db.DeploymentFailed {
				t.Fatalf("failed status write left a pending attempt: %+v %v", history, err)
			}
			active, err := store.GetActiveDeploymentGeneration(app.ID)
			if err != nil || active.DeploymentID != old.DeploymentID {
				t.Fatalf("status write failure changed serving release: %+v %v", active, err)
			}
			if srv.blockedUpdateStatus(app.Slug) != nil {
				t.Fatal("persistence failure misrepresented as a capacity blocker")
			}
		})
	}
}

func TestFailedProducerBarrierIsNotPresentedAsPreservedCapacity(t *testing.T) {
	srv, store, _, _, _, _ := buildManifestE2EServer(t, config.RuntimeConfig{})
	defer srv.Close()
	app := createGenerationTestApp(t, store, "unsafe-capacity", 1, 16)
	pending, err := store.BeginDeployment(app.ID, "v1", "/bundle")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkDeploymentProducerBarrierEntered(pending.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.FailDeploymentWithReason(pending.ID, "capacity-blocked/cpu: old diagnostic"); err != nil {
		t.Fatal(err)
	}
	if srv.blockedUpdateStatus(app.Slug) != nil {
		t.Fatal("real failure incorrectly promised preserved sessions")
	}
}
