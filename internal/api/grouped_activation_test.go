package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/activation"
	"github.com/rvben/shinyhub/internal/config"
	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/deploy"
	"github.com/rvben/shinyhub/internal/lifecycle"
)

func TestGroupedRollingRestartUsesDistinctServingIdentity(t *testing.T) {
	srv, store, token, _, _, rt := buildManifestE2EServer(t, config.RuntimeConfig{})
	defer srv.Close()
	rt.serveTraffic = true
	srv.cfg.Server.DrainTimeout = 20 * time.Millisecond
	srv.SetAvailableMemoryForTest(func() (int, error) { return 4096, nil })
	app := createGenerationTestApp(t, store, "grouped-roll", 1, 16)
	if _, err := store.DB().Exec("UPDATE apps SET worker_isolation='grouped',worker_grouped_size=4,worker_max_workers=2 WHERE id=?", app.ID); err != nil {
		t.Fatal(err)
	}
	if rec := deployBareGeneration(t, srv, token, app.Slug, "print('v1')", false); rec.Code != 200 {
		t.Fatalf("deploy: %d %s", rec.Code, rec.Body.String())
	}
	srv.SetDeployRunForTest(func(p deploy.Params) (*deploy.PoolResult, error) {
		if p.Preparation != deploy.PrepareSkip {
			t.Fatal("same-bundle roll attempted preparation")
		}
		p.HealthCheck = func(string, time.Duration, http.RoundTripper) error { return nil }
		// Ordinary serving workers need the operation lock during candidate
		// readiness. A rolling activation must not hold it across this wait.
		p.HealthCheck = func(string, time.Duration, http.RoundTripper) error {
			acquired := make(chan struct{})
			go func() { release := srv.acquireDeployLock(app.Slug); release(); close(acquired) }()
			select {
			case <-acquired:
				waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				defer cancel()
				if err := srv.WaitForAppOperations(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
					return errors.New("ownership shutdown lost readiness operation")
				}
				return nil
			case <-time.After(time.Second):
				return errors.New("serving spawn blocked behind activation readiness")
			}
		}
		return deploy.Run(p)
	})
	first, err := store.GetServingDeployment(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	previous := first.ID
	for i := 0; i < 2; i++ {
		app, err = store.GetAppByID(app.ID)
		if err != nil {
			t.Fatal(err)
		}
		current, err := store.GetServingDeployment(app.ID)
		if err != nil {
			t.Fatal(err)
		}
		a, err := store.EnqueueRollingRestart(app, current)
		if err != nil {
			t.Fatal(err)
		}
		a, err = store.ClaimNextScheduleActivation(time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if err = srv.Roll(context.Background(), a); err != nil {
			t.Fatal(err)
		}
		after, err := store.GetServingDeployment(app.ID)
		if err != nil {
			t.Fatal(err)
		}
		if after.ID == previous || after.Version != first.Version || after.BundleDir != first.BundleDir || after.ActivationToken == current.ActivationToken {
			t.Fatalf("invalid new serving identity: %+v", after)
		}
		// Retry after publication must complete the same execution, never roll again.
		if err = srv.Roll(context.Background(), a); err != nil {
			t.Fatalf("retry: %v", err)
		}
		retry, _ := store.GetServingDeployment(app.ID)
		if retry.ID != after.ID {
			t.Fatal("retry allocated another generation")
		}
		if err = store.FinishScheduleActivation(a.ID, "succeeded", "", time.Now(), false); err != nil {
			t.Fatal(err)
		}
		if err = srv.waitForPreviousGeneration(context.Background(), app, after.ID, true); err != nil {
			t.Fatal(err)
		}
		previous = after.ID
		history, err := store.ListRecentDeployments(app.ID, 2)
		if err != nil {
			t.Fatal(err)
		}
		updatedApp, err := store.GetAppByID(app.ID)
		if err != nil || updatedApp.ReleaseNumber != 1 {
			t.Fatalf("roll changed release number: %+v, %v", updatedApp, err)
		}
		if len(history) != 1 || history[0].ID != first.ID {
			t.Fatalf("roll changed release history: %+v", history)
		}
	}
}

func TestGroupedRollCapsWarmSpareSurgeAtWorkerLimit(t *testing.T) {
	srv, store, token, _, _, rt := buildManifestE2EServer(t, config.RuntimeConfig{})
	defer srv.Close()
	rt.serveTraffic = true
	app := createGenerationTestApp(t, store, "bounded-grouped-roll", 1, 16)
	if _, err := store.DB().Exec("UPDATE apps SET worker_isolation='grouped',worker_grouped_size=4,worker_max_workers=1,worker_warm_spares=1 WHERE id=?", app.ID); err != nil {
		t.Fatal(err)
	}
	if rec := deployBareGeneration(t, srv, token, app.Slug, "print('serving')", false); rec.Code != http.StatusOK {
		t.Fatal(rec.Body.String())
	}
	srv.cfg.Server.UpdateCapacity.MaxSurgeReplicas = 1
	srv.SetDeployRunForTest(func(p deploy.Params) (*deploy.PoolResult, error) {
		p.HealthCheck = func(string, time.Duration, http.RoundTripper) error { return nil }
		return deploy.Run(p)
	})
	app, err := store.GetAppByID(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.GetServingDeployment(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueRollingRestart(app, before); err != nil {
		t.Fatal(err)
	}
	a, err := store.ClaimNextScheduleActivation(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Roll(context.Background(), a); err != nil {
		t.Fatalf("one-worker candidate should fit the surge ceiling: %v", err)
	}
	after, err := store.GetServingDeployment(app.ID)
	if err != nil || after.ID == before.ID {
		t.Fatalf("roll did not publish a new serving generation: %+v %v", after, err)
	}
}

func TestGroupedRestartFallbackKeepsAppLockDuringReadiness(t *testing.T) {
	srv, store, token, _, _, _ := buildManifestE2EServer(t, config.RuntimeConfig{})
	defer srv.Close()
	app := createGenerationTestApp(t, store, "grouped-restart-fallback", 1, 16)
	if _, err := store.DB().Exec("UPDATE apps SET worker_isolation='grouped',worker_grouped_size=4,worker_max_workers=2 WHERE id=?", app.ID); err != nil {
		t.Fatal(err)
	}
	if rec := deployBareGeneration(t, srv, token, app.Slug, "print('v1')", false); rec.Code != 200 {
		t.Fatal(rec.Body.String())
	}
	app, _ = store.GetAppByID(app.ID)
	source, _ := store.GetServingDeployment(app.ID)
	a, err := store.EnqueueRollingRestart(app, source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().Exec("UPDATE schedule_activations SET roll_fallback='restart' WHERE id=?", a.ID); err != nil {
		t.Fatal(err)
	}
	a, err = store.ClaimNextScheduleActivation(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	srv.SetAvailableMemoryForTest(func() (int, error) { return 0, nil })
	checked := false
	srv.SetDeployRunForTest(func(p deploy.Params) (*deploy.PoolResult, error) {
		p.HealthCheck = func(string, time.Duration, http.RoundTripper) error {
			checked = true
			if release, ok := srv.TryAcquireAppOperation(app.Slug); ok {
				release()
				return errors.New("fallback released app lock while serving pool was stopped")
			}
			return nil
		}
		return deploy.Run(p)
	})
	if err := srv.Roll(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if !checked {
		t.Fatal("candidate readiness was not checked")
	}
}

func TestGroupedActivationTerminalOutcomeClearsCandidate(t *testing.T) {
	for _, status := range []string{db.DeploymentPending, db.DeploymentFailed} {
		t.Run(status, func(t *testing.T) {
			srv, store, token, _, _, _ := buildManifestE2EServer(t, config.RuntimeConfig{})
			defer srv.Close()
			app := createGenerationTestApp(t, store, "abandoned-roll", 1, 16)
			_, err := store.DB().Exec("UPDATE apps SET worker_isolation='grouped',worker_grouped_size=4,worker_max_workers=2 WHERE id=?", app.ID)
			if err != nil {
				t.Fatal(err)
			}
			if rec := deployBareGeneration(t, srv, token, app.Slug, "print('v1')", false); rec.Code != 200 {
				t.Fatal(rec.Body.String())
			}
			app, _ = store.GetAppByID(app.ID)
			source, _ := store.GetServingDeployment(app.ID)
			a, err := store.EnqueueRollingRestart(app, source)
			if err != nil {
				t.Fatal(err)
			}
			a, err = store.ClaimNextScheduleActivation(time.Now())
			if err != nil {
				t.Fatal(err)
			}
			candidate, err := store.ActivationDeployment(a.ID, source)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.UpsertDeploymentReplica(db.UpsertDeploymentReplicaParams{AppID: app.ID, DeploymentID: candidate.ID, Index: 99, Status: "starting", Provider: "native"}); err != nil {
				t.Fatal(err)
			}
			if status == db.DeploymentFailed {
				if err := store.FailDeploymentWithReason(candidate.ID, "interrupted candidate"); err != nil {
					t.Fatal(err)
				}
			}
			if blocked, err := store.HasPendingDeployment(app.ID); err != nil || blocked {
				t.Fatalf("same-bundle candidate blocks serving admissions: %v %v", blocked, err)
			}
			if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: app.Slug, Status: "hibernated"}); err != nil {
				t.Fatal(err)
			}
			if err := srv.Roll(context.Background(), a); !errors.Is(err, activation.ErrNotNeeded) {
				t.Fatalf("outcome=%v", err)
			}
			after, err := store.GetDeploymentByID(candidate.ID)
			if err != nil || after.Status != db.DeploymentFailed {
				t.Fatalf("candidate=%+v err=%v", after, err)
			}

			rows, err := store.ListDeploymentReplicas(app.ID)
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range rows {
				if row.DeploymentID == candidate.ID {
					t.Fatal("terminal candidate runtime ledger survived cleanup")
				}
			}

		})
	}
}

func TestInterruptedServingActivationPreservesScheduleEdits(t *testing.T) {
	srv, store, token, _, _, _ := buildManifestE2EServer(t, config.RuntimeConfig{})
	defer srv.Close()
	app := createGenerationTestApp(t, store, "roll-recovery", 1, 16)
	if rec := deployBareGeneration(t, srv, token, app.Slug, "print('v1')", false); rec.Code != 200 {
		t.Fatal(rec.Body.String())
	}
	id, err := store.CreateSchedule(db.CreateScheduleParams{AppID: app.ID, Name: "refresh", CronExpr: "0 5 * * *", CommandJSON: `["true"]`, Enabled: true, TimeoutSeconds: 60, OverlapPolicy: "skip", MissedPolicy: "skip", DeployTrigger: "never", OnSuccess: "none", RollFallback: "defer"})
	if err != nil {
		t.Fatal(err)
	}
	source, _ := store.GetServingDeployment(app.ID)
	a, err := store.EnqueueRollingRestart(app, source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ActivationDeployment(a.ID, source); err != nil {
		t.Fatal(err)
	}
	cron := "0 6 * * *"
	if err := store.UpdateSchedule(id, db.UpdateScheduleParams{CronExpr: &cron}); err != nil {
		t.Fatal(err)
	}
	if err := lifecycle.ReconcileInflightDeployments(store); err != nil {
		t.Fatal(err)
	}
	sc, err := store.GetSchedule(id)
	if err != nil || sc.CronExpr != cron {
		t.Fatalf("schedule edit lost: %+v %v", sc, err)
	}
}

func TestGroupedRollWaitsBeforeCommittingRestartFallback(t *testing.T) {
	srv, store, token, mgr, _, rt := buildManifestE2EServer(t, config.RuntimeConfig{})
	defer srv.Close()
	rt.serveTraffic = true
	rt.holdStarted = make(chan struct{}, 1)
	rt.holdRelease = make(chan struct{})
	defer close(rt.holdRelease)
	app := createGenerationTestApp(t, store, "grouped-fallback-drain", 1, 16)
	if _, err := store.DB().Exec("UPDATE apps SET worker_isolation='grouped',worker_grouped_size=4,worker_max_workers=2 WHERE id=?", app.ID); err != nil {
		t.Fatal(err)
	}
	srv.SetAvailableMemoryForTest(func() (int, error) { return 4096, nil })
	if rec := deployBareGeneration(t, srv, token, app.Slug, "print('v1')", false); rec.Code != 200 {
		t.Fatal(rec.Body.String())
	}
	// Grouped first deploy prepares an idle pool. A first roll supplies its
	// initial ready worker before the held request enters it.
	app, _ = store.GetAppByID(app.ID)
	initial, _ := store.GetServingDeployment(app.ID)
	if _, err := store.EnqueueRollingRestart(app, initial); err != nil {
		t.Fatal(err)
	}
	boot, err := store.ClaimNextScheduleActivation(time.Now())
	if err != nil || boot == nil {
		t.Fatalf("initial claim: %v", err)
	}
	if err := srv.Roll(context.Background(), boot); err != nil {
		t.Fatal(err)
	}
	if err := store.FinishScheduleActivation(boot.ID, "succeeded", "", time.Now(), false); err != nil {
		t.Fatal(err)
	}
	first, _ := store.GetServingDeployment(app.ID)
	firstSlot := -1
	for _, worker := range mgr.AllGenerationsForSlug(app.Slug) {
		if worker.DeploymentID == first.ID {
			firstSlot = worker.Index
			break
		}
	}
	if firstSlot < 0 {
		t.Fatal("initial worker missing")
	}
	go proxyGenerationBody(srv, app.Slug, "?hold=1")
	select {
	case <-rt.holdStarted:
	case <-time.After(time.Second):
		t.Fatal("not held")
	}
	if rec := deployBareGeneration(t, srv, token, app.Slug, "print('v2')", false); rec.Code != 200 {
		t.Fatal(rec.Body.String())
	}
	if !srv.proxy.IsGenerationDraining(app.Slug, first.ID) {
		t.Fatal("first generation not draining")
	}
	app, _ = store.GetAppByID(app.ID)
	current, _ := store.GetServingDeployment(app.ID)
	a, err := store.EnqueueRollingRestart(app, current)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().Exec("UPDATE schedule_activations SET roll_fallback='restart' WHERE id=?", a.ID); err != nil {
		t.Fatal(err)
	}
	a, err = store.ClaimNextScheduleActivation(time.Now())
	if err != nil || a == nil {
		t.Fatalf("claim: %v", err)
	}
	srv.SetAvailableMemoryForTest(func() (int, error) { return 0, nil })
	var waiting *activation.WaitingError
	if err := srv.Roll(context.Background(), a); !errors.As(err, &waiting) {
		t.Fatalf("roll = %v, want waiting", err)
	}
	if waiting.Reason != "stop-first fallback must wait for retained generations" {
		t.Fatalf("wrong wait: %s", waiting.Reason)
	}
	if info, ok := mgr.GetGenerationReplica(app.Slug, first.ID, firstSlot); !ok || info.Status != "running" {
		t.Fatal("fallback stopped retained worker")
	}
	saved, err := store.GetScheduleActivation(a.ID)
	if err != nil {
		t.Fatal(err)
	}
	if saved.Phase == "grouped_restart" {
		t.Fatal("waiting committed disruptive fallback")
	}
}

func TestGroupedActivationYieldsToProducerDuringReadiness(t *testing.T) {
	for _, holdLifecycle := range []bool{false, true} {
		t.Run(fmt.Sprintf("lifecycle_busy=%t", holdLifecycle), func(t *testing.T) {
			var heldLifecycle func()
			srv, store, token, rt := newManifestE2EServerWithJobs(t)
			rt.serveTraffic = true
			block := make(chan struct{})
			var unblockOnce sync.Once
			unblock := func() { unblockOnce.Do(func() { close(block) }) }
			defer func() {
				if heldLifecycle != nil {
					heldLifecycle()
				}
				unblock()
				srv.jobs.Stop(context.Background())
				_ = srv.manager.StopAll()
				srv.Close()
			}()
			srv.cfg.Server.DrainTimeout = 20 * time.Millisecond
			srv.SetAvailableMemoryForTest(func() (int, error) { return 4096, nil })
			app := createGenerationTestApp(t, store, "grouped-producer-collision", 1, 16)
			if _, err := store.DB().Exec("UPDATE apps SET worker_isolation='grouped',worker_grouped_size=4,worker_max_workers=2 WHERE id=?", app.ID); err != nil {
				t.Fatal(err)
			}
			if rec := deployBareGeneration(t, srv, token, app.Slug, "print('v1')", false); rec.Code != 200 {
				t.Fatal(rec.Body.String())
			}
			other := createGenerationTestApp(t, store, "other-activation", 1, 16)
			if rec := deployBareGeneration(t, srv, token, other.Slug, "print('other')", false); rec.Code != 200 {
				t.Fatal(rec.Body.String())
			}
			scheduleID, err := store.CreateSchedule(db.CreateScheduleParams{AppID: app.ID, Name: "refresh", CronExpr: "0 5 * * *", CommandJSON: `["true"]`, Enabled: true, TimeoutSeconds: 60, OverlapPolicy: "skip", MissedPolicy: "skip", DeployTrigger: "bundle_change", OnSuccess: "none"})
			if err != nil {
				t.Fatal(err)
			}
			app, err = store.GetAppByID(app.ID)
			if err != nil {
				t.Fatal(err)
			}
			current, err := store.GetServingDeployment(app.ID)
			if err != nil {
				t.Fatal(err)
			}
			queued, err := store.EnqueueRollingRestart(app, current)
			if err != nil {
				t.Fatal(err)
			}
			otherCurrent, err := store.GetServingDeployment(other.ID)
			if err != nil {
				t.Fatal(err)
			}
			second, err := store.EnqueueRollingRestart(other, otherCurrent)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: other.Slug, Status: "stopped"}); err != nil {
				t.Fatal(err)
			}
			rt.producerBlock = block
			var once sync.Once
			var runID int64
			srv.SetDeployRunForTest(func(p deploy.Params) (*deploy.PoolResult, error) {
				p.HealthCheck = func(string, time.Duration, http.RoundTripper) error {
					var startErr error
					once.Do(func() {
						runID, startErr = srv.jobs.Run(scheduleID, "schedule", nil)
						if holdLifecycle {
							heldLifecycle = srv.acquireDeployLock(app.Slug)
						}
					})
					return startErr
				}
				return deploy.Run(p)
			})
			coordinator := activation.New(store, srv, time.Second)
			completed := make(chan error, 1)
			go func() {
				worked, err := coordinator.ProcessNext(context.Background())
				if !worked && err == nil {
					err = errors.New("activation not claimed")
				}
				completed <- err
			}()
			select {
			case err := <-completed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("grouped activation blocked behind producer")
			}
			if runID == 0 {
				t.Fatal("producer did not fire during readiness")
			}
			stored, err := store.GetScheduleActivation(queued.ID)
			if err != nil || stored.Attempts != 0 || (stored.Status != "pending" && stored.Status != "repairing") {
				t.Fatalf("producer wait consumed an attempt: %+v %v", stored, err)
			}
			if worked, err := coordinator.ProcessNext(context.Background()); !worked || err != nil {
				t.Fatalf("other app could not progress: worked=%t err=%v", worked, err)
			}
			stored, err = store.GetScheduleActivation(second.ID)
			if err != nil || stored.Status != "not_needed" {
				t.Fatalf("other app=%+v err=%v", stored, err)
			}
			if holdLifecycle {
				candidateID, err := store.ActivationDeploymentID(queued.ID)
				if err != nil || candidateID == 0 {
					t.Fatalf("retained candidate=%d err=%v", candidateID, err)
				}
				if inFlight, err := store.ScheduleActivationInFlight(app.ID); err != nil || !inFlight {
					t.Fatalf("retained candidate lost lifecycle fence: %t %v", inFlight, err)
				}
				heldLifecycle()
				heldLifecycle = nil
				unblock()
				deadline := time.Now().Add(3 * time.Second)
				for {
					run, err := store.GetScheduleRun(runID)
					if err != nil {
						t.Fatal(err)
					}
					if run.Status == "succeeded" {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("producer did not finish: %s", run.Status)
					}
					time.Sleep(10 * time.Millisecond)
				}
				// The retained worker would consume this entire surge allowance if
				// admission ran before cleanup on the second claim.
				srv.cfg.Server.UpdateCapacity.MaxSurgeReplicas = groupedCandidateWorkers(app)
				if _, err := store.DB().Exec("UPDATE schedule_activations SET due_at=? WHERE id=?", time.Now().UTC().Add(-time.Second), queued.ID); err != nil {
					t.Fatal(err)
				}
				if worked, err := coordinator.ProcessNext(context.Background()); !worked || err != nil {
					t.Fatalf("reclaim failed: worked=%t err=%v", worked, err)
				}
				old, err := store.GetDeploymentByID(candidateID)
				if err != nil || old.Status != db.DeploymentFailed {
					t.Fatalf("retained candidate was not failed after cleanup: %+v %v", old, err)
				}
				for _, worker := range srv.manager.All() {
					if worker.Slug == app.Slug && worker.DeploymentID == candidateID {
						t.Fatalf("retained candidate worker survived reclaim: %+v", worker)
					}
				}
				stored, err = store.GetScheduleActivation(queued.ID)
				if err != nil || stored.Status != "succeeded" {
					t.Fatalf("reclaimed activation did not progress: %+v %v", stored, err)
				}
			}
		})
	}
}
