package db_test

import (
	"errors"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/db"
)

func TestManualRestartsQueueIndependentlyBehindRunningActivation(t *testing.T) {
	store := newScheduleStore(t)
	appID := newScheduleAppFixture(t, store, "manual-roll-queue")
	app, _ := store.GetAppByID(appID)
	dep := promoteConvergenceDeployment(t, store, appID, "v1", "digest-v1")
	first, err := store.EnqueueRollingRestart(app, dep)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimNextScheduleActivation(time.Now())
	if err != nil || claimed.ID != first.ID {
		t.Fatalf("claim=%+v %v", claimed, err)
	}
	second, err := store.EnqueueRollingRestart(app, dep)
	if err != nil {
		t.Fatal(err)
	}
	if second.ID == first.ID || second.Status != "pending" {
		t.Fatal("manual restart reused an execution which may have read stale environment")
	}
	if err := store.FinishScheduleActivation(first.ID, "succeeded", "", time.Now(), false); err != nil {
		t.Fatal(err)
	}
	claimed, err = store.ClaimNextScheduleActivation(time.Now())
	if err != nil || claimed.ID != second.ID {
		t.Fatalf("queued restart was lost: %+v %v", claimed, err)
	}
}

func TestGenerationDrainWaitDoesNotConsumeRolloutAttempts(t *testing.T) {
	store := newScheduleStore(t)
	appID := newScheduleAppFixture(t, store, "manual-roll-long-drain")
	app, _ := store.GetAppByID(appID)
	dep := promoteConvergenceDeployment(t, store, appID, "v1", "digest-v1")
	a, err := store.EnqueueRollingRestart(app, dep)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for i := 0; i < 5; i++ {
		claimed, err := store.ClaimNextScheduleActivation(now)
		if err != nil || claimed.ID != a.ID {
			t.Fatalf("claim=%+v %v", claimed, err)
		}
		now = now.Add(time.Minute)
		if err := store.WaitScheduleActivation(a.ID, "previous generation is still draining", now); err != nil {
			t.Fatal(err)
		}
	}
	a, err = store.GetScheduleActivation(a.ID)
	if err != nil || a.Attempts != 0 || a.CapacityDeferrals != 0 || a.Status != "pending" {
		t.Fatalf("waiting consumed attempts: %+v %v", a, err)
	}
}

func TestGenerationDrainWaitPreservesRecoveryFence(t *testing.T) {
	for _, phase := range []string{"recovering", "grouped_restart"} {
		t.Run(phase, func(t *testing.T) {
			store := newScheduleStore(t)
			appID := newScheduleAppFixture(t, store, "recover-wait")
			app, _ := store.GetAppByID(appID)
			dep := promoteConvergenceDeployment(t, store, appID, "v1", "digest-v1")
			a, err := store.EnqueueRollingRestart(app, dep)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now()
			if _, err := store.ClaimNextScheduleActivation(now); err != nil {
				t.Fatal(err)
			}
			if _, err := store.RequeueRunningScheduleActivations(now); err != nil {
				t.Fatal(err)
			}
			claimed, err := store.ClaimNextScheduleActivation(now)
			if err != nil || claimed.Phase != "recovering" {
				t.Fatalf("reclaim=%+v %v", claimed, err)
			}
			if phase == "grouped_restart" {
				if err := store.UpdateScheduleActivationProgress(a.ID, phase, -1, 0); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.WaitScheduleActivation(a.ID, "retiring parent", now); err != nil {
				t.Fatal(err)
			}
			waiting, err := store.GetScheduleActivation(a.ID)
			if err != nil || waiting.Status != "repairing" || waiting.Phase != phase {
				t.Fatalf("wait=%+v %v", waiting, err)
			}
			if inFlight, err := store.ScheduleActivationInFlight(appID); err != nil || !inFlight {
				t.Fatalf("recovery fence=%v %v", inFlight, err)
			}
			claimed, err = store.ClaimNextScheduleActivation(now)
			if err != nil || claimed.Phase != phase {
				t.Fatalf("reclaim after wait=%+v %v", claimed, err)
			}
		})
	}
}

func TestManualRestartCoalescesOnlyUnstartedWork(t *testing.T) {
	store := newScheduleStore(t)
	appID := newScheduleAppFixture(t, store, "manual-roll-coalesce")
	app, _ := store.GetAppByID(appID)
	dep := promoteConvergenceDeployment(t, store, appID, "v1", "digest-v1")
	first, err := store.EnqueueRollingRestart(app, dep)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.EnqueueRollingRestart(app, dep)
	if err != nil || second.ID != first.ID {
		t.Fatalf("unstarted request not coalesced: %+v %v", second, err)
	}
	if _, err := store.ClaimNextScheduleActivation(time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.WaitScheduleActivation(first.ID, "draining", time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	third, err := store.EnqueueRollingRestart(app, dep)
	if err != nil || third.ID == first.ID {
		t.Fatalf("started request was incorrectly reused: %+v %v", third, err)
	}
	if _, err := store.ClaimNextScheduleActivation(time.Now()); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("new activation overtook waiting predecessor: %v", err)
	}
	if _, err := store.ClaimNextScheduleActivation(time.Now().Add(2 * time.Hour)); err != nil {
		t.Fatal(err)
	}
}

func TestManualRestartSupportsLegacyServingFallback(t *testing.T) {
	store := newScheduleStore(t)
	appID := newScheduleAppFixture(t, store, "manual-roll-legacy")
	app, _ := store.GetAppByID(appID)
	dep := promoteConvergenceDeployment(t, store, appID, "v1", "digest-v1")
	if _, err := store.DB().Exec("UPDATE apps SET active_deployment_id=NULL WHERE id=?", appID); err != nil {
		t.Fatal(err)
	}
	a, err := store.EnqueueRollingRestart(app, dep)
	if err != nil || a.SourceDeploymentID == nil || *a.SourceDeploymentID != dep.ID {
		t.Fatalf("legacy enqueue=%+v %v", a, err)
	}
}

func TestManualRestartRejectsStaleServingTarget(t *testing.T) {
	store := newScheduleStore(t)
	appID := newScheduleAppFixture(t, store, "manual-roll-stale")
	app, _ := store.GetAppByID(appID)
	old := promoteConvergenceDeployment(t, store, appID, "v1", "digest-v1")
	promoteConvergenceDeployment(t, store, appID, "v2", "digest-v2")
	if _, err := store.EnqueueRollingRestart(app, old); !errors.Is(err, db.ErrRollingRestartTargetChanged) {
		t.Fatalf("stale enqueue=%v", err)
	}
	rows, err := store.ListScheduleActivationsByApp(appID, 50)
	if err != nil || len(rows) != 0 {
		t.Fatalf("stale request left queued work: %+v %v", rows, err)
	}
}

func TestManualRestartIsNeitherCoalescedNorScheduledRollDamper(t *testing.T) {
	store := newScheduleStore(t)
	appID := newScheduleAppFixture(t, store, "manual-roll-damper")
	app, err := store.GetAppByID(appID)
	if err != nil {
		t.Fatal(err)
	}
	dep := promoteConvergenceDeployment(t, store, appID, "v1", "digest-v1")
	manual, err := store.EnqueueRollingRestart(app, dep)
	if err != nil {
		t.Fatal(err)
	}
	id, err := store.CreateSchedule(db.CreateScheduleParams{AppID: appID, Name: "refresh", CronExpr: "0 5 * * *", CommandJSON: `["true"]`, Enabled: true, TimeoutSeconds: 60, OverlapPolicy: "skip", MissedPolicy: "skip", OnSuccess: "roll", MinRollIntervalSeconds: 3600})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Add(time.Second)
	run := insertActivationRun(t, store, id, now.Add(-time.Minute), "roll", 3600)
	scheduled, err := store.CompleteScheduleRunAndEnqueueActivation(db.CompleteScheduleRunParams{RunID: run, Status: "succeeded", ExitCode: intPtr(0), FinishedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	current, err := store.GetScheduleActivation(manual.ID)
	if err != nil || current.Status != "pending" {
		t.Fatalf("manual restart superseded: %+v %v", current, err)
	}
	claimed, err := store.ClaimNextScheduleActivation(now)
	if err != nil || claimed.ID != manual.ID {
		t.Fatalf("claim=%+v %v", claimed, err)
	}
	if err := store.FinishScheduleActivation(manual.ID, "succeeded", "", now, false); err != nil {
		t.Fatal(err)
	}
	after, err := store.GetScheduleActivation(scheduled.ID)
	if err != nil || !after.DueAt.Equal(scheduled.DueAt) {
		t.Fatalf("manual restart damped scheduled roll: %+v %v", after, err)
	}
}

func TestTerminalServingCandidateWithoutRuntimeIsReaped(t *testing.T) {
	store := newScheduleStore(t)
	appID := newScheduleAppFixture(t, store, "terminal-roll")
	app, _ := store.GetAppByID(appID)
	dep := promoteConvergenceDeployment(t, store, appID, "v1", "digest-v1")
	a, err := store.EnqueueRollingRestart(app, dep)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := store.ActivationDeployment(a.ID, dep)
	if err != nil {
		t.Fatal(err)
	}
	a, err = store.ClaimNextScheduleActivation(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.FinishScheduleActivation(a.ID, "failed", "deadline expired", time.Now(), false); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNextScheduleActivation(time.Now()); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("claim=%v", err)
	}
	after, err := store.GetDeploymentByID(candidate.ID)
	if err != nil || after.Status != db.DeploymentFailed {
		t.Fatalf("candidate=%+v %v", after, err)
	}
}

func TestServingActivationPreservesProducerConvergenceAdmission(t *testing.T) {
	store := newScheduleStore(t)
	appID := newScheduleAppFixture(t, store, "serving-producer-convergence")
	app, _ := store.GetAppByID(appID)
	dep := promoteConvergenceDeployment(t, store, appID, "v1", "digest-v1")
	createConvergenceSchedule(t, store, appID, `["true"]`, "bundle_change")
	if _, err := store.ReconcileDeployObligationsForDeployment(appID, dep.ID); err != nil {
		t.Fatal(err)
	}
	a, err := store.EnqueueRollingRestart(app, dep)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := store.ActivationDeployment(a.ID, dep)
	if err != nil {
		t.Fatal(err)
	}
	// A serving-only candidate does not change the immutable producer bundle.
	claimed, err := store.ClaimNextDeployObligation()
	if err != nil || claimed.DeploymentID != dep.ID {
		t.Fatalf("pending serving candidate blocked producer: %+v %v", claimed, err)
	}
	if err := store.PromoteDeployment(candidate.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileAllDeployObligations(); err != nil {
		t.Fatal(err)
	}
	claimed, err = store.ClaimNextDeployObligation()
	if err != nil || claimed.DeploymentID != candidate.ID {
		t.Fatalf("convergence missed serving authority: %+v %v", claimed, err)
	}
}

func TestGroupedCandidateRetainsRuntimeOwnership(t *testing.T) {
	for _, action := range []string{"roll", "none"} {
		t.Run(action, func(t *testing.T) {
			store := newScheduleStore(t)
			appID := newScheduleAppFixture(t, store, "retained-grouped")
			dep := promoteConvergenceDeployment(t, store, appID, "v1", "digest-v1")
			scheduleID := createConvergenceSchedule(t, store, appID, `["true"]`, "bundle_change")
			now := time.Now().UTC().Truncate(time.Second)
			run := insertActivationRun(t, store, scheduleID, now.Add(-time.Minute), "roll", 0)
			a, err := store.CompleteScheduleRunAndEnqueueActivation(db.CompleteScheduleRunParams{RunID: run, Status: "succeeded", ExitCode: intPtr(0), FinishedAt: now})
			if err != nil || a == nil {
				t.Fatalf("activation=%+v %v", a, err)
			}
			candidate, err := store.ActivationDeployment(a.ID, dep)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.UpsertDeploymentReplica(db.UpsertDeploymentReplicaParams{AppID: appID, DeploymentID: candidate.ID, Index: 0, Status: "starting"}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.ClaimNextScheduleActivation(now); err != nil {
				t.Fatal(err)
			}
			if err := store.UpdateScheduleActivationProgress(a.ID, "recovering", -1, 0); err != nil {
				t.Fatal(err)
			}
			if err := store.DeferScheduleActivation(a.ID, "deferred_capacity", "tight host", now, now); err != nil {
				t.Fatal(err)
			}
			stored, err := store.GetScheduleActivation(a.ID)
			if err != nil || stored.Status != "repairing" || stored.Phase != "recovering" || stored.Attempts != 0 {
				t.Fatalf("runtime owner lost on capacity defer: %+v %v", stored, err)
			}
			// Also protect a durable worker if an older server left the owner
			// pending: in-flight checks, coalescing and repair-only claims must
			// all recognize grouped runtime without a canonical replicas row.
			if _, err := store.DB().Exec("UPDATE schedule_activations SET status='pending' WHERE id=?", a.ID); err != nil {
				t.Fatal(err)
			}
			if inFlight, err := store.ScheduleActivationInFlight(appID); err != nil || !inFlight {
				t.Fatalf("runtime lost lifecycle fence: %t %v", inFlight, err)
			}
			run = insertActivationRun(t, store, scheduleID, now, action, 0)
			if _, err := store.CompleteScheduleRunAndEnqueueActivation(db.CompleteScheduleRunParams{RunID: run, Status: "succeeded", ExitCode: intPtr(0), FinishedAt: now.Add(time.Second)}); err != nil {
				t.Fatal(err)
			}
			stored, err = store.GetScheduleActivation(a.ID)
			if err != nil || stored.Status != "pending" {
				t.Fatalf("runtime owner coalesced away: %+v %v", stored, err)
			}
			claimed, err := store.ClaimNextScheduleActivationRepair(now.Add(time.Minute))
			if err != nil || claimed.ID != a.ID {
				t.Fatalf("grouped runtime not reclaimed for repair: %+v %v", claimed, err)
			}
		})
	}
}
