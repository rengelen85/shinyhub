package db_test

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/schedulespec"
)

func TestRunRetentionPreservesActiveJobsAndTheirTerminalOutcome(t *testing.T) {
	for _, publisher := range []bool{false, true} {
		t.Run(fmt.Sprintf("publishes_data=%t", publisher), func(t *testing.T) {
			store := newScheduleStore(t)
			appID := newScheduleAppFixture(t, store, "active-retention")
			deployment := promoteConvergenceDeployment(t, store, appID, "v1", "sha256:v1")
			scheduleID := createConvergenceSchedule(t, store, appID, `["producer"]`, schedulespec.DeployTriggerBundleChange)
			if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: "active-retention", Status: "running"}); err != nil {
				t.Fatal(err)
			}
			canonical, fingerprint, err := schedulespec.ProducerIdentity(`["producer"]`)
			if err != nil {
				t.Fatal(err)
			}
			started := time.Now().UTC().Add(-2 * time.Hour)
			activeID, err := store.InsertScheduleRun(db.InsertScheduleRunParams{
				ScheduleID: scheduleID, Status: "running", Trigger: "manual", StartedAt: started,
				OnSuccess: "none", RollFallback: "defer", DeploymentID: &deployment.ID,
				AppVersion: "v1", ContentDigest: "sha256:v1", ProducerCommandJSON: canonical,
				ProducerFingerprint: fingerprint, PublishesData: publisher,
			})
			if err != nil {
				t.Fatal(err)
			}
			var newestID int64
			// A long-running job can accumulate newer skipped overlap records.
			// Retention must bound those records without erasing the active job.
			for i := 1; i <= 3; i++ {
				newestID, err = store.InsertScheduleRun(db.InsertScheduleRunParams{
					ScheduleID: scheduleID, Status: "running", Trigger: "schedule",
					StartedAt: started.Add(time.Duration(i) * time.Minute), OnSuccess: "none", RollFallback: "defer",
				})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := store.CompleteScheduleRunAndEnqueueActivation(db.CompleteScheduleRunParams{RunID: newestID, Status: "skipped", FinishedAt: started.Add(time.Duration(i)*time.Minute + time.Second)}); err != nil {
					t.Fatal(err)
				}
			}
			removed, err := store.PruneScheduleRuns(scheduleID, 1)
			if err != nil || removed != 2 {
				t.Fatalf("pruning removed active work or retained obsolete history: removed=%d err=%v; want 2", removed, err)
			}
			active, err := store.GetScheduleRun(activeID)
			if err != nil || active.Status != "running" {
				t.Fatalf("retention erased the active job: run=%+v err=%v", active, err)
			}
			newest, err := store.GetScheduleRun(newestID)
			if err != nil || newest.Status != "skipped" {
				t.Fatalf("retention lost newest history: run=%+v err=%v", newest, err)
			}
			if _, err := store.CompleteScheduleRunAndEnqueueActivation(db.CompleteScheduleRunParams{RunID: activeID, Status: "failed", FinishedAt: time.Now().UTC(), DataWriteAttempted: publisher}); err != nil {
				t.Fatalf("retained job cannot persist its terminal result: %v", err)
			}
			app, err := store.GetAppByID(appID)
			wantStatus := "running"
			if publisher {
				wantStatus = "failed"
			}
			if err != nil || app.Status != wantStatus {
				t.Fatalf("retained outcome lost lifecycle effect: app=%+v err=%v; want %s", app, err, wantStatus)
			}
			blocked, err := store.AppCompatibilityQuarantined(appID)
			if err != nil || blocked != publisher {
				t.Fatalf("retained write lost quarantine: blocked=%t err=%v; want %t", blocked, err, publisher)
			}
			if publisher {
				if !strings.Contains(app.LastError, fmt.Sprintf("producer run %d", activeID)) {
					t.Fatalf("retained failure lost diagnosis: %s", app.LastError)
				}
				if _, err := store.PruneScheduleRuns(scheduleID, 1); err != nil {
					t.Fatal(err)
				}
				if _, err := store.GetScheduleRun(activeID); err != nil {
					t.Fatalf("retention erased the failed write before repair: %v", err)
				}
				repairID := insertProducerRun(t, store, scheduleID, deployment, `["producer"]`)
				finishProducerRun(t, store, repairID, "succeeded")
				app, err = store.GetAppByID(appID)
				if err != nil || app.Status != "running" || app.LastError != "" {
					t.Fatalf("retained failure cannot be repaired: app=%+v err=%v", app, err)
				}
			}
		})
	}
}

func TestActiveRunsDoNotConsumeCompletedHistoryAllowance(t *testing.T) {
	store := newScheduleStore(t)
	appID := newScheduleAppFixture(t, store, "retention-budget")
	scheduleID := createConvergenceSchedule(t, store, appID, `["job"]`, schedulespec.DeployTriggerNever)
	started := time.Now().UTC().Add(-5 * time.Hour)
	insert := func(offset time.Duration, terminal bool) int64 {
		t.Helper()
		id, err := store.InsertScheduleRun(db.InsertScheduleRunParams{
			ScheduleID: scheduleID, Status: "running", Trigger: "manual", StartedAt: started.Add(offset),
			OnSuccess: "none", RollFallback: "defer",
		})
		if err != nil {
			t.Fatal(err)
		}
		if terminal {
			if _, err := store.CompleteScheduleRunAndEnqueueActivation(db.CompleteScheduleRunParams{RunID: id, Status: "succeeded", FinishedAt: started.Add(offset + time.Minute)}); err != nil {
				t.Fatal(err)
			}
		}
		return id
	}
	oldActive := insert(0, false)
	oldCompleted := insert(time.Hour, true)
	middleCompleted := insert(2*time.Hour, true)
	newCompleted := insert(3*time.Hour, true)
	newActive := insert(4*time.Hour, false)
	// The public retention contract keeps two completed records in addition
	// to active jobs, whether those jobs sort before or after the history.
	removed, err := store.PruneScheduleRuns(scheduleID, 2)
	if err != nil || removed != 1 {
		t.Fatalf("active jobs consumed completed history allowance: removed=%d err=%v; want 1", removed, err)
	}
	for _, id := range []int64{oldActive, newActive} {
		run, err := store.GetScheduleRun(id)
		if err != nil || run.Status != "running" {
			t.Fatalf("active job %d lost: run=%+v err=%v", id, run, err)
		}
	}
	for _, id := range []int64{middleCompleted, newCompleted} {
		run, err := store.GetScheduleRun(id)
		if err != nil || run.Status != "succeeded" {
			t.Fatalf("completed history %d lost: run=%+v err=%v", id, run, err)
		}
	}
	if _, err := store.GetScheduleRun(oldCompleted); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("old completed history was not pruned: %v", err)
	}
	if removed, err := store.PruneScheduleRuns(scheduleID, 2); err != nil || removed != 0 {
		t.Fatalf("repeated pruning is not stable: removed=%d err=%v", removed, err)
	}
}

func TestConcurrentRetentionAndFailedPublicationPreserveRepairEvidence(t *testing.T) {
	store := newScheduleStore(t)
	appID := newScheduleAppFixture(t, store, "retention-completion")
	deployment := promoteConvergenceDeployment(t, store, appID, "v1", "sha256:v1")
	scheduleID := createConvergenceSchedule(t, store, appID, `["producer"]`, schedulespec.DeployTriggerBundleChange)
	if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: "retention-completion", Status: "running"}); err != nil {
		t.Fatal(err)
	}
	// Keep a newer overlap record so each physical writer is outside the
	// configured history allowance, regardless of which operation wins.
	overlapID, err := store.InsertScheduleRun(db.InsertScheduleRunParams{
		ScheduleID: scheduleID, Status: "running", Trigger: "schedule", StartedAt: time.Now().UTC().Add(time.Hour),
		OnSuccess: "none", RollFallback: "defer",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteScheduleRunAndEnqueueActivation(db.CompleteScheduleRunParams{RunID: overlapID, Status: "skipped", FinishedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	for range 8 {
		runID := insertProducerRun(t, store, scheduleID, deployment, `["producer"]`)
		start := make(chan struct{})
		results := make(chan error, 2)
		var ready sync.WaitGroup
		ready.Add(2)
		go func() {
			ready.Done()
			<-start
			_, err := store.CompleteScheduleRunAndEnqueueActivation(db.CompleteScheduleRunParams{RunID: runID, Status: "failed", FinishedAt: time.Now().UTC(), DataWriteAttempted: true})
			results <- err
		}()
		go func() {
			ready.Done()
			<-start
			_, err := store.PruneScheduleRuns(scheduleID, 1)
			results <- err
		}()
		ready.Wait()
		close(start)
		for range 2 {
			select {
			case err := <-results:
				if err != nil {
					t.Fatalf("maintenance raced away a terminal target: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("maintenance and publication did not finish")
			}
		}
		run, err := store.GetScheduleRun(runID)
		if err != nil || run.Status != "failed" {
			t.Fatalf("maintenance lost failed writer: run=%+v err=%v", run, err)
		}
		app, err := store.GetAppByID(appID)
		if err != nil || app.Status != "failed" || !strings.Contains(app.LastError, fmt.Sprintf("producer run %d", runID)) {
			t.Fatalf("maintenance lost repair diagnosis: app=%+v err=%v", app, err)
		}
		if blocked, err := store.AppCompatibilityQuarantined(appID); err != nil || !blocked {
			t.Fatalf("maintenance cleared failed-write quarantine: blocked=%t err=%v", blocked, err)
		}
	}
	repairID := insertProducerRun(t, store, scheduleID, deployment, `["producer"]`)
	finishProducerRun(t, store, repairID, "succeeded")
	app, err := store.GetAppByID(appID)
	if err != nil || app.Status != "running" || app.LastError != "" {
		t.Fatalf("concurrent retention made failure irreparable: app=%+v err=%v", app, err)
	}
}
