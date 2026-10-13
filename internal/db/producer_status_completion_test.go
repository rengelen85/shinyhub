package db_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/schedulespec"
)

func TestProducerRecoveryAfterNonWritingBlockerFinishes(t *testing.T) {
	for _, initial := range []string{"running", "hibernated"} {
		for _, terminal := range []string{"cancelled", "interrupted", "failed", "superseded"} {
			t.Run(initial+"/"+terminal, func(t *testing.T) {
				store, appID, queued := repairedProducerWithQueuedBlocker(t, initial)
				before, err := store.GetAppDataPublication(appID)
				if err != nil {
					t.Fatal(err)
				}
				params := db.CompleteScheduleRunParams{RunID: queued, Status: terminal, FinishedAt: time.Now().UTC(), DataWriteAttempted: false}
				for range 2 { // Duplicate completions must not duplicate transitions.
					if _, err := store.CompleteScheduleRunAndEnqueueActivation(params); err != nil {
						t.Fatal(err)
					}
				}
				blocked, err := store.AppCompatibilityQuarantined(appID)
				if err != nil || blocked {
					t.Fatalf("no compatibility blocker should remain: %v %v", blocked, err)
				}
				app, err := store.GetAppByID(appID)
				if err != nil {
					t.Fatal(err)
				}
				if app.Status != initial || app.LastError != "" {
					t.Fatalf("fully repaired app stayed unavailable: %+v", app)
				}
				var sequence int64
				if err := store.DB().QueryRow(`SELECT data_write_sequence FROM schedule_runs WHERE id=?`, queued).Scan(&sequence); err != nil || sequence != 0 {
					t.Fatalf("non-writer was recorded as a physical write: %d %v", sequence, err)
				}
				after, err := store.GetAppDataPublication(appID)
				if err != nil || after.Generation != before.Generation || after.ScheduleRunID != before.ScheduleRunID {
					t.Fatalf("non-writer changed publication: %+v %v", after, err)
				}
				var count int
				if err := store.DB().QueryRow(`SELECT COUNT(*) FROM audit_events WHERE action='app_status_changed'`).Scan(&count); err != nil || count != 2 {
					t.Fatalf("transition count=%d err=%v", count, err)
				}
				var raw string
				if err := store.DB().QueryRow(`SELECT detail FROM audit_events WHERE action='app_status_changed' ORDER BY id DESC LIMIT 1`).Scan(&raw); err != nil {
					t.Fatal(err)
				}
				var detail map[string]any
				if err := json.Unmarshal([]byte(raw), &detail); err != nil {
					t.Fatal(err)
				}
				if detail["schedule_run_id"] != float64(queued) || detail["to"] != initial || detail["reason"] != "producer_repair_succeeded" {
					t.Fatalf("missing recovery provenance: %s", raw)
				}
			})
		}
	}
}

func repairedProducerWithQueuedBlocker(t *testing.T, initial string) (*db.Store, int64, int64) {
	t.Helper()
	store := newScheduleStore(t)
	appID := newScheduleAppFixture(t, store, "queued-repair")
	dep := promoteConvergenceDeployment(t, store, appID, "v1", "sha256:v1")
	schedule := createConvergenceSchedule(t, store, appID, `["producer"]`, schedulespec.DeployTriggerBundleChange)
	if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: "queued-repair", Status: initial}); err != nil {
		t.Fatal(err)
	}
	finishProducerRun(t, store, insertProducerRun(t, store, schedule, dep, `["producer"]`), "failed")
	repair := insertProducerRun(t, store, schedule, dep, `["producer"]`)
	queued := insertProducerRun(t, store, schedule, dep, `["producer"]`)
	finishProducerRun(t, store, repair, "succeeded")
	app, err := store.GetAppByID(appID)
	if err != nil {
		t.Fatal(err)
	}
	if app.Status != "failed" {
		t.Fatalf("queued publisher should still block: %+v", app)
	}
	return store, appID, queued
}

func TestNonWritingCompletionPreservesIndependentIntentAndBlockers(t *testing.T) {
	for _, mode := range []string{"operator_stop", "other_failed_producer", "audit_failure"} {
		t.Run(mode, func(t *testing.T) {
			store, appID, queued := repairedProducerWithQueuedBlocker(t, "running")
			want := "failed"
			switch mode {
			case "operator_stop":
				if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: "queued-repair", Status: "stopped"}); err != nil {
					t.Fatal(err)
				}
				want = "stopped"
			case "other_failed_producer":
				schedule, err := store.CreateSchedule(db.CreateScheduleParams{AppID: appID, Name: "second", CronExpr: "0 6 * * *", CommandJSON: `["second"]`, Enabled: true, DeployTrigger: schedulespec.DeployTriggerBundleChange, OnSuccess: "none", RollFallback: "defer"})
				if err != nil {
					t.Fatal(err)
				}
				dep, err := store.GetServingDeployment(appID)
				if err != nil {
					t.Fatal(err)
				}
				finishProducerRun(t, store, insertProducerRun(t, store, schedule, dep, `["second"]`), "failed")
			case "audit_failure":
				if _, err := store.DB().Exec(`ALTER TABLE audit_events RENAME TO unavailable_audit_events`); err != nil {
					t.Fatal(err)
				}
			}
			_, err := store.CompleteScheduleRunAndEnqueueActivation(db.CompleteScheduleRunParams{RunID: queued, Status: "cancelled", FinishedAt: time.Now().UTC()})
			if mode == "audit_failure" {
				if err == nil {
					t.Fatal("recovery succeeded without an audit")
				}
				run, err := store.GetScheduleRun(queued)
				if err != nil || run.Status != "running" {
					t.Fatalf("completion escaped rollback: %+v %v", run, err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			app, err := store.GetAppByID(appID)
			if err != nil {
				t.Fatal(err)
			}
			if app.Status != want {
				t.Fatalf("independent intent/blocker was overridden: %+v", app)
			}
		})
	}
}
