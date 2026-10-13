package db_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/schedulespec"
)

func TestProducerRepairAuditFailurePreservesWholePublication(t *testing.T) {
	store := newScheduleStore(t)
	appID := newScheduleAppFixture(t, store, "atomic-producer-repair")
	dep := promoteConvergenceDeployment(t, store, appID, "v1", "sha256:v1")
	schedule := createConvergenceSchedule(t, store, appID, `["producer"]`, schedulespec.DeployTriggerBundleChange)
	if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: "atomic-producer-repair", Status: "running"}); err != nil {
		t.Fatal(err)
	}
	originalRun := insertProducerRun(t, store, schedule, dep, `["producer"]`)
	finishProducerRun(t, store, originalRun, "succeeded")
	failedRun := insertProducerRun(t, store, schedule, dep, `["producer"]`)
	finishProducerRun(t, store, failedRun, "failed")
	repairRun := insertProducerRun(t, store, schedule, dep, `["producer"]`)
	params := db.CompleteScheduleRunParams{RunID: repairRun, Status: "succeeded", FinishedAt: time.Now().UTC()}
	// Fault injection at the database boundary. Observations below use the
	// same public reads as the scheduler, API, and recovery coordinator.
	if _, err := store.DB().Exec(`ALTER TABLE audit_events RENAME TO unavailable_audit_events`); err != nil {
		t.Fatal(err)
	}
	auditUnavailable := true
	defer func() {
		if auditUnavailable {
			if _, err := store.DB().Exec(`ALTER TABLE unavailable_audit_events RENAME TO audit_events`); err != nil {
				t.Errorf("restore audit table: %v", err)
			}
		}
	}()
	if _, err := store.CompleteScheduleRunAndEnqueueActivation(params); err == nil {
		t.Fatal("repair committed without its transition audit")
	}
	assertPublication := func(runID, generation int64, status string, blocked bool) {
		t.Helper()
		publication, err := store.GetAppDataPublication(appID)
		if err != nil || publication.Generation != generation || publication.ScheduleRunID != runID || publication.ProducerDeploymentID == nil || *publication.ProducerDeploymentID != dep.ID || publication.ProducerContentDigest != "sha256:v1" {
			t.Fatalf("publication escaped transaction: %+v err=%v; want run=%d generation=%d", publication, err, runID, generation)
		}
		producer, err := store.GetScheduleProducerState(schedule)
		if err != nil || producer.ScheduleRunID == nil || *producer.ScheduleRunID != runID || producer.PublicationGeneration != generation {
			t.Fatalf("producer state disagrees with publication: %+v err=%v", producer, err)
		}
		app, err := store.GetAppByID(appID)
		if err != nil || app.Status != status {
			t.Fatalf("status escaped transaction: %+v err=%v want=%s", app, err, status)
		}
		quarantined, err := store.AppCompatibilityQuarantined(appID)
		if err != nil || quarantined != blocked {
			t.Fatalf("compatibility fence=%t err=%v want=%t", quarantined, err, blocked)
		}
		if blocked && !strings.Contains(app.LastError, fmt.Sprintf("producer run %d", failedRun)) {
			t.Fatalf("rollback lost original failure diagnosis: %s", app.LastError)
		}
		if !blocked && app.LastError != "" {
			t.Fatalf("repair retained failure diagnosis: %s", app.LastError)
		}
	}
	assertPublication(originalRun, 1, "failed", true)
	run, err := store.GetScheduleRun(repairRun)
	if err != nil || run.Status != "running" {
		t.Fatalf("run completion escaped rollback: %+v err=%v", run, err)
	}
	if _, err := store.DB().Exec(`ALTER TABLE unavailable_audit_events RENAME TO audit_events`); err != nil {
		t.Fatal(err)
	}
	auditUnavailable = false
	for range 3 {
		if _, err := store.CompleteScheduleRunAndEnqueueActivation(params); err != nil {
			t.Fatal(err)
		}
		assertPublication(repairRun, 2, "running", false)
	}
	count, err := store.CountAuditEvents("app_status_changed")
	if err != nil || count != 2 {
		t.Fatalf("retry duplicated transition audit: count=%d err=%v", count, err)
	}
}
