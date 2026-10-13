package db_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/schedulespec"
)

func TestProducerDiagnosticIdentifiesRemainingFailure(t *testing.T) {
	for _, repairFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("repair-first=%t", repairFirst), func(t *testing.T) {
			store := newScheduleStore(t)
			id := newScheduleAppFixture(t, store, "diagnostics")
			dep := promoteConvergenceDeployment(t, store, id, "v1", "sha256:v1")
			first := createConvergenceSchedule(t, store, id, `["first"]`, schedulespec.DeployTriggerBundleChange)
			second, err := store.CreateSchedule(db.CreateScheduleParams{AppID: id, Name: "second", CronExpr: "0 6 * * *", CommandJSON: `["second"]`, Enabled: true, DeployTrigger: schedulespec.DeployTriggerBundleChange, OnSuccess: "none", RollFallback: "defer"})
			if err != nil {
				t.Fatal(err)
			}
			if err = store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: "diagnostics", Status: "running"}); err != nil {
				t.Fatal(err)
			}
			a := insertProducerRun(t, store, first, dep, `["first"]`)
			finishProducerRun(t, store, a, "failed")
			b := insertProducerRun(t, store, second, dep, `["second"]`)
			finishProducerRun(t, store, b, "failed")
			app, err := store.GetAppByID(id)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(app.LastError, fmt.Sprintf("producer run %d (second)", b)) {
				t.Fatalf("latest failure absent from diagnostic: %s", app.LastError)
			}
			repaired, remaining, command, name := first, b, `["first"]`, "second"
			if !repairFirst {
				repaired, remaining, command, name = second, a, `["second"]`, "producer"
			}
			finishProducerRun(t, store, insertProducerRun(t, store, repaired, dep, command), "succeeded")
			if err := store.EnforceCompatibilityQuarantines(); err != nil {
				t.Fatal(err)
			}
			app, err = store.GetAppByID(id)
			if err != nil {
				t.Fatal(err)
			}
			if app.Status != "failed" || !strings.Contains(app.LastError, fmt.Sprintf("producer run %d (%s)", remaining, name)) {
				t.Fatalf("diagnostic does not identify remaining failure: status=%s error=%s", app.Status, app.LastError)
			}
		})
	}
}

func TestProducerDiagnosticTracksRunningWriterThenClearsOnCancellation(t *testing.T) {
	store := newScheduleStore(t)
	id := newScheduleAppFixture(t, store, "running-diagnostic")
	dep := promoteConvergenceDeployment(t, store, id, "v1", "sha256:v1")
	first := createConvergenceSchedule(t, store, id, `["first"]`, schedulespec.DeployTriggerBundleChange)
	second, err := store.CreateSchedule(db.CreateScheduleParams{AppID: id, Name: "second", CronExpr: "0 6 * * *", CommandJSON: `["second"]`, Enabled: true, DeployTrigger: schedulespec.DeployTriggerBundleChange, OnSuccess: "none", RollFallback: "defer"})
	if err != nil {
		t.Fatal(err)
	}
	if err = store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: "running-diagnostic", Status: "running"}); err != nil {
		t.Fatal(err)
	}
	finishProducerRun(t, store, insertProducerRun(t, store, first, dep, `["first"]`), "failed")
	running := insertProducerRun(t, store, second, dep, `["second"]`)
	finishProducerRun(t, store, insertProducerRun(t, store, first, dep, `["first"]`), "succeeded")
	app, err := store.GetAppByID(id)
	if err != nil {
		t.Fatal(err)
	}
	if app.Status != "failed" || !strings.Contains(app.LastError, fmt.Sprintf("producer run %d (second) is still running", running)) {
		t.Fatalf("running blocker not described: %+v", app)
	}
	if _, err := store.CompleteScheduleRunAndEnqueueActivation(db.CompleteScheduleRunParams{RunID: running, Status: "cancelled", FinishedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	app, err = store.GetAppByID(id)
	if err != nil || app.Status != "running" || app.LastError != "" {
		t.Fatalf("cancelled non-writer did not clear status and diagnostic: %+v %v", app, err)
	}
}

func TestProducerDiagnosticPreservesIndependentLifecycleReasons(t *testing.T) {
	for _, status := range []string{"failed", "stopped", "deleting"} {
		t.Run(status, func(t *testing.T) {
			store := newScheduleStore(t)
			id := newScheduleAppFixture(t, store, "independent-"+status)
			dep := promoteConvergenceDeployment(t, store, id, "v1", "sha256:v1")
			schedule := createConvergenceSchedule(t, store, id, `["producer"]`, schedulespec.DeployTriggerBundleChange)
			if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: "independent-" + status, Status: status, LastError: "independent lifecycle reason"}); err != nil {
				t.Fatal(err)
			}
			finishProducerRun(t, store, insertProducerRun(t, store, schedule, dep, `["producer"]`), "failed")
			finishProducerRun(t, store, insertProducerRun(t, store, schedule, dep, `["producer"]`), "succeeded")
			app, err := store.GetAppByID(id)
			if err != nil || app.Status != status || app.LastError != "independent lifecycle reason" {
				t.Fatalf("producer overwrote independent status/diagnostic: %+v %v", app, err)
			}
		})
	}
}

func TestProducerFailureInvalidatesDrainingAndIndependentFailedConsumers(t *testing.T) {
	for _, initial := range []string{"running", "failed"} {
		t.Run(initial, func(t *testing.T) {
			store := newScheduleStore(t)
			id := newScheduleAppFixture(t, store, "audit-consumers-"+initial)
			dep := promoteConvergenceDeployment(t, store, id, "v1", "sha256:v1")
			schedule := createConvergenceSchedule(t, store, id, `["producer"]`, schedulespec.DeployTriggerBundleChange)
			if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: "audit-consumers-" + initial, Status: initial}); err != nil {
				t.Fatal(err)
			}
			pid := 12345
			for index, status := range []string{"running", "draining"} {
				if err := store.UpsertReplica(db.UpsertReplicaParams{AppID: id, Index: index, PID: &pid, Status: status, Provider: "native", DesiredState: "running", DeploymentID: &dep.ID}); err != nil {
					t.Fatal(err)
				}
			}
			finishProducerRun(t, store, insertProducerRun(t, store, schedule, dep, `["producer"]`), "failed")
			replicas, err := store.ListReplicas(id)
			if err != nil {
				t.Fatal(err)
			}
			for _, replica := range replicas {
				if replica.Status != "stopped" {
					t.Errorf("quarantined slot %d retained serving status %s", replica.Index, replica.Status)
				}
				if replica.PID == nil || *replica.PID != pid || replica.DesiredState != "running" {
					t.Errorf("unconfirmed stop lost identity or intent: %+v", replica)
				}
			}
		})
	}
}
