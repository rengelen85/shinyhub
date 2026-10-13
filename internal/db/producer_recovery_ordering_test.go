package db_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/schedulespec"
)

func TestProducerRecoveryPreservesIntentAcrossEventOrders(t *testing.T) {
	// These expected states come from the lifecycle contract: both producers
	// must repair, and an explicit stop always wins over automatic recovery.
	for _, tc := range []struct {
		events      []string
		statuses    []string
		transitions int64
	}{
		{[]string{"repair-a", "repair-b"}, []string{"failed", "running"}, 2},
		{[]string{"repair-b", "repair-a"}, []string{"failed", "running"}, 2},
		{[]string{"stop", "repair-a", "repair-b"}, []string{"stopped", "stopped", "stopped"}, 1},
		{[]string{"stop", "repair-b", "repair-a"}, []string{"stopped", "stopped", "stopped"}, 1},
		{[]string{"repair-a", "stop", "repair-b"}, []string{"failed", "stopped", "stopped"}, 1},
		{[]string{"repair-b", "stop", "repair-a"}, []string{"failed", "stopped", "stopped"}, 1},
		{[]string{"repair-a", "repair-b", "stop"}, []string{"failed", "running", "stopped"}, 2},
		{[]string{"repair-b", "repair-a", "stop"}, []string{"failed", "running", "stopped"}, 2},
	} {
		t.Run(strings.Join(tc.events, "/"), func(t *testing.T) {
			store := newScheduleStore(t)
			appID := newScheduleAppFixture(t, store, "ordered-repair")
			dep := promoteConvergenceDeployment(t, store, appID, "v1", "sha256:v1")
			schedules := map[string]int64{}
			for _, name := range []string{"a", "b"} {
				id, err := store.CreateSchedule(db.CreateScheduleParams{AppID: appID, Name: name, CronExpr: "0 5 * * *", CommandJSON: fmt.Sprintf(`["%s"]`, name), Enabled: true, DeployTrigger: schedulespec.DeployTriggerBundleChange, OnSuccess: "none", RollFallback: "defer"})
				if err != nil {
					t.Fatal(err)
				}
				schedules[name] = id
			}
			if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: "ordered-repair", Status: "running"}); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"a", "b"} {
				finishProducerRun(t, store, insertProducerRun(t, store, schedules[name], dep, fmt.Sprintf(`["%s"]`, name)), "failed")
			}
			repairs := map[string]int64{}
			for _, name := range []string{"a", "b"} {
				repairs[name] = insertProducerRun(t, store, schedules[name], dep, fmt.Sprintf(`["%s"]`, name))
			}
			completed := map[string]bool{}
			generation := int64(0)
			for i, event := range tc.events {
				if event == "stop" {
					if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: "ordered-repair", Status: "stopped", LastError: "operator stopped app"}); err != nil {
						t.Fatal(err)
					}
				} else {
					name := strings.TrimPrefix(event, "repair-")
					params := db.CompleteScheduleRunParams{RunID: repairs[name], Status: "succeeded", FinishedAt: time.Now().UTC()}
					// Replay terminal notifications, including after startup
					// reconciliation, to expose duplicate side effects.
					for range 3 {
						if _, err := store.CompleteScheduleRunAndEnqueueActivation(params); err != nil {
							t.Fatal(err)
						}
						// Check before startup reconciliation can hide an early
						// recovery by quarantining the app again.
						app, err := store.GetAppByID(appID)
						if err != nil || app.Status != tc.statuses[i] {
							t.Fatalf("immediately after %s: app=%+v err=%v; want %s", event, app, err, tc.statuses[i])
						}
						if err := store.EnforceCompatibilityQuarantines(); err != nil {
							t.Fatal(err)
						}
					}
					completed[name] = true
					generation++
					publication, err := store.GetAppDataPublication(appID)
					if err != nil || publication.Generation != generation || publication.ScheduleRunID != repairs[name] {
						t.Fatalf("%s duplicate completion changed publication: %+v err=%v", event, publication, err)
					}
				}
				if err := store.ReconcileAllDeployObligations(); err != nil {
					t.Fatal(err)
				}
				if err := store.EnforceCompatibilityQuarantines(); err != nil {
					t.Fatal(err)
				}
				app, err := store.GetAppByID(appID)
				if err != nil || app.Status != tc.statuses[i] {
					t.Fatalf("after %s: app=%+v err=%v; want %s", event, app, err, tc.statuses[i])
				}
				blocked, err := store.AppCompatibilityQuarantined(appID)
				if err != nil || blocked != !(completed["a"] && completed["b"]) {
					t.Fatalf("after %s: blocked=%t err=%v", event, blocked, err)
				}
				if app.Status == "stopped" && app.LastError != "operator stopped app" {
					t.Fatalf("automatic repair overwrote operator reason: %s", app.LastError)
				}
			}
			count, err := store.CountAuditEvents("app_status_changed")
			if err != nil || count != tc.transitions {
				t.Fatalf("transition count=%d err=%v want=%d", count, err, tc.transitions)
			}
		})
	}
}

func TestConcurrentOperatorStopWinsProducerRepair(t *testing.T) {
	store := newScheduleStore(t)
	appID := newScheduleAppFixture(t, store, "concurrent-stop-repair")
	dep := promoteConvergenceDeployment(t, store, appID, "v1", "sha256:v1")
	schedule := createConvergenceSchedule(t, store, appID, `["producer"]`, schedulespec.DeployTriggerBundleChange)
	if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: "concurrent-stop-repair", Status: "running"}); err != nil {
		t.Fatal(err)
	}
	finishProducerRun(t, store, insertProducerRun(t, store, schedule, dep, `["producer"]`), "failed")
	repair := insertProducerRun(t, store, schedule, dep, `["producer"]`)
	start := make(chan struct{})
	ready := make(chan struct{}, 2)
	results := make(chan error, 2)
	go func() {
		ready <- struct{}{}
		<-start
		_, err := store.CompleteScheduleRunAndEnqueueActivation(db.CompleteScheduleRunParams{RunID: repair, Status: "succeeded", FinishedAt: time.Now().UTC()})
		results <- err
	}()
	go func() {
		ready <- struct{}{}
		<-start
		results <- store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: "concurrent-stop-repair", Status: "stopped", LastError: "operator stopped app"})
	}()
	<-ready
	<-ready
	close(start)
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for range 2 {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-deadline.C:
			t.Fatal("competing lifecycle operations did not finish")
		}
	}
	for range 3 {
		if err := store.EnforceCompatibilityQuarantines(); err != nil {
			t.Fatal(err)
		}
		if _, err := store.CompleteScheduleRunAndEnqueueActivation(db.CompleteScheduleRunParams{RunID: repair, Status: "succeeded", FinishedAt: time.Now().UTC()}); err != nil {
			t.Fatal(err)
		}
		app, err := store.GetAppByID(appID)
		if err != nil || app.Status != "stopped" || app.LastError != "operator stopped app" {
			t.Fatalf("concurrent repair undid stop: %+v err=%v", app, err)
		}
		blocked, err := store.AppCompatibilityQuarantined(appID)
		if err != nil || blocked {
			t.Fatalf("operator stop hid an unfinished repair: blocked=%t err=%v", blocked, err)
		}
	}
}
