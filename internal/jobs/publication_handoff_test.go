package jobs_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/dbtest"
	"github.com/rvben/shinyhub/internal/jobs"
	"github.com/rvben/shinyhub/internal/process"
)

func TestProducerRepairWaitsForFailedConsumerCleanupAcrossManagers(t *testing.T) {
	store, app, scheduleID := publicationHandoffFixture(t)
	sharedData := t.TempDir()
	failedRuntime := &fakeRuntime{exitInfo: process.ExitInfo{Code: 1}}
	repairRuntime := &fakeRuntime{exitInfo: process.ExitInfo{Code: 0}}
	failedManager := publicationHandoffManager(t, store, sharedData, failedRuntime)
	repairManager := publicationHandoffManager(t, store, sharedData, repairRuntime)
	cleanupEntered := make(chan struct{})
	allowCleanup := make(chan struct{})
	var releaseOnce sync.Once
	releaseCleanup := func() { releaseOnce.Do(func() { close(allowCleanup) }) }
	// Release the deliberately stalled external cleanup before joining managers,
	// including when an assertion fails.
	t.Cleanup(releaseCleanup)
	failedManager.SetUnsafePublicationHandler(func(id int64, slug, status string) {
		if id != app.ID || slug != app.Slug || status != "failed" {
			t.Errorf("cleanup received app=%d slug=%s status=%s", id, slug, status)
		}
		close(cleanupEntered)
		<-allowCleanup
	})
	failedID, err := failedManager.Run(scheduleID, "manual", nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-cleanupEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("failed producer did not reach consumer cleanup")
	}
	failed, err := store.GetScheduleRun(failedID)
	if err != nil || failed.Status != "failed" {
		t.Fatalf("cleanup began before durable failure: run=%+v err=%v", failed, err)
	}
	// A terminal run row is not permission to recover: old consumer cleanup is
	// still capable of removing a freshly repaired replica. The second manager
	// has independent in-process gates and must honor the shared file fence.
	assertRecoveryFenceUnavailable(t, repairManager, app.ID)
	repairID, err := repairManager.Run(scheduleID, "manual", nil)
	if err != nil {
		t.Fatal(err)
	}
	assertRecoveryFenceUnavailable(t, repairManager, app.ID)
	repairRuntime.mu.Lock()
	calls := repairRuntime.calls
	repairRuntime.mu.Unlock()
	if calls != 0 {
		t.Fatalf("repair executed %d times before failed consumer cleanup finished", calls)
	}
	state, err := store.GetAppByID(app.ID)
	if err != nil || state.Status != "failed" {
		t.Fatalf("repair escaped cleanup fence: app=%+v err=%v", state, err)
	}
	releaseCleanup()
	waitPublicationRun(t, store, repairID, "succeeded")
	state, err = store.GetAppByID(app.ID)
	if err != nil || state.Status != "running" || state.LastError != "" {
		t.Fatalf("repair did not restore availability: app=%+v err=%v", state, err)
	}
	publication, err := store.GetAppDataPublication(app.ID)
	if err != nil || publication.Generation != 1 || publication.ScheduleRunID != repairID {
		t.Fatalf("repair publication=%+v err=%v", publication, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	release, err := failedManager.AcquirePublicationRecoveryFences(ctx, []int64{app.ID})
	if err != nil {
		t.Fatalf("completed repair leaked publication fence: %v", err)
	}
	release()
	releaseConsumer, err := failedManager.AcquireCompatibleConsumerBootGate(app.ID)
	if err != nil {
		t.Fatalf("repaired app still rejects consumer boot: %v", err)
	}
	releaseConsumer()
}

func TestCancelledRecoveryReleasesPartialFencesAcrossManagers(t *testing.T) {
	store, firstApp, _ := publicationHandoffFixture(t)
	if _, err := store.CreateApp(db.CreateAppParams{Slug: "second-handoff", Name: "second-handoff", OwnerID: firstApp.OwnerID}); err != nil {
		t.Fatal(err)
	}
	secondApp, err := store.GetAppBySlug("second-handoff")
	if err != nil {
		t.Fatal(err)
	}
	secondID := secondApp.ID
	sharedData := t.TempDir()
	consumer := publicationHandoffManager(t, store, sharedData, &fakeRuntime{})
	recovering := publicationHandoffManager(t, store, sharedData, &fakeRuntime{})
	probe := publicationHandoffManager(t, store, sharedData, &fakeRuntime{})
	releaseConsumer, err := consumer.AcquireConsumerBootGate(secondID)
	if err != nil {
		t.Fatal(err)
	}
	var releaseOnce sync.Once
	finishConsumer := func() { releaseOnce.Do(releaseConsumer) }
	defer finishConsumer()
	// Deliberately unordered and duplicated input: startup may discover the same
	// app through several running jobs. It must not self-deadlock, and losing
	// its lease halfway through cannot strand the earlier app's fences.
	ids := []int64{secondID, firstApp.ID, firstApp.ID}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	release, err := recovering.AcquirePublicationRecoveryFences(ctx, ids)
	cancel()
	if release != nil {
		release()
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("recovery crossed live consumer: %v", err)
	}
	assertRecoveryFenceAvailable(t, probe, []int64{firstApp.ID})
	// Retry with the same manager and the same batch. This checks cleanup of
	// both filesystem locks and its own in-process gates.
	finishConsumer()
	assertRecoveryFenceAvailable(t, recovering, ids)
	assertRecoveryFenceAvailable(t, probe, []int64{firstApp.ID, secondID})
}

func TestTerminalPersistenceOutageKeepsPublicationFencedUntilDurableFailure(t *testing.T) {
	store, app, scheduleID := publicationHandoffFixture(t)
	sharedData := t.TempDir()
	runtime := &fakeRuntime{exitInfo: process.ExitInfo{Code: 1}}
	writer := publicationHandoffManager(t, store, sharedData, runtime)
	successor := publicationHandoffManager(t, store, sharedData, &fakeRuntime{})
	cleanup := make(chan struct{}, 1)
	writer.SetUnsafePublicationHandler(func(int64, string, string) { cleanup <- struct{}{} })
	// Fail the real transactional audit insert, rather than mocking completion.
	// The physical writer has exited, but its terminal transition must roll back
	// and retry while the manager retains the cross-process exclusion fence.
	if _, err := store.DB().Exec(`ALTER TABLE audit_events RENAME TO unavailable_audit_events`); err != nil {
		t.Fatal(err)
	}
	var restoreOnce sync.Once
	restoreAudit := func() {
		restoreOnce.Do(func() {
			if _, err := store.DB().Exec(`ALTER TABLE unavailable_audit_events RENAME TO audit_events`); err != nil {
				t.Errorf("restore audit table: %v", err)
			}
		})
	}
	t.Cleanup(restoreAudit)
	runID, err := writer.Run(scheduleID, "manual", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForCalls(t, runtime, 1, 5*time.Second)
	assertRecoveryFenceUnavailable(t, successor, app.ID)
	type bootResult struct {
		release func()
		err     error
	}
	boot := make(chan bootResult, 1)
	go func() {
		release, err := successor.AcquireCompatibleConsumerBootGate(app.ID)
		boot <- bootResult{release, err}
	}()
	bootObserved := false
	t.Cleanup(func() {
		restoreAudit()
		if !bootObserved {
			select {
			case result := <-boot:
				if result.release != nil {
					result.release()
				}
			case <-time.After(5 * time.Second):
				t.Error("consumer boot did not leave the restored publication fence")
			}
		}
	})
	// Recovery also takes the job-execution fence. Check an ordinary consumer
	// separately so that fence cannot conceal an early publication unlock.
	select {
	case result := <-boot:
		bootObserved = true
		if result.release != nil {
			result.release()
		}
		t.Fatalf("consumer boot crossed non-durable producer failure: %v", result.err)
	case <-time.After(100 * time.Millisecond):
	}
	run, err := store.GetScheduleRun(runID)
	if err != nil || run.Status != "running" {
		t.Fatalf("non-durable terminal state escaped rollback: run=%+v err=%v", run, err)
	}
	state, err := store.GetAppByID(app.ID)
	if err != nil || state.Status != "running" || state.LastError != "" {
		t.Fatalf("non-durable app transition escaped rollback: app=%+v err=%v", state, err)
	}
	select {
	case <-cleanup:
		t.Fatal("consumer cleanup ran before failure became durable")
	default:
	}
	restoreAudit()
	select {
	case <-cleanup:
	case <-time.After(5 * time.Second):
		t.Fatal("terminal persistence did not recover when the database recovered")
	}
	waitPublicationRun(t, store, runID, "failed")
	select {
	case result := <-boot:
		bootObserved = true
		if result.release != nil {
			result.release()
		}
		if !errors.Is(result.err, jobs.ErrAppCompatibilityQuarantined) {
			t.Fatalf("waiting consumer can boot against failed publication: %v", result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiting consumer did not recheck durable compatibility")
	}
	assertRecoveryFenceAvailable(t, successor, []int64{app.ID})
	release, err := successor.AcquireCompatibleConsumerBootGate(app.ID)
	if release != nil {
		release()
	}
	if !errors.Is(err, jobs.ErrAppCompatibilityQuarantined) {
		t.Fatalf("consumer can boot against failed publication: %v", err)
	}
	count, err := store.CountAuditEvents("app_status_changed")
	if err != nil || count != 1 {
		t.Fatalf("terminal retry duplicated or lost transition audit: count=%d err=%v", count, err)
	}
}

func assertRecoveryFenceAvailable(t *testing.T, m *jobs.Manager, ids []int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	release, err := m.AcquirePublicationRecoveryFences(ctx, ids)
	if err != nil {
		t.Fatalf("recovery leaked a fence for apps %v: %v", ids, err)
	}
	// Startup teardown can be reached twice. Its release must be idempotent.
	release()
	release()
}

func publicationHandoffFixture(t *testing.T) (*db.Store, *db.App, int64) {
	t.Helper()
	store := dbtest.New(t)
	if err := store.CreateUser(db.CreateUserParams{Username: "owner", PasswordHash: "h", Role: "developer"}); err != nil {
		t.Fatal(err)
	}
	owner, err := store.GetUserByUsername("owner")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateApp(db.CreateAppParams{Slug: "producer-handoff", Name: "producer-handoff", OwnerID: owner.ID}); err != nil {
		t.Fatal(err)
	}
	app, err := store.GetAppBySlug("producer-handoff")
	if err != nil {
		t.Fatal(err)
	}
	deployment, err := store.BeginDeployment(app.ID, "v1", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetDeploymentDigest(deployment.ID, "sha256:v1"); err != nil {
		t.Fatal(err)
	}
	if err := store.PromoteDeployment(deployment.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: app.Slug, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	scheduleID, err := store.CreateSchedule(db.CreateScheduleParams{
		AppID: app.ID, Name: "refresh", CronExpr: "0 5 * * *", CommandJSON: `["producer"]`,
		Enabled: true, TimeoutSeconds: 10, OverlapPolicy: "concurrent", MissedPolicy: "skip",
		DeployTrigger: "bundle_change", OnSuccess: "none", RollFallback: "defer",
	})
	if err != nil {
		t.Fatal(err)
	}
	return store, app, scheduleID
}

func publicationHandoffManager(t *testing.T, store *db.Store, sharedData string, runtime process.Runtime) *jobs.Manager {
	t.Helper()
	m, err := jobs.NewManager(process.NewManager(t.TempDir(), runtime), nil, process.DefaultTier, store, nil, sharedData, sharedData)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if !m.Stop(ctx) {
			t.Error("publication manager did not join its runs")
		}
	})
	return m
}

func assertRecoveryFenceUnavailable(t *testing.T, m *jobs.Manager, appID int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	release, err := m.AcquirePublicationRecoveryFences(ctx, []int64{appID})
	if release != nil {
		release()
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("recovery crossed unfinished publication: err=%v", err)
	}
}

func waitPublicationRun(t *testing.T, store *db.Store, runID int64, status string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		run, err := store.GetScheduleRun(runID)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status == status {
			return
		}
		if run.Status != "running" || time.Now().After(deadline) {
			t.Fatalf("run %d status=%s; want %s", runID, run.Status, status)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
