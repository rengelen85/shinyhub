package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/deploy"
	"github.com/rvben/shinyhub/internal/deployevent"
	"github.com/rvben/shinyhub/internal/lifecycle"
	"github.com/rvben/shinyhub/internal/schedulespec"
)

func TestDeploy_RepairsQuarantinedSleepingAppWithoutConsumerBoot(t *testing.T) {
	srv, store, token, runtime := newManifestE2EServerWithJobs(t)
	t.Cleanup(func() { srv.jobs.Stop(context.Background()); _ = srv.manager.StopAll(); srv.Close() })
	runtime.serveTraffic = true
	const slug = "quarantined-sleeper"
	if _, err := store.CreateApp(db.CreateAppParams{Slug: slug, Name: slug, OwnerID: 1}); err != nil {
		t.Fatal(err)
	}
	const manifest = `[app.worker]
isolation = "multiplex"
[[schedule]]
name = "refresh-data"
cron = "0 2 * * *"
cmd = "python refresh.py"
deploy_trigger = "bundle_change"
`
	streamRepair := false
	upload := func(source string) *httptest.ResponseRecorder {
		t.Helper()
		body, contentType := buildMultiFileBundleUpload(t, map[string]string{"app.py": source, "refresh.py": "print('data')", "shinyhub.toml": manifest})
		req := httptest.NewRequest(http.MethodPost, "/api/apps/"+slug+"/deploy", body)
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", contentType)
		req.Header.Set("X-ShinyHub-Allow-Downtime", "1")
		if streamRepair {
			req.Header.Set("Accept", deployevent.MediaType)
		}
		rec := httptest.NewRecorder()
		srv.Router().ServeHTTP(rec, req)
		return rec
	}
	if rec := upload("print('v1')"); rec.Code != http.StatusOK {
		t.Fatalf("first deploy: %d %s", rec.Code, rec.Body.String())
	}
	app, err := store.GetAppBySlug(slug)
	if err != nil {
		t.Fatal(err)
	}
	// Establish the pre-failure sleeping state without waiting ten real minutes.
	if err := srv.manager.StopConfirmed(slug); err != nil {
		t.Fatal(err)
	}
	srv.proxy.Deregister(slug)
	if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: slug, Status: "hibernated"}); err != nil {
		t.Fatal(err)
	}
	dep, err := store.GetServingDeployment(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	scheduleID := scheduleIDByName(t, store, app.ID, "refresh-data")
	command, fingerprint, err := schedulespec.ProducerIdentity(`["python","refresh.py"]`)
	if err != nil {
		t.Fatal(err)
	}
	runID, err := store.InsertScheduleRun(db.InsertScheduleRunParams{ScheduleID: scheduleID, Status: "running", Trigger: "manual", StartedAt: time.Now().UTC(), DeploymentID: &dep.ID, AppVersion: dep.Version, ContentDigest: dep.ContentDigest, ProducerCommandJSON: command, ProducerFingerprint: fingerprint, PublishesData: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteScheduleRunAndEnqueueActivation(db.CompleteScheduleRunParams{RunID: runID, Status: "failed", FinishedAt: time.Now().UTC(), DataWriteAttempted: true}); err != nil {
		t.Fatal(err)
	}
	failed, err := store.GetAppBySlug(slug)
	if err != nil || failed.Status != "failed" || failed.ProducerRecoveryStatus != "hibernated" {
		t.Fatalf("quarantine=%+v err=%v", failed, err)
	}

	// A second failed producer, now inside the corrective deployment barrier,
	// must not lose the original sleeping intent before a successful retry.
	runtime.mu.Lock()
	runtime.producerErr = errors.New("repair producer failed")
	runtime.mu.Unlock()
	if rec := upload("print('broken-v2')"); rec.Code != http.StatusInternalServerError {
		t.Fatalf("failed repair: %d %s", rec.Code, rec.Body.String())
	}
	stillFailed, err := store.GetAppBySlug(slug)
	if err != nil || stillFailed.Status != "failed" || stillFailed.ProducerRecoveryStatus != "hibernated" {
		t.Fatalf("lost recovery intent: %+v err=%v", stillFailed, err)
	}
	runtime.mu.Lock()
	runtime.producerErr = nil
	runtime.mu.Unlock()

	// Successful repair must retain its saved intent until the deployment's
	// later declaration snapshot is committed. Fail that checkpoint after the
	// physical producer run succeeds, then retry once more.
	removeSnapshotFailure := installDBFailureTrigger(t, store, dbFailureTrigger{name: "reject_candidate_snapshot", table: "deployment_schedule_snapshots", event: "INSERT", condition: "1 = 1"})
	if rec := upload("print('snapshot-failure')"); rec.Code != http.StatusInternalServerError {
		t.Fatalf("snapshot failure: %d %s", rec.Code, rec.Body.String())
	}
	runs, err := store.ListScheduleRuns(scheduleID, 1, 0)
	if err != nil || len(runs) != 1 || runs[0].Status != "succeeded" {
		t.Fatalf("snapshot checkpoint did not follow successful producer: %+v err=%v", runs, err)
	}
	stillFailed, err = store.GetAppBySlug(slug)
	if err != nil || stillFailed.Status != "failed" || stillFailed.ProducerRecoveryStatus != "hibernated" {
		t.Fatalf("successful producer lost recovery intent before commit: %+v err=%v", stillFailed, err)
	}
	removeSnapshotFailure()
	streamRepair = true
	rec := upload("print('v2')")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"type":"result"`) {
		t.Fatalf("repair deploy: %d %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "Bundle prepared; app remains hibernated") || strings.Contains(rec.Body.String(), "worker activation follows") {
		t.Fatalf("incorrect preparation progress: %s", rec.Body.String())
	}
	repaired, err := store.GetAppBySlug(slug)
	if err != nil || repaired.Status != "hibernated" {
		t.Fatalf("repaired=%+v err=%v", repaired, err)
	}
	if blocked, err := store.AppCompatibilityQuarantined(app.ID); err != nil || blocked {
		t.Fatalf("compatibility quarantine=%v err=%v", blocked, err)
	}
	if srv.proxy.PoolHasAny(slug) {
		t.Fatal("repair deploy started an unsolicited consumer")
	}
	for _, info := range srv.manager.All() {
		if info.Slug == slug {
			t.Fatalf("repair left a process: %+v", info)
		}
	}

	current, err := store.GetServingDeployment(app.ID)
	if err != nil || current.ID == dep.ID {
		t.Fatalf("repair did not publish a new serving bundle: %+v err=%v", current, err)
	}
	boot := func(_ context.Context, _ string, bundleDir string, index int) (*deploy.Result, error) {
		if bundleDir != current.BundleDir {
			return nil, fmt.Errorf("wake selected old bundle %q, want %q", bundleDir, current.BundleDir)
		}
		fresh, err := store.GetAppByID(app.ID)
		if err != nil {
			return nil, err
		}
		p := srv.activationDeployParams(fresh, current)
		p.HealthCheck = func(string, time.Duration, http.RoundTripper) error { return nil }
		return deploy.RunReplica(p, index)
	}
	watcher := lifecycle.New(lifecycle.Config{DefaultWorkerIsolation: "multiplex"}, srv.manager, srv.proxy, store, boot)
	watcher.SetConsumerBootGate(srv.jobs.AcquireCompatibleConsumerBootGate)
	watcher.SetTryAppOperation(srv.TryAcquireAppOperation)
	srv.proxy.SetAppStatusLookup(func(slug string) (string, string) {
		fresh, err := store.GetAppBySlug(slug)
		if err != nil {
			return "", ""
		}
		return fresh.MissStatus(srv.DeployInFlight(slug))
	})
	woke := make(chan struct{})
	srv.proxy.SetWakeTrigger(func(ctx context.Context, slug string) { watcher.WakeTrigger(ctx, slug); close(woke) })
	srv.proxy.SetWakeHoldTimeout(time.Second)
	srv.proxy.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/app/"+slug+"/", nil))
	select {
	case <-woke:
	case <-time.After(5 * time.Second):
		t.Fatal("visitor did not wake the repaired app")
	}
	if info, ok := srv.manager.GetReplica(slug, 0); !ok || info.DeploymentID != current.ID {
		t.Fatalf("visitor did not boot the repaired bundle: %+v", info)
	}
}

func TestDeploy_HandoffAuditFailureStillCompletesBookkeeping(t *testing.T) {
	srv, store, token, _ := newManifestE2EServerWithJobs(t)
	t.Cleanup(func() { srv.jobs.Stop(context.Background()); _ = srv.manager.StopAll(); srv.Close() })
	zeroFloor := 0
	srv.cfg.Server.MinAvailableMemoryMB = &zeroFloor
	app := createGenerationTestApp(t, store, "handoff-audit", 1, 16)
	if _, err := store.CreateSchedule(db.CreateScheduleParams{AppID: app.ID, Name: "first", CronExpr: "0 * * * *", CommandJSON: `["true"]`, Enabled: true, TimeoutSeconds: 60, OverlapPolicy: "skip", MissedPolicy: "skip", DeployTrigger: "first_deploy", OnSuccess: "none"}); err != nil {
		t.Fatal(err)
	}
	if rec := deployBareGeneration(t, srv, token, app.Slug, "print('v1')", false); rec.Code != http.StatusOK {
		t.Fatalf("first deploy: %d %s", rec.Code, rec.Body.String())
	}
	previous, err := store.GetServingDeployment(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	// Make a real transition necessary after handoff's preflight has passed.
	// An unchanged running status would correctly skip the audit insert.
	srv.promoteDeployment = func(id int64) error {
		if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: app.Slug, Status: "degraded"}); err != nil {
			return err
		}
		return store.PromoteDeployment(id)
	}
	var auditFailures atomic.Int32
	store.SetAuditErrorHook(func() { auditFailures.Add(1) })
	installDBFailureTrigger(t, store, dbFailureTrigger{name: "reject_handoff_audit", table: "audit_events", event: "INSERT", condition: "NEW.action = 'app_status_changed'"})
	rec := deployBareGeneration(t, srv, token, app.Slug, "print('v2')", false)
	if rec.Code != http.StatusOK {
		t.Fatalf("published handoff failed: %d %s", rec.Code, rec.Body.String())
	}
	var response struct {
		Warning     string                      `json:"warning"`
		Handoff     bool                        `json:"handoff"`
		Convergence []ScheduleConvergenceResult `json:"schedule_convergence"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if !response.Handoff || response.Warning != "" || len(response.Convergence) != 1 || response.Convergence[0].Status != "satisfied" || auditFailures.Load() != 1 {
		t.Fatalf("audit failure blocked status or convergence: %s failures=%d", rec.Body.String(), auditFailures.Load())
	}
	fresh, err := store.GetAppByID(app.ID)
	if err != nil || fresh.Status != "running" {
		t.Fatalf("published handoff status was suppressed by audit: %+v err=%v", fresh, err)
	}
	current, err := store.GetServingDeployment(app.ID)
	if err != nil || current.ID == previous.ID || !current.Prepared {
		t.Fatalf("handoff preparation was not recorded: %+v err=%v", current, err)
	}
	if info, ok := srv.manager.GetReplica(app.Slug, 0); !ok || info.DeploymentID != current.ID || !srv.proxy.PoolHasAny(app.Slug) {
		t.Fatalf("published handoff lost its pool: %+v", info)
	}
}
