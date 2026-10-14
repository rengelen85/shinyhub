package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/auth"
	"github.com/rvben/shinyhub/internal/config"
	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/dbtest"
	"github.com/rvben/shinyhub/internal/jobs"
	"github.com/rvben/shinyhub/internal/lifecycle"
	"github.com/rvben/shinyhub/internal/process"
	"github.com/rvben/shinyhub/internal/proxy"
)

// Different backend content distinguishes a new consumer from a stale route.
// All scheduling, publication, recovery, and routing use production components.
type restartRepairRuntime struct {
	producerServingRuntime
	body string
}

func (r restartRepairRuntime) Start(ctx context.Context, p process.StartParams, out io.Writer) (process.ReplicaEndpoint, error) {
	p.AppVersion = r.body
	return r.manifestFakeRuntime.Start(ctx, p, out)
}

func TestProducerRepairSurvivesDatabaseAndServerRestart(t *testing.T) {
	for _, stop := range []bool{false, true} {
		t.Run(fmt.Sprintf("operator_stop=%t", stop), func(t *testing.T) {
			var store *db.Store
			var dsn string
			if os.Getenv("SHINYHUB_TEST_POSTGRES_DSN") != "" {
				store, dsn = dbtest.NewPostgres(t)
			} else {
				dsn = filepath.Join(t.TempDir(), "hub.db")
				dbtest.WriteSQLiteFile(t, dsn)
				var err error
				store, err = db.Open(dsn)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
			}
			hash, err := testHashPassword("pass")
			if err != nil {
				t.Fatal(err)
			}
			if err := store.CreateUser(db.CreateUserParams{Username: "admin", PasswordHash: hash, Role: "admin"}); err != nil {
				t.Fatal(err)
			}
			admin, err := store.GetUserByUsername("admin")
			if err != nil {
				t.Fatal(err)
			}
			token, err := auth.IssueJWT(admin.ID, admin.Username, admin.Role, "test-secret")
			if err != nil {
				t.Fatal(err)
			}
			const slug = "restart-repair"
			if _, err := store.CreateApp(db.CreateAppParams{Slug: slug, Name: slug, OwnerID: admin.ID}); err != nil {
				t.Fatal(err)
			}
			app, err := store.GetAppBySlug(slug)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.ApplyAppManifestSettings(db.ApplyAppManifestSettingsParams{AppID: app.ID, SetWorkerIsolation: true, WorkerIsolation: "grouped", SetWorkerGroupedSize: true, WorkerGroupedSize: 4, SetWorkerMaxWorkers: true, WorkerMaxWorkers: 4}); err != nil {
				t.Fatal(err)
			}
			bundle := t.TempDir()
			if err := os.WriteFile(filepath.Join(bundle, "app.py"), []byte("print('app')\n"), 0600); err != nil {
				t.Fatal(err)
			}
			dep, err := store.BeginDeployment(app.ID, "v1", bundle)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.SetDeploymentDigest(dep.ID, "sha256:restart-v1"); err != nil {
				t.Fatal(err)
			}
			if err := store.RecordDeploymentWorkerIsolation(app.ID, dep.ID, "grouped"); err != nil {
				t.Fatal(err)
			}
			if err := store.PromoteDeployment(dep.ID); err != nil {
				t.Fatal(err)
			}
			if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: slug, Status: "running"}); err != nil {
				t.Fatal(err)
			}
			scheduleID, err := store.CreateSchedule(db.CreateScheduleParams{AppID: app.ID, Name: "refresh-data", CronExpr: "0 5 * * *", CommandJSON: `["refresh"]`, Enabled: true, TimeoutSeconds: 30, OverlapPolicy: "skip", MissedPolicy: "skip", DeployTrigger: "bundle_change", OnSuccess: "none", RollFallback: "defer"})
			if err != nil {
				t.Fatal(err)
			}
			cfg := &config.Config{Auth: config.AuthConfig{Secret: "test-secret"}, Storage: config.StorageConfig{AppsDir: t.TempDir()}, Runtime: config.RuntimeConfig{DefaultWorkerIsolation: "grouped"}}
			// Each phase owns fresh server, jobs, process-manager and proxy instances.
			bootServer := func(s *db.Store, body string, nextPID int) (*Server, *manifestFakeRuntime, *lifecycle.Watcher, func()) {
				t.Helper()
				rt := newManifestFakeRuntime()
				rt.serveTraffic, rt.nextPID = true, nextPID
				mgr := process.NewManager(cfg.Storage.AppsDir, restartRepairRuntime{producerServingRuntime{rt}, body})
				prx := proxy.New()
				srv := New(cfg, s, mgr, prx)
				jm, err := jobs.NewManager(mgr, nil, process.DefaultTier, s, nil, cfg.Storage.AppsDir, cfg.Storage.AppsDir)
				if err != nil {
					t.Fatal(err)
				}
				srv.SetJobs(jm, nil)
				watcher := lifecycle.New(lifecycle.Config{DefaultWorkerIsolation: "grouped"}, mgr, prx, s, nil)
				watcher.SetConsumerBootGate(jm.AcquireCompatibleConsumerBootGate)
				watcher.SetTryAppOperation(srv.TryAcquireAppOperation)
				prx.SetWakeTrigger(watcher.WakeTrigger)
				prx.SetAppStatusLookup(func(slug string) (string, string) {
					a, err := s.GetAppBySlug(slug)
					if err != nil {
						return "", ""
					}
					return a.MissStatus(srv.DeployInFlight(slug))
				})
				prx.SetPoolSize(slug, app.Replicas)
				prx.SetPoolAppID(slug, app.ID)
				prx.SetPoolMode(slug, config.IsolationGrouped, 4, 4)
				spawner := &lifecycle.ElasticSpawner{Store: s, Manager: mgr, Proxy: prx, RuntimeCfg: cfg.Runtime, AcquireConsumerBootGate: jm.AcquireCompatibleConsumerBootGate, AcquireAppOperation: srv.AcquireAppOperation}
				prx.SetSpawnFunc(spawner.Spawn)
				prx.SetTerminateFunc(spawner.Terminate)
				closed := false
				closeServer := func() {
					if closed {
						return
					}
					closed = true
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					jm.Stop(ctx)
					if err := mgr.StopAll(); err != nil {
						t.Errorf("stop server processes: %v", err)
					}
					srv.Close()
				}
				t.Cleanup(closeServer)
				return srv, rt, watcher, closeServer
			}
			srv, runtime, watcher, closeServer := bootServer(store, "original data", 1000000100)
			path := fmt.Sprintf("/api/apps/%s/schedules/%d", slug, scheduleID)
			request := func(method, path, body string, want int) *httptest.ResponseRecorder {
				t.Helper()
				req := httptest.NewRequest(method, path, strings.NewReader(body))
				req.Header.Set("Authorization", "Bearer "+token)
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				srv.Router().ServeHTTP(rec, req)
				if rec.Code != want {
					t.Fatalf("%s %s: %d %s; want %d", method, path, rec.Code, rec.Body.String(), want)
				}
				return rec
			}
			run := func(status string) int64 {
				t.Helper()
				rec := request(http.MethodPost, path+"/run", "", http.StatusAccepted)
				var admitted struct {
					RunID int64 `json:"run_id"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &admitted); err != nil {
					t.Fatal(err)
				}
				deadline := time.Now().Add(5 * time.Second)
				for {
					row, err := store.GetScheduleRun(admitted.RunID)
					if err != nil {
						t.Fatal(err)
					}
					if row.Status == status {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("run %d: %+v; want %s", admitted.RunID, row, status)
					}
					time.Sleep(5 * time.Millisecond)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := srv.jobs.InterruptAndDrain(ctx); err != nil {
					t.Fatal(err)
				}
				srv.jobs.ResumeAdmissions()
				return admitted.RunID
			}
			var cookies []*http.Cookie
			visit := func() *httptest.ResponseRecorder {
				req := httptest.NewRequest(http.MethodGet, "/app/"+slug+"/", nil)
				for _, c := range cookies {
					req.AddCookie(c)
				}
				rec := httptest.NewRecorder()
				srv.proxy.ServeHTTP(rec, req)
				if fresh := rec.Result().Cookies(); len(fresh) > 0 {
					cookies = fresh
				}
				return rec
			}
			assertServing := func(body string) {
				t.Helper()
				deadline := time.Now().Add(5 * time.Second)
				for {
					rec := visit()
					if rec.Code == http.StatusOK && rec.Body.String() == body {
						return
					}
					if time.Now().After(deadline) {
						t.Fatalf("did not serve %q: %d %s", body, rec.Code, rec.Body.String())
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
			run("succeeded")
			assertServing("original data")
			runtime.mu.Lock()
			runtime.producerErr = errors.New("transient upstream failure")
			runtime.mu.Unlock()
			failedID := run("failed")
			request(http.MethodPatch, path, `{"enabled":false,"deploy_trigger":"never"}`, http.StatusOK)
			if stop {
				request(http.MethodPost, "/api/apps/"+slug+"/stop", "", http.StatusOK)
			}
			closeServer()
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = db.Open(dsn)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			if err := store.Migrate(); err != nil {
				t.Fatal(err)
			}
			var restartedRuntime *manifestFakeRuntime
			srv, restartedRuntime, watcher, _ = bootServer(store, "repaired data", 1000000200)
			// Native recovery verifies that the old synthetic PID is absent. The
			// HTTP runtime takes over only after persisted identities are reconciled.
			srv.manager.RegisterRuntime(process.DefaultTier, process.NewNativeRuntime())
			// Run the public startup recovery steps against persisted state before
			// admitting a repair. No old server object or connection remains in use.
			recoveryCtx, cancelRecovery := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelRecovery()
			release, err := srv.jobs.AcquirePublicationRecoveryFences(recoveryCtx, []int64{app.ID})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.MarkRunningSchedulesInterrupted(); err != nil {
				release()
				t.Fatal(err)
			}
			if err := lifecycle.ReconcileInflightDeployments(store); err != nil {
				release()
				t.Fatal(err)
			}
			if err := store.RepairDeploymentGenerationLedger(); err != nil {
				release()
				t.Fatal(err)
			}
			inputs, err := lifecycle.PrepareRecovery(store)
			if err != nil {
				release()
				t.Fatal(err)
			}
			lifecycle.RecoverProcesses(store, srv.manager, srv.proxy, 32, false, "grouped", nil, inputs)
			srv.manager.RegisterRuntime(process.DefaultTier, restartRepairRuntime{producerServingRuntime{restartedRuntime}, "repaired data"})
			if rows, err := store.ListDeploymentReplicas(app.ID); err != nil || len(rows) != 0 {
				release()
				t.Fatalf("restart retained retired consumer identities: %+v %v", rows, err)
			}
			release()
			watcher.RunOnce()
			readApp := func() db.App {
				t.Helper()
				rec := request(http.MethodGet, "/api/apps/"+slug, "", http.StatusOK)
				var view struct {
					App db.App `json:"app"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &view); err != nil {
					t.Fatal(err)
				}
				return view.App
			}
			view := readApp()
			wantStatus := "failed"
			if stop {
				wantStatus = "stopped"
			}
			if view.DesiredStatus != wantStatus || !view.ProducerRepairRequired || !view.CompatibilityQuarantined || len(view.ProducerRepairSchedules) != 1 || view.ProducerRepairSchedules[0] != "refresh-data" {
				t.Fatalf("restart erased failure or operator intent: %+v", view)
			}
			if !stop && !strings.Contains(view.LastReplicaError, fmt.Sprintf("producer run %d", failedID)) {
				t.Fatalf("restart lost failure diagnostic: %+v", view)
			}
			denied := request(http.MethodDelete, path, "", http.StatusConflict)
			if !strings.Contains(denied.Body.String(), "unrepaired failed data write") {
				t.Fatalf("restart lost deletion diagnosis: %s", denied.Body.String())
			}
			rec := visit()
			wantPage := "This app is unavailable"
			if stop {
				wantPage = "This app is stopped"
			}
			if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), wantPage) {
				t.Fatalf("restarted quarantined app appeared healthy or starting: %d %s", rec.Code, rec.Body.String())
			}
			repairedID := run("succeeded")
			publication, err := store.GetAppDataPublication(app.ID)
			if err != nil || publication.Generation != 2 || publication.ScheduleRunID != repairedID {
				t.Fatalf("repair publication: %+v %v", publication, err)
			}
			watcher.RunOnce()
			view = readApp()
			wantStatus = "running"
			if stop {
				wantStatus = "stopped"
			}
			if view.DesiredStatus != wantStatus || view.ProducerRepairRequired || view.CompatibilityQuarantined || view.LastReplicaError != "" {
				t.Fatalf("repair after restart lost operator intent or diagnosis: %+v", view)
			}
			if stop {
				for range 3 {
					rec := visit()
					if rec.Code == http.StatusOK || rec.Body.String() == "repaired data" {
						t.Fatalf("repair restarted stopped app: %d %s", rec.Code, rec.Body.String())
					}
				}
				request(http.MethodPost, "/api/apps/"+slug+"/restart?if_not_running=true", "", http.StatusOK)
			}
			assertServing("repaired data")
			rows, err := store.ListDeploymentReplicas(app.ID)
			if err != nil || len(rows) != 1 || rows[0].DataGeneration != 2 || rows[0].DeploymentID != dep.ID || rows[0].Status != "running" {
				t.Fatalf("replacement consumer did not boot repaired publication: %+v %v", rows, err)
			}
		})
	}
}
