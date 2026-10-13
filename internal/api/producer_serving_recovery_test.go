package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/activation"
	"github.com/rvben/shinyhub/internal/config"
	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/deploy"
	"github.com/rvben/shinyhub/internal/jobs"
	"github.com/rvben/shinyhub/internal/lifecycle"
	"github.com/rvben/shinyhub/internal/process"
	"github.com/rvben/shinyhub/internal/schedulespec"
)

// Prove serving availability, not merely recovered app metadata. The runtime
// runs real HTTP backends and terminates them on Signal; the production manager,
// jobs, lifecycle watcher, activation coordinator, and proxy own all transitions.
func TestProducerRecovery_ServesAfterRepair(t *testing.T) {
	for _, isolation := range []string{"multiplex", "grouped", "per_session"} {
		for _, initial := range []string{"running", "hibernated", "restart"} {
			for _, policy := range []string{"none", "roll"} {
				if initial == "restart" && (isolation != "multiplex" || policy != "none") {
					continue
				}
				t.Run(isolation+"/"+initial+"/"+policy, func(t *testing.T) {
					srv, store, token, mgr, appsDir, rt := buildManifestE2EServer(t, config.RuntimeConfig{})
					srv.cfg.Runtime.Docker.DefaultMemoryMB = 64
					rt.serveTraffic = true
					mgr.RegisterRuntime(process.DefaultTier, producerServingRuntime{rt})
					jm, err := jobs.NewManager(mgr, nil, process.DefaultTier, store, nil, appsDir, appsDir)
					if err != nil {
						t.Fatal(err)
					}
					srv.SetJobs(jm, nil)
					t.Cleanup(func() {
						ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()
						jm.Stop(ctx)
						_ = mgr.StopConfirmed("serving-repair")
					})
					slug := "serving-repair"
					if _, err := store.CreateApp(db.CreateAppParams{Slug: slug, Name: slug, OwnerID: 1}); err != nil {
						t.Fatal(err)
					}
					if _, err := store.DB().Exec("UPDATE apps SET worker_grouped_size=4, worker_max_workers=4 WHERE slug=?", slug); err != nil {
						t.Fatal(err)
					}
					app, err := store.GetAppBySlug(slug)
					if err != nil {
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
					if err := store.SetDeploymentDigest(dep.ID, "sha256:v1"); err != nil {
						t.Fatal(err)
					}
					if err := store.RecordDeploymentWorkerIsolation(app.ID, dep.ID, isolation); err != nil {
						t.Fatal(err)
					}
					if err := store.PromoteDeployment(dep.ID); err != nil {
						t.Fatal(err)
					}
					boot := func(_ context.Context, _ string, _ string, index int) (*deploy.Result, error) {
						current, err := store.GetServingDeployment(app.ID)
						if err != nil {
							return nil, err
						}
						fresh, err := store.GetAppByID(app.ID)
						if err != nil {
							return nil, err
						}
						p := srv.activationDeployParams(fresh, current)
						p.AppID = app.ID
						p.WorkerIsolation = isolation
						p.HealthCheck = func(string, time.Duration, http.RoundTripper) error { return nil }
						return deploy.RunReplica(p, index)
					}
					watcher := lifecycle.New(lifecycle.Config{RestartMaxAttempts: 5, DefaultWorkerIsolation: isolation}, mgr, srv.proxy, store, boot)
					watcher.SetConsumerBootGate(jm.AcquireCompatibleConsumerBootGate)
					watcher.SetTryAppOperation(srv.TryAcquireAppOperation)
					srv.proxy.SetWakeTrigger(watcher.WakeTrigger)
					srv.proxy.SetWakeHoldTimeout(2 * time.Second)
					srv.proxy.SetAppStatusLookup(func(slug string) (string, string) {
						a, err := store.GetAppBySlug(slug)
						if err != nil {
							return "", ""
						}
						return a.MissStatus(srv.DeployInFlight(slug))
					})
					srv.proxy.SetPoolSize(slug, app.Replicas)
					srv.proxy.SetPoolAppID(slug, app.ID)
					srv.proxy.SetPoolMode(slug, config.WorkerIsolationMode(isolation), 4, 4)
					spawner := &lifecycle.ElasticSpawner{Store: store, Manager: mgr, Proxy: srv.proxy,
						HealthCheck:             func(string, time.Duration, http.RoundTripper) error { return nil },
						AcquireConsumerBootGate: jm.AcquireCompatibleConsumerBootGate,
						AcquireAppOperation:     srv.AcquireAppOperation}
					srv.proxy.SetSpawnFunc(spawner.Spawn)
					srv.proxy.SetTerminateFunc(spawner.Terminate)
					assertServing := func() {
						t.Helper()
						deadline := time.Now().Add(5 * time.Second)
						var cookies []*http.Cookie
						for {
							rec := httptest.NewRecorder()
							req := httptest.NewRequest(http.MethodGet, "/app/"+slug+"/", nil)
							for _, cookie := range cookies {
								req.AddCookie(cookie)
							}
							srv.proxy.ServeHTTP(rec, req)
							if fresh := rec.Result().Cookies(); len(fresh) > 0 {
								cookies = fresh
							}
							if rec.Code == http.StatusOK && rec.Body.String() == "v1" {
								return
							}
							if time.Now().After(deadline) {
								t.Fatalf("consumer did not serve backend content: %d %q", rec.Code, rec.Body.String())
							}
							time.Sleep(10 * time.Millisecond)
						}
					}
					if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: slug, Status: "running"}); err != nil {
						t.Fatal(err)
					}
					if isolation == "multiplex" {
						result, err := boot(context.Background(), slug, bundle, 0)
						if err != nil {
							t.Fatal(err)
						}
						if err := store.UpsertReplica(db.UpsertReplicaParams{AppID: app.ID, Index: 0, PID: &result.PID, Port: &result.Port, Status: "running", DesiredState: "running", Provider: result.Provider, Tier: result.Tier, EndpointURL: result.EndpointURL, WorkerID: result.WorkerID, DeploymentID: &dep.ID, AppVersion: "v1", ConsumerBooted: true}); err != nil {
							t.Fatal(err)
						}
					} else {
						assertServing()
					}

					if initial == "hibernated" {
						if err := mgr.StopConfirmed(slug); err != nil {
							t.Fatal(err)
						}
						srv.proxy.Deregister(slug)
						if err := store.UpdateReplicaStatus(app.ID, 0, "stopped"); isolation == "multiplex" && err != nil {
							t.Fatal(err)
						}
						if isolation == "multiplex" {
							if err := store.SetReplicaDesiredState(app.ID, 0, "stopped"); err != nil {
								t.Fatal(err)
							}
						}
					}
					if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: slug, Status: map[string]string{"running": "running", "hibernated": "hibernated", "restart": "running"}[initial]}); err != nil {
						t.Fatal(err)
					}
					schedule, err := store.CreateSchedule(db.CreateScheduleParams{AppID: app.ID, Name: "producer", CronExpr: "0 5 * * *", CommandJSON: `["producer"]`, Enabled: true, TimeoutSeconds: 30, OverlapPolicy: "skip", MissedPolicy: "skip", DeployTrigger: schedulespec.DeployTriggerBundleChange, OnSuccess: policy, RollFallback: "restart"})
					if err != nil {
						t.Fatal(err)
					}
					if _, err := store.ReconcileDeployObligationsForDeployment(app.ID, dep.ID); err != nil {
						t.Fatal(err)
					}
					obligation, err := store.ClaimNextDeployObligationFor(app.ID, dep.ID)
					if err != nil {
						t.Fatal(err)
					}
					rt.mu.Lock()
					rt.producerErr = errors.New("transient upstream failure")
					rt.mu.Unlock()
					runID, err := jm.RunDeployObligation(obligation)
					if err != nil {
						t.Fatal(err)
					}
					deadline := time.Now().Add(5 * time.Second)
					for {
						run, err := store.GetScheduleRun(runID)
						if err != nil {
							t.Fatal(err)
						}
						if run.Status != "running" {
							if run.Status != "failed" {
								t.Fatalf("run status=%s", run.Status)
							}
							break
						}
						if time.Now().After(deadline) {
							t.Fatal("producer failure timed out")
						}
						time.Sleep(10 * time.Millisecond)
					}
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					if err := jm.InterruptAndDrain(ctx); err != nil {
						t.Fatal(err)
					}
					if info, ok := mgr.GetReplica(slug, 0); ok && info.Status == process.StatusRunning {
						t.Fatal("failed producer left a live consumer")
					}
					if initial == "restart" {
						// A hub can die after committing quarantine but before stopping
						// consumers. Persist a real surviving process-group leader.
						cmd := exec.Command("sleep", "30")
						cmd.Dir = bundle
						cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
						if err := cmd.Start(); err != nil {
							t.Fatal(err)
						}
						pid := cmd.Process.Pid
						done := make(chan struct{})
						go func() { _ = cmd.Wait(); close(done) }()
						t.Cleanup(func() { _ = syscall.Kill(-pid, syscall.SIGKILL); <-done })
						if _, err := store.DB().Exec("UPDATE replicas SET pid=?, provider='native' WHERE app_id=? AND idx=0", pid, app.ID); err != nil {
							t.Fatal(err)
						}
						inputs, err := lifecycle.PrepareRecovery(store)
						if err != nil {
							t.Fatal(err)
						}
						lifecycle.RecoverProcesses(store, mgr, srv.proxy, 0, false, isolation, watcher, inputs)
						select {
						case <-done:
						case <-time.After(3 * time.Second):
							t.Fatal("startup quarantine left a surviving consumer")
						}
						rows, err := store.ListReplicas(app.ID)
						if err != nil || len(rows) != 1 || rows[0].PID != nil {
							t.Fatalf("startup did not clear confirmed stopped identity: %+v %v", rows, err)
						}
					}
					rec := httptest.NewRecorder()
					srv.proxy.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/app/"+slug+"/", nil))
					if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "This app is unavailable") {
						t.Fatalf("failed response=%d %s", rec.Code, rec.Body.String())
					}
					rt.mu.Lock()
					rt.producerErr = nil
					rt.mu.Unlock()
					jm.ResumeAdmissions()
					path := fmt.Sprintf("/api/apps/%s/schedules/%d/convergence/retry", slug, schedule)
					req := httptest.NewRequest(http.MethodPost, path, nil)
					req.Header.Set("Authorization", "Bearer "+token)
					rec = httptest.NewRecorder()
					srv.Router().ServeHTTP(rec, req)
					if rec.Code != http.StatusAccepted {
						t.Fatalf("retry response=%d %s", rec.Code, rec.Body.String())
					}
					deadline = time.Now().Add(5 * time.Second)
					for {
						o, err := store.GetDeployObligation(obligation.ID)
						if err != nil {
							t.Fatal(err)
						}
						if o.Status == "satisfied" {
							break
						}
						if time.Now().After(deadline) {
							t.Fatalf("repair timed out: %+v", o)
						}
						time.Sleep(10 * time.Millisecond)
					}
					if policy == "roll" {
						coordinator := activation.New(store, srv, time.Second)
						if _, err := coordinator.ProcessNext(context.Background()); err != nil {
							t.Fatal(err)
						}
					}
					watcher.RunOnce()
					assertServing()
				})
			}
		}
	}
}

// The HTTP-backed fake exposes the native producer runtime capabilities so
// ElasticSpawner exercises its durable-before-publication path.
type producerServingRuntime struct{ *manifestFakeRuntime }

func (producerServingRuntime) SupportsGuardedStart() bool { return true }
