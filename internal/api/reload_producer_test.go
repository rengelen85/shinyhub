package api

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/config"
	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/jobs"
	"github.com/rvben/shinyhub/internal/process"
)

// This is the reported stop chain: producer cancellation invokes the serving
// failure hook before writing the interrupted audit event. A graceful reload
// pause lets the producer complete, leaving the newly rolled consumer alive.
func TestReloadProducerRetirementPreservesConsumerOnlyAfterWriterFinishes(t *testing.T) {
	for _, interrupt := range []bool{true, false} {
		name := "graceful reload"
		if interrupt {
			name = "interrupted writer stops consumers"
		}
		t.Run(name, func(t *testing.T) {
			srv, app := newScaleTestServer(t, "roll-reload", 1, &config.Config{})
			jm, err := jobs.NewManager(srv.manager, nil, process.DefaultTier, srv.store, nil, t.TempDir(), t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			srv.SetJobs(jm, nil)
			t.Cleanup(func() { jm.Stop(context.Background()); _ = srv.manager.StopAll(); srv.Close() })
			dep, err := srv.store.GetServingDeployment(app.ID)
			if err != nil {
				t.Fatal(err)
			}
			info, err := srv.manager.Start(process.StartParams{Slug: app.Slug, Index: 0, Dir: dep.BundleDir, Command: []string{"sleep", "30"}, Port: 20991, DeploymentID: dep.ID})
			if err != nil {
				t.Fatal(err)
			}
			srv.proxy.SetPoolSize(app.Slug, 1)
			if err := srv.proxy.RegisterReplica(app.Slug, 0, info.EndpointURL, nil, dep.ID); err != nil {
				t.Fatal(err)
			}
			id, err := srv.store.CreateSchedule(db.CreateScheduleParams{
				AppID: app.ID, Name: "refresh-pend-data", CronExpr: "*/15 * * * *", Enabled: true,
				CommandJSON:    `["sh","-c","touch .producer-entered; while [ ! -e .producer-finish ]; do sleep 0.02; done"]`,
				TimeoutSeconds: 30, OverlapPolicy: "skip", MissedPolicy: "skip", DeployTrigger: "first_deploy", OnSuccess: "roll", RollFallback: "defer",
			})
			if err != nil {
				t.Fatal(err)
			}
			runID, err := jm.Run(id, "schedule", nil)
			if err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(3 * time.Second)
			for {
				if _, err := os.Stat(filepath.Join(dep.BundleDir, ".producer-entered")); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("producer never started")
				}
				time.Sleep(5 * time.Millisecond)
			}
			if !interrupt {
				jm.PauseAdmissions()
				if blockers, err := jm.ReloadBlockers(); err != nil || len(blockers) != 1 {
					t.Fatalf("reload did not wait: %v, %v", blockers, err)
				}
				if err := os.WriteFile(filepath.Join(dep.BundleDir, ".producer-finish"), nil, 0600); err != nil {
					t.Fatal(err)
				}
				for {
					blockers, err := jm.ReloadBlockers()
					if err != nil {
						t.Fatal(err)
					}
					if len(blockers) == 0 {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("producer did not finish")
					}
					time.Sleep(5 * time.Millisecond)
				}
			}
			if err := jm.InterruptAndDrain(context.Background()); err != nil {
				t.Fatal(err)
			}
			run, err := srv.store.GetScheduleRun(runID)
			if err != nil {
				t.Fatal(err)
			}
			quarantined, err := srv.store.AppCompatibilityQuarantined(app.ID)
			if err != nil {
				t.Fatal(err)
			}
			current, ok := srv.manager.GetReplica(app.Slug, 0)
			if interrupt {
				if run.Status != "interrupted" || !quarantined || (ok && current.Status != process.StatusStopped) {
					t.Fatalf("interrupted chain: run=%+v quarantine=%v replica=%+v", run, quarantined, current)
				}
				if err := srv.guardCompatibilityQuarantine(app.ID, "restart"); err == nil || !strings.Contains(err.Error(), "shinyhub schedule run roll-reload refresh-pend-data --follow") {
					t.Fatalf("restart repair instructions: %v", err)
				}
			} else if run.Status != "succeeded" || quarantined || !ok || current.Status != process.StatusRunning || srv.proxy.ReplicaTargetURL(app.Slug, 0) != info.EndpointURL {
				t.Fatalf("graceful handoff lost consumer: run=%+v quarantine=%v replica=%+v", run, quarantined, current)
			}
		})
	}
}
