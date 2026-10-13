package api

import (
	"context"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/config"
	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/jobs"
	"github.com/rvben/shinyhub/internal/process"
)

func TestProducerFailureWithSparseConsumerPool(t *testing.T) {
	srv, app := newScaleTestServer(t, "sparse-failure", 2, &config.Config{})
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
	// Replica zero is absent, as after a crash or partial-success deployment.
	info, err := srv.manager.Start(process.StartParams{Slug: app.Slug, Index: 1, Dir: dep.BundleDir, Command: []string{"sleep", "30"}, Port: 20992, DeploymentID: dep.ID})
	if err != nil {
		t.Fatal(err)
	}
	if slots := srv.manager.AllForSlug(app.Slug); len(slots) != 2 || slots[0] != nil {
		t.Fatalf("fixture must contain an empty consumer slot: %+v", slots)
	}
	srv.proxy.SetPoolSize(app.Slug, 2)
	if err := srv.proxy.RegisterReplica(app.Slug, 1, info.EndpointURL, nil, dep.ID); err != nil {
		t.Fatal(err)
	}
	id, err := srv.store.CreateSchedule(db.CreateScheduleParams{AppID: app.ID, Name: "refresh", CronExpr: "0 * * * *", Enabled: true, CommandJSON: `["sh","-c","exit 1"]`, TimeoutSeconds: 10, OverlapPolicy: "skip", MissedPolicy: "skip", DeployTrigger: "bundle_change", OnSuccess: "none", RollFallback: "defer"})
	if err != nil {
		t.Fatal(err)
	}
	runID, err := jm.Run(id, "manual", nil)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		run, err := srv.store.GetScheduleRun(runID)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status != "running" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("producer never finished")
		}
		time.Sleep(5 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := jm.InterruptAndDrain(ctx); err != nil {
		t.Fatal(err)
	}
	run, err := srv.store.GetScheduleRun(runID)
	if err != nil || run.Status != "failed" {
		t.Fatalf("producer did not execute and fail: run=%+v err=%v", run, err)
	}
	if _, ok := srv.manager.GetReplica(app.Slug, 1); ok {
		t.Fatal("failed producer left its consumer tracked")
	}
	if target := srv.proxy.ReplicaTargetURL(app.Slug, 1); target != "" {
		t.Fatalf("quarantined consumer remains routable: %s", target)
	}
	rows, err := srv.store.ListReplicas(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[1].PID != nil || rows[1].Port != nil || rows[1].Status != "stopped" || rows[1].DesiredState != "running" {
		t.Fatalf("consumer identity or recovery intent after stop: %+v", rows)
	}
	var audits int
	if err := srv.store.DB().QueryRow(`SELECT COUNT(*) FROM audit_events WHERE action='schedule_run_failed'`).Scan(&audits); err != nil || audits != 1 {
		t.Fatalf("failure callback did not finish auditing: count=%d err=%v", audits, err)
	}
}
