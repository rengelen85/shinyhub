package api

import (
	"context"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/autoscale"
	"github.com/rvben/shinyhub/internal/config"
	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/process"
)

func enableWarmRetirement(t *testing.T, s *Server, appID int64) {
	t.Helper()
	if err := s.store.SetAppAutoscale(db.SetAppAutoscaleParams{AppID: appID, Enabled: true, MinReplicas: 1, MaxReplicas: 4, Target: 0.6}); err != nil {
		t.Fatal(err)
	}
	if err := s.store.UpdateAppMinWarmReplicas(appID, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.proxy.RegisterReplica("demo", 0, "http://127.0.0.1:9000", nil, 0); err != nil {
		t.Fatal(err)
	}
}

func TestRetireWarmReplica_RechecksChangedState(t *testing.T) {
	for _, change := range []string{"expanded", "resized", "warm floor raised", "autoscale floor raised", "autoscale disabled", "survivor missing", "fleet survivor missing", "stopped"} {
		t.Run(change, func(t *testing.T) {
			s, app, _ := newWarmExpandServer(t, "demo", 2, []int{1}, nil)
			enableWarmRetirement(t, s, app.ID)
			switch change {
			case "expanded":
				if changed, err := s.WarmExpand("demo"); err != nil || !changed {
					t.Fatalf("expand = %v, %v", changed, err)
				}
			case "resized":
				if err := s.store.UpdateAppReplicas(app.ID, 3); err != nil {
					t.Fatal(err)
				}
				if err := s.store.UpsertReplica(db.UpsertReplicaParams{AppID: app.ID, Index: 2, Status: "stopped", DesiredState: db.ReplicaDesiredWarm, Tier: "default"}); err != nil {
					t.Fatal(err)
				}
			case "warm floor raised":
				if err := s.store.UpdateAppMinWarmReplicas(app.ID, 2); err != nil {
					t.Fatal(err)
				}
			case "autoscale floor raised", "autoscale disabled":
				min := 1
				if change == "autoscale floor raised" {
					min = 2
				}
				if err := s.store.SetAppAutoscale(db.SetAppAutoscaleParams{AppID: app.ID, Enabled: change != "autoscale disabled", MinReplicas: min, MaxReplicas: 4, Target: 0.6}); err != nil {
					t.Fatal(err)
				}
			case "survivor missing":
				s.proxy.DeregisterReplicaIfTarget("demo", 0, s.proxy.ReplicaTargetURL("demo", 0))
			case "fleet survivor missing":
				s.SetCluster("owner")
				if err := s.store.UpsertReplicaSessions("peer", []db.ReplicaSessionRow{{AppID: app.ID, Idx: 1, Active: 0}}); err != nil {
					t.Fatal(err)
				}
			case "stopped":
				if err := s.store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: "demo", Status: "stopped"}); err != nil {
					t.Fatal(err)
				}
			}
			before, err := s.store.GetAppBySlug("demo")
			if err != nil {
				t.Fatal(err)
			}
			if changed, err := s.RetireWarmReplica("demo", 2); err != nil || changed {
				t.Fatalf("retire stale decision = %v, %v; want false, nil", changed, err)
			}
			after, err := s.store.GetAppBySlug("demo")
			if err != nil || after.Replicas != before.Replicas {
				t.Fatalf("replicas changed: before=%+v after=%+v err=%v", before, after, err)
			}
			rows, err := s.store.ListReplicas(app.ID)
			if err != nil || len(rows) != before.Replicas {
				t.Fatalf("replica rows lost: rows=%+v err=%v", rows, err)
			}
		})
	}
}

// Freeze a real process, then prove retirement thaws only to terminate it. No
// consumer start or health check is involved, and tier placement shrinks too.
type retirementSnapshotter struct {
	*process.NativeRuntime
	resumes int
}

func (r *retirementSnapshotter) Suspend(_ context.Context, h process.RunHandle) (bool, error) {
	err := syscall.Kill(h.PID, syscall.SIGSTOP)
	return err == nil, err
}
func (r *retirementSnapshotter) Resume(_ context.Context, h process.RunHandle) (process.ReplicaEndpoint, error) {
	r.resumes++
	return process.ReplicaEndpoint{Provider: "native", Handle: h}, syscall.Kill(h.PID, syscall.SIGCONT)
}

func TestRetireWarmReplica_ReapsSuspendedProcessAndPlacement(t *testing.T) {
	cfg := &config.Config{}
	cfg.Runtime.Tiers = []config.TierConfig{{Name: "local", Runtime: "native"}, {Name: "burst", Runtime: "native"}}
	s, app := newScaleTestServer(t, "demo", 2, cfg)
	s.proxy.SetPoolSize("demo", 2)
	enableWarmRetirement(t, s, app.ID)
	if err := s.store.SetAppPlacement(app.ID, `{"burst":2}`, 2); err != nil {
		t.Fatal(err)
	}
	rows, err := s.store.ListReplicas(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if err := s.store.UpsertReplica(db.UpsertReplicaParams{AppID: app.ID, Index: row.Index, PID: row.PID, Port: row.Port, Status: row.Status, Provider: row.Provider, Tier: "burst", DesiredState: row.DesiredState, DeploymentID: row.DeploymentID}); err != nil {
			t.Fatal(err)
		}
	}
	rt := &retirementSnapshotter{NativeRuntime: process.NewNativeRuntime()}
	s.manager.RegisterRuntime("burst", rt)
	info, err := s.manager.Start(process.StartParams{Slug: "demo", Index: 1, Tier: "burst", Dir: t.TempDir(), Command: []string{"sleep", "30"}, Port: 19491})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.manager.StopReplica("demo", 1) })
	if changed, err := s.WarmShrink("demo", 1, time.Millisecond); err != nil || !changed {
		t.Fatalf("shrink = %v, %v", changed, err)
	}
	rows, err = s.store.ListReplicas(app.ID)
	if err != nil || rows[1].Status != "suspended" {
		t.Fatalf("not suspended: rows=%+v err=%v", rows, err)
	}
	if changed, err := s.RetireWarmReplica("demo", 2); err != nil || !changed {
		t.Fatalf("retire = %v, %v", changed, err)
	}
	if err := syscall.Kill(info.PID, 0); err == nil {
		t.Fatal("suspended victim process survived retirement")
	}
	if _, exists := s.manager.GetReplica("demo", 1); exists {
		t.Fatal("manager retained retired victim")
	}
	if rt.resumes != 1 {
		t.Fatalf("resume calls=%d; want one thaw for termination", rt.resumes)
	}
	stored, err := s.store.GetAppBySlug("demo")
	if err != nil || stored.Replicas != 1 || stored.PlacementMap()["burst"] != 1 {
		t.Fatalf("placement did not shrink: app=%+v err=%v", stored, err)
	}
	rows, err = s.store.ListReplicas(app.ID)
	if err != nil || len(rows) != 1 || rows[0].Index != 0 {
		t.Fatalf("retired row survived: rows=%+v err=%v", rows, err)
	}
	if counts := s.proxy.ReplicaSessionCounts("demo"); len(counts) != 1 || counts[0] < 0 {
		t.Fatalf("routing did not shrink: %v", counts)
	}
	if changed, err := s.WarmExpand("demo"); err != nil || changed {
		t.Fatalf("expand after retirement = %v, %v", changed, err)
	}
}

func TestRetireWarmReplica_StopFailurePreservesWarmIntent(t *testing.T) {
	s, app, _ := newWarmExpandServer(t, "demo", 2, []int{1}, nil)
	enableWarmRetirement(t, s, app.ID)
	s.manager.RegisterRuntime("default", &stopFailRuntime{})
	if _, err := s.manager.Start(process.StartParams{Slug: "demo", Index: 1, Tier: "default", Dir: t.TempDir(), Command: []string{"sleep", "30"}, Port: 19492}); err != nil {
		t.Fatal(err)
	}
	if changed, err := s.RetireWarmReplica("demo", 2); err == nil || changed {
		t.Fatalf("failed stop = %v, %v", changed, err)
	}
	rows, err := s.store.ListReplicas(app.ID)
	if err != nil || len(rows) != 2 || rows[1].DesiredState != db.ReplicaDesiredWarm {
		t.Fatalf("failed retirement lost warm intent: rows=%+v err=%v", rows, err)
	}
	stored, err := s.store.GetAppBySlug("demo")
	if err != nil || stored.Replicas != 2 {
		t.Fatalf("failed retirement changed count: app=%+v err=%v", stored, err)
	}
	if counts := s.proxy.ReplicaSessionCounts("demo"); len(counts) != 2 || counts[1] != -1 {
		t.Fatalf("parked victim became routable: %v", counts)
	}
}

// Force expansion precisely after the controller's parked-state read and before
// the primitive acquires its lock. Retirement must no-op, without cooldown.
type expandBeforeRetirementScaler struct {
	*Server
	before func()
	cancel context.CancelFunc
	result chan error
}

func (s *expandBeforeRetirementScaler) RetireWarmReplica(slug string, expected int) (bool, error) {
	s.before()
	changed, err := s.Server.RetireWarmReplica(slug, expected)
	s.cancel()
	if changed && err == nil {
		err = fmt.Errorf("retired a replica that expanded after the decision")
	}
	s.result <- err
	return changed, err
}

// Cancellation can race a ready ticker in Run. Supply one app snapshot so this
// regression observes exactly the invalidated decision, not a later fresh tick.
type singlePassAutoscaleLister struct {
	*db.Store
	listed bool
}

func (l *singlePassAutoscaleLister) ListAutoscaleApps() ([]*db.App, error) {
	if l.listed {
		return nil, nil
	}
	l.listed = true
	return l.Store.ListAutoscaleApps()
}

func TestWarmRetirement_ConcurrentExpandInvalidatesControllerDecision(t *testing.T) {
	s, app, _ := newWarmExpandServer(t, "demo", 2, []int{1}, nil)
	enableWarmRetirement(t, s, app.ID)
	ctx, cancel := context.WithCancel(context.Background())
	scaler := &expandBeforeRetirementScaler{Server: s, cancel: cancel, result: make(chan error, 1)}
	scaler.before = func() {
		if changed, err := s.WarmExpand("demo"); err != nil || !changed {
			t.Errorf("concurrent expand = %v, %v", changed, err)
		}
	}
	controller := autoscale.New(autoscale.Config{ScanInterval: time.Millisecond, DefaultCap: 75, DefaultTarget: 0.6, RuntimeMax: 4}, &singlePassAutoscaleLister{Store: s.store}, s.proxy, scaler, s.store, s.store, nil)
	done := make(chan struct{})
	go func() { defer close(done); controller.Run(ctx) }()
	defer func() { cancel(); <-done }()
	select {
	case err := <-scaler.result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("controller did not attempt parked retirement")
	}
	<-done
	stored, err := s.store.GetAppBySlug("demo")
	if err != nil || stored.Replicas != 2 || stored.LastAutoscaleAt != 0 {
		t.Fatalf("stale retirement changed app/cooldown: app=%+v err=%v", stored, err)
	}
	rows, err := s.store.ListReplicas(app.ID)
	if err != nil || len(rows) != 2 || rows[1].Status != "running" || rows[1].DesiredState != "running" {
		t.Fatalf("expanded row damaged: rows=%+v err=%v", rows, err)
	}
	if counts := s.proxy.ReplicaSessionCounts("demo"); len(counts) != 2 || counts[0] < 0 || counts[1] < 0 {
		t.Fatalf("expanded routes damaged: %v", counts)
	}
}

func TestRetireWarmReplica_RechecksUnderScaleLock(t *testing.T) {
	s, app, _ := newWarmExpandServer(t, "demo", 2, []int{1}, nil)
	enableWarmRetirement(t, s, app.ID)
	release := s.acquireDeployLock("demo")
	locked := true
	defer func() {
		if locked {
			release()
		}
	}()
	result := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		changed, err := s.RetireWarmReplica("demo", 2)
		if changed && err == nil {
			err = fmt.Errorf("removed a replica expanded while waiting for the lock")
		}
		result <- err
	}()
	<-started
	select {
	case err := <-result:
		t.Fatalf("retirement completed while scale lock held: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	// Simulate expansion's committed row and route while it owns this same lock.
	rows, err := s.store.ListReplicas(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	row := rows[1]
	if err := s.store.UpsertReplica(db.UpsertReplicaParams{AppID: app.ID, Index: 1, PID: row.PID, Port: row.Port, Status: "running", Provider: row.Provider, Tier: row.Tier, DesiredState: "running", DeploymentID: row.DeploymentID}); err != nil {
		t.Fatal(err)
	}
	if err := s.proxy.RegisterReplica("demo", 1, "http://127.0.0.1:9001", nil, 0); err != nil {
		t.Fatal(err)
	}
	release()
	locked = false
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("retirement did not finish after unlocking")
	}
	stored, err := s.store.GetAppBySlug("demo")
	if err != nil || stored.Replicas != 2 {
		t.Fatalf("stale retirement changed count: app=%+v err=%v", stored, err)
	}
}
