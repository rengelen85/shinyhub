package lifecycle_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/fargate"
	"github.com/rvben/shinyhub/internal/lifecycle"
	"github.com/rvben/shinyhub/internal/process"
	"github.com/rvben/shinyhub/internal/proxy"
)

type generationRecoveryRuntime struct {
	*fakeRemoteRuntime
	done    chan struct{}
	once    sync.Once
	signals chan process.RunHandle
}

func (r *generationRecoveryRuntime) Signal(handle process.RunHandle, _ syscall.Signal) error {
	if r.signals != nil {
		r.signals <- handle
	}
	r.once.Do(func() { close(r.done) })
	return nil
}

func (r *generationRecoveryRuntime) Wait(ctx context.Context, _ process.RunHandle) error {
	select {
	case <-r.done:
	case <-ctx.Done():
	}
	return nil
}

func TestRemoteRecoveryRetainsGenerationOwnership(t *testing.T) {
	for _, tc := range []struct{ name, workerID, endpoint string }{
		{"remote worker", "node-b", "https://b:8443/live"},
		{"ECS waiting for IP", fargate.WorkerID, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := mustOpenStore(t)
			app := mustCreateApp(t, store, "remote-generation")
			dep, err := store.CreateDeployment(db.CreateDeploymentParams{AppID: app.ID, Version: "v1", BundleDir: t.TempDir(), Status: db.DeploymentSucceeded})
			if err != nil {
				t.Fatal(err)
			}
			if err := store.UpsertReplica(db.UpsertReplicaParams{AppID: app.ID, Index: 0, Status: "running", DesiredState: "running", Provider: "remote_docker", Tier: "remote", WorkerID: tc.workerID, EndpointURL: tc.endpoint, DeploymentID: &dep.ID, AppVersion: dep.Version}); err != nil {
				t.Fatal(err)
			}
			if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: app.Slug, Status: "running"}); err != nil {
				t.Fatal(err)
			}
			rt := &generationRecoveryRuntime{done: make(chan struct{}), fakeRemoteRuntime: &fakeRemoteRuntime{items: []process.InventoryItem{{
				ContainerID: "surviving-container", WorkerID: tc.workerID, Running: true, URL: tc.endpoint,
				Labels: map[string]string{process.LabelSlug: app.Slug, process.LabelReplicaIndex: "0", process.LabelDeploymentID: strconv.FormatInt(dep.ID, 10)},
			}}}}
			mgr := process.NewManager(t.TempDir(), rt)
			t.Cleanup(func() { _ = mgr.StopAll() })
			lifecycle.RecoverProcesses(store, mgr, proxy.New(), 0, false, "multiplex", nil, mustPrepareRecovery(t, store))
			info, ok := mgr.GetGenerationReplica(app.Slug, dep.ID, 0)
			if !ok || info.DeploymentID != dep.ID || info.AppVersion != dep.Version {
				t.Fatalf("recovered container lost generation ownership: info=%+v found=%v; want deployment=%d version=%s", info, ok, dep.ID, dep.Version)
			}
			if err := mgr.StopGeneration(app.Slug, dep.ID); err != nil {
				t.Fatalf("cannot retire recovered generation: %v", err)
			}
		})
	}
}

func TestRemoteRecoveryAdoptsInFlightRollSurge(t *testing.T) {
	store := mustOpenStore(t)
	app := mustCreateApp(t, store, "remote-roll-surge")
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("surviving roll replica")) }))
	t.Cleanup(backend.Close)
	dep, err := store.BeginDeployment(app.ID, "v1", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PromoteDeployment(dep.ID); err != nil {
		t.Fatal(err)
	}
	scheduleID, err := store.CreateSchedule(db.CreateScheduleParams{
		AppID: app.ID, Name: "refresh", CronExpr: "0 * * * *", CommandJSON: `["true"]`,
		Enabled: true, TimeoutSeconds: 60, OverlapPolicy: "skip", MissedPolicy: "skip", OnSuccess: "roll",
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	runID, err := store.InsertScheduleRun(db.InsertScheduleRunParams{
		ScheduleID: scheduleID, Status: "running", Trigger: "schedule", StartedAt: now, OnSuccess: "roll",
	})
	if err != nil {
		t.Fatal(err)
	}
	exitCode := 0
	activation, err := store.CompleteScheduleRunAndEnqueueActivation(db.CompleteScheduleRunParams{
		RunID: runID, Status: "succeeded", ExitCode: &exitCode, FinishedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if activation == nil {
		t.Fatal("successful roll run did not create an activation")
	}
	claimed, err := store.ClaimNextScheduleActivation(now)
	if err != nil {
		t.Fatal(err)
	}
	if claimed == nil || claimed.ID != activation.ID {
		t.Fatal("roll activation was not claimed")
	}
	if err := store.UpdateScheduleActivationProgress(activation.ID, "draining", 1, 0); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertActivationReplica(db.UpsertReplicaParams{
		AppID: app.ID, Index: 1, Status: "running", DesiredState: "running", Provider: "remote_docker", Tier: "remote",
		WorkerID: "node-b", EndpointURL: backend.URL, DeploymentID: &dep.ID, AppVersion: dep.Version,
	}, activation.TargetGeneration, activation.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: app.Slug, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	rt := &generationRecoveryRuntime{done: make(chan struct{}), fakeRemoteRuntime: &fakeRemoteRuntime{items: []process.InventoryItem{{
		ContainerID: "surviving-surge", WorkerID: "node-b", Running: true, URL: backend.URL,
		Labels: map[string]string{process.LabelSlug: app.Slug, process.LabelReplicaIndex: "1", process.LabelDeploymentID: strconv.FormatInt(dep.ID, 10)},
	}}}}
	mgr := process.NewManager(t.TempDir(), rt)
	t.Cleanup(func() { _ = mgr.StopAll() })
	prx := proxy.New()
	lifecycle.RecoverProcesses(store, mgr, prx, 0, false, "multiplex", nil, mustPrepareRecovery(t, store))
	info, ok := mgr.GetGenerationReplica(app.Slug, dep.ID, 1)
	if !ok || info.EndpointURL != backend.URL {
		t.Fatalf("live roll surge was not recovered: info=%+v found=%v", info, ok)
	}
	rec := httptest.NewRecorder()
	prx.ServeHTTP(rec, httptest.NewRequest("GET", "/app/"+app.Slug+"/", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "surviving roll replica" {
		t.Fatalf("recovered surge cannot serve visitors: status=%d body=%s", rec.Code, rec.Body.String())
	}
	recovered, err := store.GetAppBySlug(app.Slug)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Status != "running" {
		t.Fatalf("serving roll surge was reported as %s", recovered.Status)
	}
}

func TestRemoteRecoveryDoesNotLetStoppedTaskHideLiveReplacement(t *testing.T) {
	for _, status := range []string{"running", "stopped"} {
		t.Run(status, func(t *testing.T) {
			store := mustOpenStore(t)
			app := mustCreateApp(t, store, "remote-replacement")
			dep, err := store.BeginDeployment(app.ID, "v1", t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := store.PromoteDeployment(dep.ID); err != nil {
				t.Fatal(err)
			}
			if err := store.UpsertReplica(db.UpsertReplicaParams{
				AppID: app.ID, Index: 0, Status: status, DesiredState: "running", Provider: "remote_docker", Tier: "remote",
				WorkerID: "node-b", EndpointURL: "https://b:8443/live", DeploymentID: &dep.ID, AppVersion: dep.Version,
			}); err != nil {
				t.Fatal(err)
			}
			if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: app.Slug, Status: "running"}); err != nil {
				t.Fatal(err)
			}
			labels := map[string]string{process.LabelSlug: app.Slug, process.LabelReplicaIndex: "0", process.LabelDeploymentID: strconv.FormatInt(dep.ID, 10)}
			rt := &generationRecoveryRuntime{done: make(chan struct{}), signals: make(chan process.RunHandle, 10), fakeRemoteRuntime: &fakeRemoteRuntime{items: []process.InventoryItem{
				{ContainerID: "terminated-task", WorkerID: "node-b", Running: false, Labels: labels},
				{ContainerID: "live-replacement", WorkerID: "node-b", Running: true, URL: "https://b:8443/live", Labels: labels},
			}}}
			mgr := process.NewManager(t.TempDir(), rt)
			t.Cleanup(func() { _ = mgr.StopAll() })
			lifecycle.RecoverProcesses(store, mgr, proxy.New(), 0, false, "multiplex", nil, mustPrepareRecovery(t, store))
			if status == "running" {
				info, ok := mgr.GetGenerationReplica(app.Slug, dep.ID, 0)
				if !ok || info.EndpointURL != "https://b:8443/live" {
					t.Fatalf("terminated task hid live replacement: info=%+v found=%v", info, ok)
				}
			} else {
				select {
				case <-rt.done:
					handle := <-rt.signals
					if handle.ContainerID != "node-b/live-replacement" {
						t.Fatalf("cleanup stopped the wrong task: %+v", handle)
					}
				default:
					t.Fatal("quarantine cleanup forgot runtime identity without stopping the live replacement")
				}
				rows, err := store.ListReplicas(app.ID)
				if err != nil {
					t.Fatal(err)
				}
				if len(rows) != 1 || rows[0].WorkerID != "" || rows[0].EndpointURL != "" {
					t.Fatalf("confirmed cleanup retained runtime identity: %+v", rows)
				}
			}
		})
	}
}

func TestRemoteRecoveryPreservesAmbiguousLiveOwnership(t *testing.T) {
	for _, status := range []string{"running", "stopped"} {
		t.Run(status, func(t *testing.T) {
			store := mustOpenStore(t)
			app := mustCreateApp(t, store, "remote-ambiguous")
			dep, err := store.BeginDeployment(app.ID, "v1", t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := store.PromoteDeployment(dep.ID); err != nil {
				t.Fatal(err)
			}
			if err := store.UpsertReplica(db.UpsertReplicaParams{
				AppID: app.ID, Index: 0, Status: status, DesiredState: "running", Provider: "remote_docker", Tier: "remote",
				WorkerID: "node-b", EndpointURL: "https://b:8443/recorded", DeploymentID: &dep.ID, AppVersion: dep.Version,
			}); err != nil {
				t.Fatal(err)
			}
			if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: app.Slug, Status: "running"}); err != nil {
				t.Fatal(err)
			}
			labels := map[string]string{process.LabelSlug: app.Slug, process.LabelReplicaIndex: "0", process.LabelDeploymentID: strconv.FormatInt(dep.ID, 10)}
			rt := &generationRecoveryRuntime{done: make(chan struct{}), fakeRemoteRuntime: &fakeRemoteRuntime{items: []process.InventoryItem{
				{ContainerID: "candidate-a", WorkerID: "node-b", Running: true, URL: "https://b:8443/a", Labels: labels},
				{ContainerID: "candidate-b", WorkerID: "node-b", Running: true, URL: "https://b:8443/b", Labels: labels},
			}}}
			mgr := process.NewManager(t.TempDir(), rt)
			t.Cleanup(func() { _ = mgr.StopAll() })
			prx := proxy.New()
			lifecycle.RecoverProcesses(store, mgr, prx, 0, false, "multiplex", nil, mustPrepareRecovery(t, store))
			if prx.HasLiveReplica(app.Slug) {
				t.Error("ambiguous process was exposed to visitors")
			}
			if _, ok := mgr.GetReplica(app.Slug, 0); ok {
				t.Error("recovery adopted an arbitrary live process with ambiguous ownership")
			}
			select {
			case <-rt.done:
				t.Error("cleanup signalled an arbitrary live process with ambiguous ownership")
			default:
			}
			rows, err := store.ListReplicas(app.ID)
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 || rows[0].WorkerID != "node-b" || rows[0].EndpointURL != "https://b:8443/recorded" {
				t.Fatalf("ambiguous inventory erased durable ownership: %+v", rows)
			}
			recovered, err := store.GetAppBySlug(app.Slug)
			if err != nil {
				t.Fatal(err)
			}
			if recovered.Status != "running" {
				t.Fatalf("ambiguous live inventory allowed app to be treated as down: %s", recovered.Status)
			}
		})
	}
}
