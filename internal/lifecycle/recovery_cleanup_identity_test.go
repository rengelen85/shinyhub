package lifecycle_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"syscall"
	"testing"

	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/lifecycle"
	"github.com/rvben/shinyhub/internal/process"
	"github.com/rvben/shinyhub/internal/proxy"
)

type cleanupIdentityRuntime struct {
	*fakeDockerRuntime
	done     chan struct{}
	once     sync.Once
	signals  chan process.RunHandle
	onSignal func()
}

func (r *cleanupIdentityRuntime) Signal(handle process.RunHandle, _ syscall.Signal) error {
	r.signals <- handle
	if r.onSignal != nil {
		r.onSignal()
	}
	r.once.Do(func() { close(r.done) })
	return nil
}
func (r *cleanupIdentityRuntime) Wait(ctx context.Context, _ process.RunHandle) error {
	select {
	case <-r.done:
	case <-ctx.Done():
	}
	return nil
}

func TestRecoveryCleanupTargetsRecordedContainerIdentity(t *testing.T) {
	for _, oldPresent := range []bool{false, true} {
		t.Run(strconv.FormatBool(oldPresent), func(t *testing.T) {
			store := mustOpenStore(t)
			app := mustCreateApp(t, store, "container-cleanup")
			dep, err := store.BeginDeployment(app.ID, "v1", t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := store.PromoteDeployment(dep.ID); err != nil {
				t.Fatal(err)
			}
			if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: app.Slug, Status: "running"}); err != nil {
				t.Fatal(err)
			}
			if err := store.UpsertReplica(db.UpsertReplicaParams{AppID: app.ID, Index: 0, Status: "stopped", DesiredState: "running", Provider: "docker", WorkerID: "recorded-container", EndpointURL: "http://127.0.0.1:20100", DeploymentID: &dep.ID}); err != nil {
				t.Fatal(err)
			}
			labels := map[string]string{process.LabelSlug: app.Slug, process.LabelReplicaIndex: "0", process.LabelDeploymentID: strconv.FormatInt(dep.ID, 10)}
			rt := &cleanupIdentityRuntime{fakeDockerRuntime: &fakeDockerRuntime{containers: []process.ContainerInfo{{ID: "replacement-container", State: "running", Labels: labels}}}, done: make(chan struct{}), signals: make(chan process.RunHandle, 10)}
			if oldPresent {
				rt.containers = append(rt.containers, process.ContainerInfo{ID: "recorded-container", State: "running", Labels: labels})
			}
			mgr := process.NewManager(t.TempDir(), rt)
			t.Cleanup(func() { _ = mgr.StopAll() })
			lifecycle.RecoverProcesses(store, mgr, proxy.New(), 0, false, "multiplex", nil, mustPrepareRecovery(t, store))
			select {
			case handle := <-rt.signals:
				if !oldPresent || handle.ContainerID != "recorded-container" {
					t.Fatalf("cleanup stopped an unowned replacement: %+v", handle)
				}
			default:
				if oldPresent {
					t.Fatal("same-labeled replacement prevented cleanup of the recorded container")
				}
			}
			rows, err := store.ListReplicas(app.ID)
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 || rows[0].WorkerID != "" || rows[0].EndpointURL != "" {
				t.Fatalf("confirmed cleanup retained stale identity: %+v", rows)
			}
		})
	}
}

func TestRecoveryCleanupPreservesReplacementWrittenDuringStop(t *testing.T) {
	store := mustOpenStore(t)
	app := mustCreateApp(t, store, "cleanup-replacement-race")
	dep, err := store.BeginDeployment(app.ID, "v1", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PromoteDeployment(dep.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: app.Slug, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertReplica(db.UpsertReplicaParams{AppID: app.ID, Index: 0, Status: "stopped", DesiredState: "running", Provider: "docker", WorkerID: "retiring-container", EndpointURL: "http://127.0.0.1:20100", DeploymentID: &dep.ID}); err != nil {
		t.Fatal(err)
	}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("replacement consumer")) }))
	t.Cleanup(backend.Close)
	prx := proxy.New()
	labels := map[string]string{process.LabelSlug: app.Slug, process.LabelReplicaIndex: "0", process.LabelDeploymentID: strconv.FormatInt(dep.ID, 10)}
	rt := &cleanupIdentityRuntime{fakeDockerRuntime: &fakeDockerRuntime{containers: []process.ContainerInfo{{ID: "retiring-container", State: "running", Labels: labels}}}, done: make(chan struct{}), signals: make(chan process.RunHandle, 10)}
	rt.onSignal = func() {
		if err := store.UpsertReplica(db.UpsertReplicaParams{AppID: app.ID, Index: 0, Status: "running", DesiredState: "running", Provider: "docker", WorkerID: "replacement-container", EndpointURL: backend.URL, DeploymentID: &dep.ID, ConsumerBooted: true}); err != nil {
			t.Error(err)
		}
		if err := prx.RegisterReplica(app.Slug, 0, backend.URL, nil, dep.ID, app.ID); err != nil {
			t.Error(err)
		}
	}
	mgr := process.NewManager(t.TempDir(), rt)
	t.Cleanup(func() { _ = mgr.StopAll() })
	lifecycle.RecoverProcesses(store, mgr, prx, 0, false, "multiplex", nil, mustPrepareRecovery(t, store))
	rows, err := store.ListReplicas(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Status != "running" || rows[0].WorkerID != "replacement-container" || rows[0].EndpointURL != backend.URL {
		t.Fatalf("old cleanup erased the replacement identity: %+v", rows)
	}
	rec := httptest.NewRecorder()
	prx.ServeHTTP(rec, httptest.NewRequest("GET", "/app/"+app.Slug+"/", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "replacement consumer" {
		t.Fatalf("old cleanup removed the replacement route: status=%d body=%s", rec.Code, rec.Body.String())
	}
}
