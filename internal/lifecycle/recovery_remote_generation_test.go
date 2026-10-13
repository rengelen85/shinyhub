package lifecycle_test

import (
	"context"
	"strconv"
	"sync"
	"syscall"
	"testing"

	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/fargate"
	"github.com/rvben/shinyhub/internal/lifecycle"
	"github.com/rvben/shinyhub/internal/process"
	"github.com/rvben/shinyhub/internal/proxy"
)

type generationRecoveryRuntime struct {
	*fakeRemoteRuntime
	done chan struct{}
	once sync.Once
}

func (r *generationRecoveryRuntime) Signal(process.RunHandle, syscall.Signal) error {
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
