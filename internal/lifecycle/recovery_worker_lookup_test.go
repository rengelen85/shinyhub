package lifecycle_test

import (
	"context"
	"testing"

	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/lifecycle"
	"github.com/rvben/shinyhub/internal/process"
	"github.com/rvben/shinyhub/internal/proxy"
)

type workerLookupFailureInventory struct {
	*fakeRemoteRuntime
	failLookup func()
}

func (r *workerLookupFailureInventory) Inventory(ctx context.Context) ([]process.InventoryItem, error) {
	r.failLookup()
	return r.fakeRemoteRuntime.Inventory(ctx)
}

func TestRecoveryDoesNotReplaceUnreachableWorkerWhenRegistryLookupFails(t *testing.T) {
	store := mustOpenStore(t)
	app := mustCreateApp(t, store, "unknown-worker")
	if err := store.UpsertWorker(db.Worker{NodeID: "node-b", AdvertiseAddr: "b:8443", Tier: "remote", Status: "up"}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertReplica(db.UpsertReplicaParams{AppID: app.ID, Index: 0, Status: "running", DesiredState: "running", Provider: "remote_docker", Tier: "remote", WorkerID: "node-b", EndpointURL: "https://b:8443/live"}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: app.Slug, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	remote := &workerLookupFailureInventory{
		fakeRemoteRuntime: &fakeRemoteRuntime{err: &process.PartialInventoryError{Workers: []string{"node-b"}}},
		failLookup: func() {
			if _, err := store.DB().Exec(`ALTER TABLE workers RENAME TO unavailable_workers`); err != nil {
				t.Fatal(err)
			}
		},
	}
	t.Cleanup(func() {
		if _, err := store.DB().Exec(`ALTER TABLE unavailable_workers RENAME TO workers`); err != nil {
			t.Errorf("restore worker registry: %v", err)
		}
	})
	mgr := process.NewManager(t.TempDir(), process.NewNativeRuntime())
	mgr.RegisterRuntime("remote", remote)
	lifecycle.RecoverProcesses(store, mgr, proxy.New(), 0, false, "multiplex", nil, mustPrepareRecovery(t, store))
	replicas, err := store.ListReplicas(app.ID)
	if err != nil || len(replicas) != 1 {
		t.Fatalf("replicas=%+v err=%v", replicas, err)
	}
	if r := replicas[0]; r.Status != "running" || r.WorkerID != "node-b" || r.EndpointURL != "https://b:8443/live" {
		t.Fatalf("unknown registry state allowed replacement of a potentially live worker: %+v", r)
	}
	current, err := store.GetAppByID(app.ID)
	if err != nil || current.Status != "running" {
		t.Fatalf("unknown worker state changed serving intent: app=%+v err=%v", current, err)
	}
}
