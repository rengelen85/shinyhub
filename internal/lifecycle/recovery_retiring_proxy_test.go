package lifecycle_test

import (
	"testing"

	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/lifecycle"
	"github.com/rvben/shinyhub/internal/process"
	"github.com/rvben/shinyhub/internal/proxy"
)

func TestObsoleteGenerationCleanupWaitsForRetiringProxy(t *testing.T) {
	store := mustOpenStore(t)
	app := mustCreateApp(t, store, "parent-held-generation")
	old, err := store.BeginDeployment(app.ID, "old", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PromoteDeployment(old.ID); err != nil {
		t.Fatal(err)
	}
	next, err := store.BeginDeployment(app.ID, "new", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PromoteDeployment(next.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertDeploymentReplica(db.UpsertDeploymentReplicaParams{AppID: app.ID, DeploymentID: old.ID, Index: 0, Status: "running", Provider: "native"}); err != nil {
		t.Fatal(err)
	}
	mgr := process.NewManager(t.TempDir(), process.NewNativeRuntime())
	prx := proxy.New()
	done := make(chan struct{})
	prx.SetRetiringParent(done)
	lifecycle.CleanupObsoleteDeploymentGenerations(store, mgr, prx, app)
	rows, err := store.ListDeploymentReplicas(app.ID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("recovery discarded parent-held generation: %+v %v", rows, err)
	}
	close(done)
	lifecycle.CleanupObsoleteDeploymentGenerations(store, mgr, prx, app)
	rows, err = store.ListDeploymentReplicas(app.ID)
	if err != nil || len(rows) != 0 {
		t.Fatalf("cleanup did not resume after parent exit: %+v %v", rows, err)
	}
}

func TestContainerSweepProtectsUnadoptedDeploymentGeneration(t *testing.T) {
	store := mustOpenStore(t)
	app := mustCreateApp(t, store, "systemd-parent-held")
	dep, err := store.BeginDeployment(app.ID, "old", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertDeploymentReplica(db.UpsertDeploymentReplicaParams{
		AppID: app.ID, DeploymentID: dep.ID, Index: 0, Status: "running",
		Provider: "native", Tier: "isolated", WorkerID: "parent-held-unit",
	}); err != nil {
		t.Fatal(err)
	}
	mgr := process.NewManager(t.TempDir(), process.NewNativeRuntime())
	sweeper := &fakeSweeper{containers: []process.ContainerInfo{{ID: "parent-held-unit"}, {ID: "orphan-unit"}}}
	runtime := &tierSweepRuntime{blockingRuntime: &blockingRuntime{done: make(chan struct{})}, fakeSweeper: sweeper}
	t.Cleanup(func() { close(runtime.done) })
	mgr.RegisterRuntime("isolated", runtime)
	if err := lifecycle.SweepOrphanContainersForTiersWithGenerations(store, mgr, []string{"isolated"}); err != nil {
		t.Fatal(err)
	}
	if len(sweeper.removed) != 1 || sweeper.removed[0] != "orphan-unit" {
		t.Fatalf("sweep removed a durable generation: %v", sweeper.removed)
	}
	// Once the ledger owner confirms retirement and clears its record, a
	// leftover unit is genuinely orphaned and can be swept on recovery.
	if err := store.DeleteDeploymentReplicas(dep.ID); err != nil {
		t.Fatal(err)
	}
	sweeper.removed = nil
	if err := lifecycle.SweepOrphanContainersForTiersWithGenerations(store, mgr, []string{"isolated"}); err != nil {
		t.Fatal(err)
	}
	if len(sweeper.removed) != 2 {
		t.Fatalf("retired generation remained protected: %v", sweeper.removed)
	}
}
