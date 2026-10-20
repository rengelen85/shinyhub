package db_test

import (
	"errors"
	"testing"

	"github.com/rvben/shinyhub/internal/db"
)

func TestBlockedDeploymentPreservesReleaseAndIsNotRunnable(t *testing.T) {
	store := mustOpenDB(t)
	owner := mustCreateUser(t, store, "owner", "developer")
	app := mustCreateApp(t, store, "app", owner.ID)
	live, err := store.CreateDeployment(db.CreateDeploymentParams{AppID: app.ID, Version: "v1", BundleDir: "/b/v1"})
	if err != nil {
		t.Fatal(err)
	}
	pending, err := store.BeginDeployment(app.ID, "v2", "/b/v2")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetDeploymentDigest(pending.ID, "sha256:blocked"); err != nil {
		t.Fatal(err)
	}
	if err := store.BlockDeploymentWithReason(pending.ID, "capacity-blocked/cpu: waiting for draining predecessors"); err != nil {
		t.Fatal(err)
	}
	if err := store.BlockDeploymentWithReason(live.ID, "not pending"); err == nil {
		t.Fatal("status write reported success without transitioning a pending row")
	}
	// Generic deferred cleanup cannot turn an admission refusal into a failure.
	if err := store.FailDeployment(pending.ID); err != nil {
		t.Fatal(err)
	}
	history, err := store.ListDeploymentsBySlug(app.Slug)
	if err != nil || len(history) != 2 || history[0].Status != db.DeploymentBlocked {
		t.Fatalf("history = %+v, %v", history, err)
	}
	if history[0].ReleaseNumber != nil {
		t.Fatal("blocked attempt received a release number")
	}
	for _, list := range []func() ([]*db.Deployment, error){
		func() ([]*db.Deployment, error) { return store.ListDeployments(app.ID) },
		func() ([]*db.Deployment, error) { return store.ListRecentDeployments(app.ID, 2) },
	} {
		rows, err := list()
		if err != nil || len(rows) != 1 || rows[0].ID != live.ID {
			t.Fatalf("blocked attempt changed runnable release history: %+v %v", rows, err)
		}
	}
	if _, err := store.GetDeploymentByDigest("sha256:blocked"); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("blocked bundle eligible for remote start: %v", err)
	}
	if err := store.PromoteDeployment(pending.ID); err == nil {
		t.Fatal("blocked attempt could be promoted")
	}
	if rows, err := store.ListInflightDeployments(); err != nil || len(rows) != 0 {
		t.Fatalf("blocked attempt treated as queued work: %+v %v", rows, err)
	}
}

func TestBlockDeploymentRejectsProducerBarrier(t *testing.T) {
	store := mustOpenDB(t)
	owner := mustCreateUser(t, store, "owner", "developer")
	app := mustCreateApp(t, store, "app", owner.ID)
	pending, err := store.BeginDeployment(app.ID, "v1", "/b/v1")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkDeploymentProducerBarrierEntered(pending.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.BlockDeploymentWithReason(pending.ID, "capacity-blocked/cpu: no capacity"); err == nil {
		t.Fatal("unsafe refusal silently accepted")
	}
	if err := store.FailDeploymentWithReason(pending.ID, "real failure"); err != nil {
		t.Fatal(err)
	}
	history, err := store.ListDeploymentsBySlug(app.Slug)
	if err != nil || history[0].Status != db.DeploymentFailed {
		t.Fatalf("real failure was hidden: %+v %v", history, err)
	}
}
