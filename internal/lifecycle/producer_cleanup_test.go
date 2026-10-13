package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"

	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/dbtest"
	"github.com/rvben/shinyhub/internal/process"
)

func TestProducerCleanup_InventoryFailureRetainsIdentity(t *testing.T) {
	store := dbtest.New(t)
	app := cleanupTestApp(t, store)
	depID := int64(7)
	if err := store.UpsertReplica(db.UpsertReplicaParams{AppID: app.ID, Index: 0, Status: "stopped", DesiredState: "running", Provider: "remote_docker", WorkerID: "node-a", EndpointURL: "https://worker/app", DeploymentID: &depID}); err != nil {
		t.Fatal(err)
	}
	rt := &quarantineRemoteRuntime{err: errors.New("inventory unavailable")}
	mgr := process.NewManager(t.TempDir(), rt)
	rows, _ := store.ListReplicas(app.ID)
	if stopQuarantinedReplica(store, mgr, app, rows[0]) {
		t.Fatal("unknown inventory was treated as a confirmed stop")
	}
	rows, _ = store.ListReplicas(app.ID)
	if rows[0].WorkerID != "node-a" || rows[0].EndpointURL == "" {
		t.Fatal("unconfirmed identity was erased")
	}
	rt.err = nil
	rt.items = []process.InventoryItem{{ContainerID: "victim", WorkerID: "node-a", Running: true, Labels: map[string]string{process.LabelSlug: app.Slug, process.LabelReplicaIndex: "0", process.LabelDeploymentID: strconv.FormatInt(depID, 10)}}}
	if !stopQuarantinedReplica(store, mgr, app, rows[0]) {
		t.Fatal("reachable inventoried consumer was not cleaned up")
	}
	rows, _ = store.ListReplicas(app.ID)
	if rows[0].WorkerID != "" || rows[0].EndpointURL != "" || rows[0].DesiredState != "running" {
		t.Fatalf("cleanup lost running intent or retained runtime identity: %+v", rows[0])
	}
}

type quarantineDockerRuntime struct {
	quarantineDockerBase
	filter string
}

func (r *quarantineDockerRuntime) ListByLabel(filter string) ([]process.ContainerInfo, error) {
	r.filter = filter
	var parsed map[string][]string
	if err := json.Unmarshal([]byte(filter), &parsed); err != nil {
		return nil, err
	}
	return r.quarantineDockerBase.ListByLabel(filter)
}

func TestProducerCleanup_DockerUsesExactIdentity(t *testing.T) {
	store := dbtest.New(t)
	app := cleanupTestApp(t, store)
	depID := int64(7)
	if err := store.UpsertReplica(db.UpsertReplicaParams{AppID: app.ID, Index: 0, Status: "stopped", DesiredState: "running", Provider: "docker", WorkerID: "old-container", EndpointURL: "http://localhost/app", DeploymentID: &depID}); err != nil {
		t.Fatal(err)
	}
	rt := &quarantineDockerRuntime{quarantineDockerBase: quarantineDockerBase{listErr: context.DeadlineExceeded}}
	mgr := process.NewManager(t.TempDir(), rt)
	rows, _ := store.ListReplicas(app.ID)
	if stopQuarantinedReplica(store, mgr, app, rows[0]) {
		t.Fatal("unknown docker inventory treated as stopped")
	}
	if rt.filter != `{"label":["shinyhub.slug=producer-cleanup"]}` {
		t.Fatalf("invalid docker label filter: %s", rt.filter)
	}
	rt.listErr = nil
	if !stopQuarantinedReplica(store, mgr, app, rows[0]) {
		t.Fatal("absent docker victim did not clear")
	}
}

func cleanupTestApp(t *testing.T, store *db.Store) *db.App {
	t.Helper()
	if err := store.CreateUser(db.CreateUserParams{Username: "operator", PasswordHash: "hash", Role: "admin"}); err != nil {
		t.Fatal(err)
	}
	user, _ := store.GetUserByUsername("operator")
	if _, err := store.CreateApp(db.CreateAppParams{Slug: "producer-cleanup", Name: "producer-cleanup", OwnerID: user.ID}); err != nil {
		t.Fatal(err)
	}
	app, err := store.GetAppBySlug("producer-cleanup")
	if err != nil {
		t.Fatal(err)
	}
	return app
}

type quarantineRemoteRuntime struct {
	scriptedStopRuntime
	items []process.InventoryItem
	err   error
}

func (r *quarantineRemoteRuntime) Inventory(context.Context) ([]process.InventoryItem, error) {
	return r.items, r.err
}
func (r *quarantineRemoteRuntime) Wait(context.Context, process.RunHandle) error { return nil }

type quarantineDockerBase struct {
	scriptedStopRuntime
	listErr error
}

func (r *quarantineDockerBase) ListByLabel(string) ([]process.ContainerInfo, error) {
	return nil, r.listErr
}
func (r *quarantineDockerBase) InspectPID(string) (int, error)                { return 0, nil }
func (r *quarantineDockerBase) Wait(context.Context, process.RunHandle) error { return nil }

func (r *quarantineDockerBase) RemoveContainer(string) error { return nil }

var _ ContainerLister = (*quarantineDockerRuntime)(nil)
