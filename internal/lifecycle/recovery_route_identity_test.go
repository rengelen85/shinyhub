package lifecycle_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/lifecycle"
	"github.com/rvben/shinyhub/internal/process"
	"github.com/rvben/shinyhub/internal/proxy"
)

type replacingRecoveryInventory struct {
	*fakeDockerRuntime
	replace func()
}

func (r *replacingRecoveryInventory) ListByLabel(string) ([]process.ContainerInfo, error) {
	r.replace()
	return nil, nil // The old container has retired.
}

func TestRejectedRecoveryPreservesReplacementAtReusedEndpoint(t *testing.T) {
	store := mustOpenStore(t)
	app := mustCreateApp(t, store, "reused-endpoint")
	old, err := store.CreateDeployment(db.CreateDeploymentParams{AppID: app.ID, Version: "old", BundleDir: t.TempDir(), Status: db.DeploymentSucceeded})
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := store.BeginDeployment(app.ID, "new", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("replacement app"))
	}))
	defer backend.Close()
	if err := store.UpsertReplica(db.UpsertReplicaParams{AppID: app.ID, Index: 0, Status: "running", DesiredState: "running", Provider: "docker", WorkerID: "old-container", EndpointURL: backend.URL, DeploymentID: &old.ID}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: app.Slug, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	p := proxy.New()
	p.SetPoolAppID(app.Slug, app.ID)
	p.SetPoolSize(app.Slug, 1)
	if err := p.RegisterReplica(app.Slug, 0, backend.URL, nil, old.ID, app.ID); err != nil {
		t.Fatal(err)
	}
	rt := &replacingRecoveryInventory{fakeDockerRuntime: &fakeDockerRuntime{}, replace: func() {
		if err := p.RegisterReplica(app.Slug, 0, backend.URL, nil, replacement.ID, app.ID); err != nil {
			t.Fatal(err)
		}
	}}
	lifecycle.RecoverProcesses(store, process.NewManager(t.TempDir(), rt), p, 0, false, "multiplex", nil, mustPrepareRecovery(t, store))
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, httptest.NewRequest("GET", "/app/"+app.Slug+"/", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "replacement app" || p.ReplicaDeploymentID(app.Slug, 0) != replacement.ID {
		t.Fatalf("rejected old recovery removed a newer generation at the same endpoint: status=%d body=%s generation=%d", rec.Code, rec.Body.String(), p.ReplicaDeploymentID(app.Slug, 0))
	}
}
