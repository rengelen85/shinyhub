package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rvben/shinyhub/internal/auth"
	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/process"
)

// A roll can finish starting a process before its durable replica checkpoint.
// The observation must describe one process, rather than combining its new PID
// with the previous process's endpoint (or an empty stopped-row endpoint).
func TestGetAppReportsLiveEndpointBeforeReplicaCheckpoint(t *testing.T) {
	srv, store, mgr := newManagerTestServer(t)
	t.Cleanup(func() { _ = mgr.StopAll(); srv.Close() })
	if err := store.CreateUser(db.CreateUserParams{Username: "owner", PasswordHash: "unused", Role: "developer"}); err != nil {
		t.Fatal(err)
	}
	owner, err := store.GetUserByUsername("owner")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateApp(db.CreateAppParams{Slug: "endpoint", Name: "Endpoint", OwnerID: owner.ID}); err != nil {
		t.Fatal(err)
	}
	app, err := store.GetAppBySlug("endpoint")
	if err != nil {
		t.Fatal(err)
	}
	info, err := mgr.Start(process.StartParams{Slug: app.Slug, Index: 0, Dir: t.TempDir(), Command: []string{"sleep", "30"}, Port: 20987})
	if err != nil {
		t.Fatal(err)
	}
	token, err := auth.IssueJWT(owner.ID, owner.Username, owner.Role, "test-secret")
	if err != nil {
		t.Fatal(err)
	}
	for _, previous := range []string{"", "http://127.0.0.1:20986"} {
		t.Run("previous="+previous, func(t *testing.T) {
			if err := store.UpsertReplica(db.UpsertReplicaParams{AppID: app.ID, Index: 0, Status: "stopped", DesiredState: "running", EndpointURL: previous}); err != nil {
				t.Fatal(err)
			}
			rec := httptest.NewRecorder()
			srv.Router().ServeHTTP(rec, authedRequest(t, http.MethodGet, "/api/apps/endpoint", nil, token))
			if rec.Code != http.StatusOK {
				t.Fatalf("get app: %d %s", rec.Code, rec.Body.String())
			}
			var body struct {
				Replicas []*db.Replica `json:"replicas_status"`
			}
			if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if len(body.Replicas) != 1 {
				t.Fatalf("replicas: %+v", body.Replicas)
			}
			rep := body.Replicas[0]
			if rep.Status != "running" || rep.PID == nil || *rep.PID != info.PID || rep.EndpointURL != "http://127.0.0.1:20987" {
				t.Fatalf("live process observation mixes runtime identities: %+v", rep)
			}
		})
	}
}
