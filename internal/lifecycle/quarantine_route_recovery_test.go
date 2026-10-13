package lifecycle

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/process"
	"github.com/rvben/shinyhub/internal/proxy"
)

type quarantineHTTPTransport struct{}

func (quarantineHTTPTransport) TransportForReplica(*db.Replica) (http.RoundTripper, error) {
	return http.DefaultTransport, nil
}

type unavailableReplicaTransport struct{}

func (unavailableReplicaTransport) TransportForReplica(*db.Replica) (http.RoundTripper, error) {
	return nil, errors.New("replacement worker is unavailable")
}

func seedSurvivingReplica(t *testing.T) (*db.Store, *db.App) {
	t.Helper()
	store, app := seedWarmApp(t)
	old, err := store.BeginDeployment(app.ID, "old", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = store.PromoteDeployment(old.ID); err != nil {
		t.Fatal(err)
	}
	if err = store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: app.Slug, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("unsafe data")) }))
	t.Cleanup(backend.Close)
	if err = store.UpsertReplica(db.UpsertReplicaParams{AppID: app.ID, Index: 0, Status: "running", Provider: "docker", Tier: process.DefaultTier, EndpointURL: backend.URL, WorkerID: "surviving-worker", DesiredState: "running", DeploymentID: &old.ID}); err != nil {
		t.Fatal(err)
	}
	return store, app
}

func failCompatibilityDeployment(t *testing.T, store *db.Store, app *db.App) {
	t.Helper()
	candidate, err := store.BeginDeployment(app.ID, "new", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = store.MarkDeploymentProducerBarrierEntered(candidate.ID); err != nil {
		t.Fatal(err)
	}
	if err = store.QuarantineAndFailDeployment(candidate.ID, "producer damaged shared data"); err != nil {
		t.Fatal(err)
	}
	if err = store.EnforceCompatibilityQuarantines(); err != nil {
		t.Fatal(err)
	}
	blocked, err := store.AppCompatibilityQuarantined(app.ID)
	if err != nil || !blocked {
		t.Fatalf("not quarantined: %v %v", blocked, err)
	}
}

func seedQuarantinedReplica(t *testing.T) (*db.Store, *db.App) {
	t.Helper()
	store, app := seedSurvivingReplica(t)
	failCompatibilityDeployment(t, store, app)
	app, err := store.GetAppByID(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	return store, app
}

func TestStartupDoesNotRouteCompatibilityQuarantine(t *testing.T) {
	store, app := seedQuarantinedReplica(t)
	p := proxy.New()
	p.SetAppStatusLookup(func(string) (string, string) { return app.Status, app.LastError })
	syncer := proxy.NewPoolSyncer(p, store, quarantineHTTPTransport{}, slog.Default(), false)
	mgr := process.NewManager(t.TempDir(), stubNonNativeRuntime{})
	syncer.SetReplicaValidator(ReplicaRouteValidator(store, mgr))
	syncer.RunOnce(context.Background())
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, httptest.NewRequest("GET", "/app/demo/", nil))
	if rec.Body.String() == "unsafe data" {
		t.Fatalf("failed quarantined app served shared data, status=%d route=%s", rec.Code, p.ReplicaTargetURL(app.Slug, 0))
	}
}

func TestRecoveryIncludesIndependentCompatibilityQuarantine(t *testing.T) {
	store, app := seedQuarantinedReplica(t)
	inputs, err := PrepareRecovery(store)
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range inputs.Apps {
		if candidate.ID == app.ID {
			return
		}
	}
	t.Fatal("failed app with a durable deployment quarantine and surviving replica omitted from startup cleanup")
}

func TestPoolSyncWithdrawsAlreadyAdoptedQuarantinedRoute(t *testing.T) {
	store, app := seedSurvivingReplica(t)
	p := proxy.New()
	p.SetAppStatusLookup(func(string) (string, string) {
		a, err := store.GetAppByID(app.ID)
		if err != nil {
			t.Fatal(err)
		}
		return a.Status, a.LastError
	})
	syncer := proxy.NewPoolSyncer(p, store, quarantineHTTPTransport{}, slog.Default(), false)
	syncer.SetReplicaValidator(ReplicaRouteValidator(store, process.NewManager(t.TempDir(), stubNonNativeRuntime{})))
	// Degraded pools must keep healthy survivors available.
	if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: app.Slug, Status: "degraded"}); err != nil {
		t.Fatal(err)
	}
	syncer.RunOnce(context.Background())
	before := httptest.NewRecorder()
	p.ServeHTTP(before, httptest.NewRequest("GET", "/app/demo/", nil))
	if before.Code != 200 || before.Body.String() != "unsafe data" {
		t.Fatalf("healthy survivor not served: %d %s", before.Code, before.Body.String())
	}
	failCompatibilityDeployment(t, store, app)
	syncer.RunOnce(context.Background())
	after := httptest.NewRecorder()
	p.ServeHTTP(after, httptest.NewRequest("GET", "/app/demo/", nil))
	if after.Body.String() == "unsafe data" || p.ReplicaTargetURL(app.Slug, 0) != "" {
		t.Fatalf("previously adopted route still serves quarantined data: %d %s", after.Code, after.Body.String())
	}
}

func TestPoolSyncKeepsServingDuringHealthyProducerRun(t *testing.T) {
	store, app := seedSurvivingReplica(t)
	p := proxy.New()
	syncer := proxy.NewPoolSyncer(p, store, quarantineHTTPTransport{}, slog.Default(), false)
	syncer.SetReplicaValidator(ReplicaRouteValidator(store, process.NewManager(t.TempDir(), stubNonNativeRuntime{})))
	syncer.RunOnce(context.Background())
	schedule, err := store.CreateSchedule(db.CreateScheduleParams{AppID: app.ID, Name: "refresh", CronExpr: "0 * * * *", CommandJSON: `["refresh"]`, Enabled: true, DeployTrigger: "first_deploy", OnSuccess: "roll", RollFallback: "defer"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.InsertScheduleRun(db.InsertScheduleRunParams{ScheduleID: schedule, Status: "running", Trigger: "schedule", StartedAt: time.Now().UTC(), PublishesData: true, OnSuccess: "roll", RollFallback: "defer"}); err != nil {
		t.Fatal(err)
	}
	syncer.RunOnce(context.Background())
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, httptest.NewRequest("GET", "/app/demo/", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "unsafe data" {
		t.Fatalf("healthy refresh interrupted the existing consumer: %d %s", rec.Code, rec.Body.String())
	}
}

func TestPoolSyncDoesNotKeepRetiredEndpointWhenReplacementIsNotReady(t *testing.T) {
	store, app := seedSurvivingReplica(t)
	p := proxy.New()
	syncer := proxy.NewPoolSyncer(p, store, quarantineHTTPTransport{}, slog.Default(), false)
	syncer.SetReplicaValidator(ReplicaRouteValidator(store, process.NewManager(t.TempDir(), stubNonNativeRuntime{})))
	syncer.RunOnce(context.Background())
	old := p.ReplicaTargetURL(app.Slug, 0)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer backend.Close()
	r := replicaAt(t, store, app.ID, 0)
	if err := store.UpsertReplica(db.UpsertReplicaParams{AppID: app.ID, Index: 0, Status: "running", Provider: "docker", Tier: process.DefaultTier, EndpointURL: backend.URL, WorkerID: "replacement-worker", DesiredState: "running", DeploymentID: r.DeploymentID}); err != nil {
		t.Fatal(err)
	}
	syncer.RunOnce(context.Background())
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, httptest.NewRequest("GET", "/app/demo/", nil))
	if rec.Body.String() == "unsafe data" || p.ReplicaTargetURL(app.Slug, 0) == old {
		t.Fatalf("unready replacement left retired endpoint routable: status=%d target=%s", rec.Code, p.ReplicaTargetURL(app.Slug, 0))
	}
}

func TestPoolSyncDoesNotKeepRetiredEndpointWhenReplacementTransportFails(t *testing.T) {
	store, app := seedSurvivingReplica(t)
	p := proxy.New()
	proxy.NewPoolSyncer(p, store, quarantineHTTPTransport{}, slog.Default(), false).RunOnce(context.Background())
	r := replicaAt(t, store, app.ID, 0)
	if err := store.UpsertReplica(db.UpsertReplicaParams{AppID: app.ID, Index: 0, Status: "running", Provider: "docker", Tier: process.DefaultTier, EndpointURL: "http://127.0.0.1:1", WorkerID: "replacement-worker", DesiredState: "running", DeploymentID: r.DeploymentID}); err != nil {
		t.Fatal(err)
	}
	proxy.NewPoolSyncer(p, store, unavailableReplicaTransport{}, slog.Default(), false).RunOnce(context.Background())
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, httptest.NewRequest("GET", "/app/demo/", nil))
	if rec.Body.String() == "unsafe data" {
		t.Fatalf("unavailable replacement worker left retired endpoint serving: %d", rec.Code)
	}
}

func TestFailedAdoptionPreservesConcurrentReplacement(t *testing.T) {
	t.Run("new endpoint", func(t *testing.T) { checkConcurrentReplacement(t, false) })
	t.Run("reused endpoint", func(t *testing.T) { checkConcurrentReplacement(t, true) })
}

func checkConcurrentReplacement(t *testing.T, reuseEndpoint bool) {
	t.Helper()
	store, app := seedSurvivingReplica(t)
	p := proxy.New()
	syncer := proxy.NewPoolSyncer(p, store, quarantineHTTPTransport{}, slog.Default(), false)
	syncer.SetReplicaValidator(ReplicaRouteValidator(store, process.NewManager(t.TempDir(), stubNonNativeRuntime{})))
	syncer.RunOnce(context.Background())
	oldEndpoint := p.ReplicaTargetURL(app.Slug, 0)
	entered, release := make(chan struct{}), make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer backend.Close()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	r := replicaAt(t, store, app.ID, 0)
	if err := store.UpsertReplica(db.UpsertReplicaParams{AppID: app.ID, Index: 0, Status: "running", Provider: "docker", Tier: process.DefaultTier, EndpointURL: backend.URL, WorkerID: "replacement-worker", DesiredState: "running", DeploymentID: r.DeploymentID}); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { syncer.RunOnce(context.Background()); close(done) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("replacement readiness did not start")
	}
	current := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("current generation")) }))
	defer current.Close()
	endpoint, want := current.URL, "current generation"
	if reuseEndpoint {
		endpoint, want = oldEndpoint, "unsafe data"
	}
	latestDeployment := *r.DeploymentID + 1
	if err := p.RegisterReplica(app.Slug, 0, endpoint, nil, latestDeployment); err != nil {
		t.Fatal(err)
	}
	close(release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("replacement readiness did not finish")
	}
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, httptest.NewRequest("GET", "/app/demo/", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != want || p.ReplicaDeploymentID(app.Slug, 0) != latestDeployment {
		t.Fatalf("failed stale adoption discarded a concurrent replacement: %d %s", rec.Code, rec.Body.String())
	}
}

func TestRecoveryCleansStoppedCompatibilityQuarantineWithoutRestartingApp(t *testing.T) {
	store, app := seedQuarantinedReplica(t)
	if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: app.Slug, Status: "stopped"}); err != nil {
		t.Fatal(err)
	}
	if err := store.EnforceCompatibilityQuarantines(); err != nil {
		t.Fatal(err)
	}
	inputs, err := PrepareRecovery(store)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, candidate := range inputs.Apps {
		if candidate.ID == app.ID {
			found = true
			if candidate.Status != "stopped" {
				t.Fatalf("cleanup revived stopped app: %s", candidate.Status)
			}
		}
	}
	if !found {
		t.Fatal("operator-stopped app omitted from compatibility cleanup despite surviving replica")
	}
	current, err := store.GetAppByID(app.ID)
	if err != nil || current.Status != "stopped" {
		t.Fatalf("operator stop lost: %+v %v", current, err)
	}
}

func TestRouteAdoptionRejectsQuarantineEnteredDuringReadiness(t *testing.T) {
	store, app := seedSurvivingReplica(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-release; w.WriteHeader(200) }))
	defer backend.Close()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	replica := replicaAt(t, store, app.ID, 0)
	if err := store.UpsertReplica(db.UpsertReplicaParams{AppID: app.ID, Index: 0, Status: "running", Provider: "docker", Tier: process.DefaultTier, EndpointURL: backend.URL, WorkerID: replica.WorkerID, DesiredState: "running", DeploymentID: replica.DeploymentID}); err != nil {
		t.Fatal(err)
	}
	p := proxy.New()
	syncer := proxy.NewPoolSyncer(p, store, quarantineHTTPTransport{}, slog.Default(), false)
	syncer.SetReplicaValidator(ReplicaRouteValidator(store, process.NewManager(t.TempDir(), stubNonNativeRuntime{})))
	done := make(chan struct{})
	go func() { syncer.RunOnce(context.Background()); close(done) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("readiness probe did not start")
	}
	failCompatibilityDeployment(t, store, app)
	close(release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("route adoption did not finish")
	}
	if route := p.ReplicaTargetURL(app.Slug, 0); route != "" {
		t.Fatalf("quarantine entered during readiness but route was published: %s", route)
	}
}

func TestRecoveryStopsQuarantinedRuntimeWithoutRevivingStoppedApp(t *testing.T) {
	for _, status := range []string{"failed", "stopped"} {
		t.Run(status, func(t *testing.T) {
			store, app := seedQuarantinedReplica(t)
			if status == "stopped" {
				if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: app.Slug, Status: status, LastError: "operator stopped app"}); err != nil {
					t.Fatal(err)
				}
			}
			inputs, err := PrepareRecovery(store)
			if err != nil {
				t.Fatal(err)
			}
			mgr := process.NewManager(t.TempDir(), &quarantineDockerRuntime{})
			p := proxy.New()
			RecoverProcesses(store, mgr, p, 0, false, "multiplex", nil, inputs)
			rows, err := store.ListReplicas(app.ID)
			if err != nil || len(rows) != 1 {
				t.Fatalf("cleanup rows=%v err=%v", rows, err)
			}
			if rows[0].WorkerID != "" || rows[0].EndpointURL != "" || rows[0].Status != "stopped" || rows[0].DesiredState != "running" {
				t.Fatalf("confirmed cleanup did not clear runtime and retain intent: %+v", rows[0])
			}
			current, err := store.GetAppByID(app.ID)
			if err != nil || current.Status != status {
				t.Fatalf("cleanup revived app: %+v %v", current, err)
			}
		})
	}
}
