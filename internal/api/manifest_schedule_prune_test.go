package api

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/deploy"
)

// deployPruneBundle uploads app.py plus, when manifest is non-empty, a
// shinyhub.toml, and returns the recorder.
func deployPruneBundle(t *testing.T, srv *Server, token, slug, manifest string) *httptest.ResponseRecorder {
	t.Helper()
	files := map[string]string{"app.py": "from shiny import App\n"}
	if manifest != "" {
		files["shinyhub.toml"] = manifest
	}
	body, ctype := buildMultiFileBundleUpload(t, files)
	req := httptest.NewRequest("POST", "/api/apps/"+slug+"/deploy", body)
	req.Header.Set("Content-Type", ctype)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-ShinyHub-Allow-Downtime", "1")
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, req)
	return rec
}

func pruneScheduleBlock(name string) string {
	return fmt.Sprintf("\n[[schedule]]\nname = %q\ncron = \"0 5 * * *\"\ncmd  = \"echo %s\"\n", name, name)
}

func newPruneApp(t *testing.T, slug string) (*Server, *db.Store, string, *db.App) {
	t.Helper()
	srv, store, token := newManifestE2EServer(t)
	admin, _ := store.GetUserByUsername("admin")
	if _, err := store.CreateApp(db.CreateAppParams{Slug: slug, Name: slug, OwnerID: admin.ID}); err != nil {
		t.Fatal(err)
	}
	app, err := store.GetAppBySlug(slug)
	if err != nil {
		t.Fatal(err)
	}
	return srv, store, token, app
}

func mustSchedule(t *testing.T, store *db.Store, appID int64, name string) *db.Schedule {
	t.Helper()
	schedule, err := store.GetScheduleByName(appID, name)
	if err != nil {
		t.Fatalf("schedule %q: %v", name, err)
	}
	return schedule
}

// The scenario from the issue: refresh-data is replaced in shinyhub.toml by
// two new schedules. The redeploy must stop refresh-data from firing, keep a
// schedule created by hand, report the change and audit it.
func TestDeploy_DisablesScheduleRemovedFromManifest(t *testing.T) {
	srv, store, token, app := newPruneApp(t, "pruned")
	if rec := deployPruneBundle(t, srv, token, "pruned", pruneScheduleBlock("refresh-data")); rec.Code != http.StatusOK {
		t.Fatalf("first deploy: %d %s", rec.Code, rec.Body.String())
	}
	original := mustSchedule(t, store, app.ID, "refresh-data")
	if original.Source != db.ScheduleSourceManifest || !original.Enabled {
		t.Fatalf("declared schedule: source=%q enabled=%v", original.Source, original.Enabled)
	}
	if _, err := store.CreateSchedule(db.CreateScheduleParams{
		AppID: app.ID, Name: "adhoc", CronExpr: "0 * * * *", CommandJSON: `["echo","adhoc"]`,
		Enabled: true, TimeoutSeconds: 60, OverlapPolicy: "skip", MissedPolicy: "skip",
	}); err != nil {
		t.Fatal(err)
	}

	rec := deployPruneBundle(t, srv, token, "pruned",
		pruneScheduleBlock("refresh-latest-data")+pruneScheduleBlock("refresh-timechart-data"))
	if rec.Code != http.StatusOK {
		t.Fatalf("second deploy: %d %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `{"name":"refresh-data","action":"disabled","schedule_id":`+fmt.Sprint(original.ID)+`}`) {
		t.Fatalf("deploy summary does not report refresh-data disabled: %s", rec.Body.String())
	}
	if got := mustSchedule(t, store, app.ID, "refresh-data"); got.Enabled || got.ID != original.ID {
		t.Fatalf("refresh-data after redeploy: enabled=%v id=%d, want disabled row %d", got.Enabled, got.ID, original.ID)
	}
	for _, name := range []string{"adhoc", "refresh-latest-data", "refresh-timechart-data"} {
		if !mustSchedule(t, store, app.ID, name).Enabled {
			t.Fatalf("%s was disabled", name)
		}
	}
	events, err := store.ListAuditEvents("", 50, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range events {
		if e.Action == "schedule_update" && e.ResourceID == fmt.Sprint(original.ID) && strings.Contains(e.Detail, "not declared in manifest") {
			found = true
		}
	}
	if !found {
		t.Fatal("no schedule_update audit event for the disabled schedule")
	}

	// The schedule list tells the operator which rows a manifest owns.
	req := httptest.NewRequest("GET", "/api/apps/pruned/schedules", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	list := httptest.NewRecorder()
	srv.Router().ServeHTTP(list, req)
	if !strings.Contains(list.Body.String(), `"name":"adhoc"`) || !strings.Contains(list.Body.String(), `"source":"manual"`) ||
		!strings.Contains(list.Body.String(), `"source":"manifest"`) {
		t.Fatalf("schedule list does not expose ownership: %s", list.Body.String())
	}
}

// A manifest that declares no schedules at all still owns the ones an earlier
// manifest declared; a deploy without any shinyhub.toml declares nothing and
// leaves them alone.
func TestDeploy_ManifestWithoutSchedulesDisablesManifestSchedules(t *testing.T) {
	srv, store, token, app := newPruneApp(t, "emptied")
	if rec := deployPruneBundle(t, srv, token, "emptied", pruneScheduleBlock("nightly")); rec.Code != http.StatusOK {
		t.Fatalf("first deploy: %d %s", rec.Code, rec.Body.String())
	}
	if rec := deployPruneBundle(t, srv, token, "emptied", ""); rec.Code != http.StatusOK {
		t.Fatalf("manifest-less deploy: %d %s", rec.Code, rec.Body.String())
	}
	if !mustSchedule(t, store, app.ID, "nightly").Enabled {
		t.Fatal("a bundle without shinyhub.toml disabled a manifest schedule")
	}
	rec := deployPruneBundle(t, srv, token, "emptied", "[app]\nhibernate_timeout_minutes = 30\n")
	if rec.Code != http.StatusOK {
		t.Fatalf("schedule-less manifest deploy: %d %s", rec.Code, rec.Body.String())
	}
	if mustSchedule(t, store, app.ID, "nightly").Enabled {
		t.Fatal("manifest without [[schedule]] left the earlier manifest schedule enabled")
	}
}

// A deploy that fails after the schedule batch committed restores the prior
// declarations, so the undeclared schedule is enabled again.
func TestDeploy_FailedDeployReenablesUndeclaredSchedule(t *testing.T) {
	srv, store, token, app := newPruneApp(t, "failing")
	if rec := deployPruneBundle(t, srv, token, "failing", pruneScheduleBlock("nightly")); rec.Code != http.StatusOK {
		t.Fatalf("first deploy: %d %s", rec.Code, rec.Body.String())
	}
	activations := 0
	srv.SetDeployRunForTest(func(p deploy.Params) (*deploy.PoolResult, error) {
		if p.PrepareOnly {
			return deploy.Run(p)
		}
		activations++
		if activations > 1 {
			// Recovery restarting the previous version after the failure.
			p.HealthCheck = func(string, time.Duration, http.RoundTripper) error { return nil }
			return deploy.Run(p)
		}
		if mustSchedule(t, store, app.ID, "nightly").Enabled {
			t.Error("schedule batch did not disable nightly before activation")
		}
		return nil, errors.New("candidate failed to boot")
	})
	if rec := deployPruneBundle(t, srv, token, "failing", pruneScheduleBlock("other")); rec.Code == http.StatusOK {
		t.Fatalf("deploy unexpectedly succeeded: %s", rec.Body.String())
	}
	if got := mustSchedule(t, store, app.ID, "nightly"); !got.Enabled || got.Source != db.ScheduleSourceManifest {
		t.Fatalf("nightly after failed deploy: enabled=%v source=%q, want enabled manifest row", got.Enabled, got.Source)
	}
}

// Preflight names the schedules the deploy would disable so a fleet plan can
// show them before anything changes, and leaves manual schedules out.
func TestDeployPreflight_ReportsSchedulesTheDeployWouldDisable(t *testing.T) {
	srv, store, token, app := newPruneApp(t, "planned")
	if rec := deployPruneBundle(t, srv, token, "planned", pruneScheduleBlock("refresh-data")+pruneScheduleBlock("keep")); rec.Code != http.StatusOK {
		t.Fatalf("first deploy: %d %s", rec.Code, rec.Body.String())
	}
	if _, err := store.CreateSchedule(db.CreateScheduleParams{
		AppID: app.ID, Name: "adhoc", CronExpr: "0 * * * *", CommandJSON: `["echo","adhoc"]`,
		Enabled: true, TimeoutSeconds: 60, OverlapPolicy: "skip", MissedPolicy: "skip",
	}); err != nil {
		t.Fatal(err)
	}
	reply := decodeDeployPreflight(t, postDeployPreflight(t, srv, token, "planned",
		preflightBody(t, pruneScheduleBlock("keep"), nil, "python")))
	if !reply.Valid || len(reply.DisabledSchedules) != 1 || reply.DisabledSchedules[0] != "refresh-data" {
		t.Fatalf("preflight = %+v, want valid with refresh-data to disable", reply)
	}
	if !mustSchedule(t, store, app.ID, "refresh-data").Enabled {
		t.Fatal("preflight changed a schedule")
	}
	reply = decodeDeployPreflight(t, postDeployPreflight(t, srv, token, "planned",
		preflightBody(t, "sparse = true\n[app]\nreplicas = 1\n", nil, "python")))
	if len(reply.DisabledSchedules) != 0 {
		t.Fatalf("sparse manifest preflight disables %v, want none", reply.DisabledSchedules)
	}
}
