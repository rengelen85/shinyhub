package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rvben/shinyhub/internal/deploy"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/db"
)

func TestDeployReconcilesOwnedSchedules(t *testing.T) {
	for _, tc := range []struct {
		name, manifest string
		sparse         bool
		legacy         bool
	}{
		{"empty", "[app]\nname='target'", false, false},
		{"absent", "", false, false},
		{"rename", "[[schedule]]\nname='new'\ncron='0 5 * * *'\ncmd='true'", false, false},
		{"sparse", "sparse=true\n[app]\nhibernate_timeout_minutes=10", true, false},
		{"legacy-removal", "", false, true},
		{"legacy-sparse", "sparse=true\n[app]\nhibernate_timeout_minutes=10", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, store, token, rt := newManifestE2EServerWithJobs(t)
			if _, err := store.CreateApp(db.CreateAppParams{Slug: "reconcile", Name: "reconcile", OwnerID: 1, Access: "private"}); err != nil {
				t.Fatal(err)
			}
			app, _ := store.GetAppBySlug("reconcile")
			deployBundle := func(manifest, code string) *httptest.ResponseRecorder {
				t.Helper()
				files := map[string]string{"app.py": code}
				if manifest != "" {
					files["shinyhub.toml"] = manifest
				}
				body, ct := buildMultiFileBundleUpload(t, files)
				req := httptest.NewRequest(http.MethodPost, "/api/apps/reconcile/deploy", body)
				req.Header.Set("Content-Type", ct)
				req.Header.Set("X-ShinyHub-Allow-Downtime", "1")
				req.Header.Set("Authorization", "Bearer "+token)
				rec := httptest.NewRecorder()
				srv.Router().ServeHTTP(rec, req)
				if rec.Code != http.StatusOK {
					t.Fatalf("deploy: %d %s", rec.Code, rec.Body.String())
				}
				return rec
			}
			deployBundle("[[schedule]]\nname='old'\ncron='0 5 * * *'\ncmd='true'\ndeploy_trigger='bundle_change'", "print('v1')")
			old, err := store.GetScheduleByName(app.ID, "old")
			if err != nil {
				t.Fatal(err)
			}
			if old.ManagedBy != db.ScheduleManagedByBundle {
				t.Fatalf("ownership=%q", old.ManagedBy)
			}
			if tc.legacy {
				if _, err := store.DB().Exec("UPDATE app_schedules SET managed_by='manual' WHERE id=?", old.ID); err != nil {
					t.Fatal(err)
				}
			}
			manualID, err := store.CreateSchedule(db.CreateScheduleParams{AppID: app.ID, Name: "manual", CronExpr: "0 6 * * *", CommandJSON: `["true"]`, Enabled: true, TimeoutSeconds: 60, OverlapPolicy: "skip", MissedPolicy: "skip"})
			if err != nil {
				t.Fatal(err)
			}
			rt.mu.Lock()
			runsBefore := len(rt.producerCommands)
			rt.mu.Unlock()
			// Plan reports retirement without changing the serving declaration.
			reply, err := srv.deployPreflight(app, deployPreflightRequest{Manifest: tc.manifest, AppType: "python"})
			if err != nil {
				t.Fatal(err)
			}
			wantChanges := 1
			if tc.sparse {
				wantChanges = 0
			}
			if len(reply.ScheduleChanges) != wantChanges {
				t.Fatalf("preflight=%+v", reply)
			}
			current, _ := store.GetSchedule(old.ID)
			if !current.Enabled || current.Retired {
				t.Fatal("preflight mutated declaration")
			}
			rec := deployBundle(tc.manifest, "print('v2')")
			current, err = store.GetSchedule(old.ID)
			if err != nil {
				t.Fatal(err)
			}
			if current.Retired == tc.sparse || current.Enabled != tc.sparse || current.ManagedBy != db.ScheduleManagedByBundle {
				t.Fatalf("schedule=%+v", current)
			}
			manual, err := store.GetSchedule(manualID)
			if err != nil || !manual.Enabled || manual.Retired || manual.ManagedBy != db.ScheduleManagedByManual {
				t.Fatalf("manual changed: %+v %v", manual, err)
			}
			rt.mu.Lock()
			runsAfter := len(rt.producerCommands)
			rt.mu.Unlock()
			if !tc.sparse && runsAfter != runsBefore {
				t.Fatalf("obsolete producer ran: %d -> %d", runsBefore, runsAfter)
			}
			history, err := store.ListScheduleRuns(old.ID, 20, 0)
			if err != nil || len(history) == 0 {
				t.Fatalf("lost history: %v", err)
			}
			if !tc.sparse {
				var response struct {
					Manifest ManifestApplied `json:"manifest"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
					t.Fatal(err)
				}
				found := false
				for _, result := range response.Manifest.Schedules {
					if result.Name == "old" && result.Action == "retired" {
						found = true
					}
				}
				if !found {
					t.Fatalf("missing retirement result: %s", rec.Body.String())
				}
				for _, action := range []struct{ method, path, body string }{{"POST", "/run", ""}, {"PATCH", "", `{"enabled":true}`}} {
					req := httptest.NewRequest(action.method, fmt.Sprintf("/api/apps/reconcile/schedules/%d", old.ID)+action.path, strings.NewReader(action.body))
					req.Header.Set("Authorization", "Bearer "+token)
					rr := httptest.NewRecorder()
					srv.Router().ServeHTTP(rr, req)
					if rr.Code != http.StatusConflict {
						t.Fatalf("retired action: %d %s", rr.Code, rr.Body.String())
					}
				}
			}
			if tc.name == "legacy-sparse" {
				deployBundle("", "print('v3')")
				current, err := store.GetSchedule(old.ID)
				if err != nil || !current.Retired {
					t.Fatalf("ownership lost across sparse upgrade: %+v %v", current, err)
				}
			}
			if tc.name == "absent" {
				deployBundle("[[schedule]]\nname='old'\ncron='0 5 * * *'\ncmd='true'\ndeploy_trigger='bundle_change'", "print('v3')")
				revived, err := store.GetScheduleByName(app.ID, "old")
				if err != nil || revived.ID != old.ID || revived.Retired || !revived.Enabled {
					t.Fatalf("revival=%+v %v", revived, err)
				}
			}
		})
	}
}

func TestDeployScheduleRetirementRestoresOnConsumerFailure(t *testing.T) {
	srv, store, token, _ := newManifestE2EServerWithJobs(t)
	if _, err := store.CreateApp(db.CreateAppParams{Slug: "retire-failure", Name: "retire-failure", OwnerID: 1, Access: "private"}); err != nil {
		t.Fatal(err)
	}
	app, _ := store.GetAppBySlug("retire-failure")
	upload := func(manifest string) *httptest.ResponseRecorder {
		body, ct := buildMultiFileBundleUpload(t, map[string]string{"app.py": "print('consumer')", "shinyhub.toml": manifest})
		req := httptest.NewRequest("POST", "/api/apps/retire-failure/deploy", body)
		req.Header.Set("Content-Type", ct)
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("X-ShinyHub-Allow-Downtime", "1")
		rec := httptest.NewRecorder()
		srv.Router().ServeHTTP(rec, req)
		return rec
	}
	if rec := upload("[[schedule]]\nname='old'\ncron='0 5 * * *'\ncmd='true'"); rec.Code != http.StatusOK {
		t.Fatalf("initial deploy: %d %s", rec.Code, rec.Body.String())
	}
	original, err := store.GetScheduleByName(app.ID, "old")
	if err != nil {
		t.Fatal(err)
	}
	serving, err := store.GetServingDeployment(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	sawRetired := false
	srv.SetDeployRunForTest(func(p deploy.Params) (*deploy.PoolResult, error) {
		p.HealthCheck = func(string, time.Duration, http.RoundTripper) error { return nil }
		if !p.PrepareOnly && p.BundleDir != serving.BundleDir {
			current, err := store.GetSchedule(original.ID)
			if err != nil {
				return nil, err
			}
			sawRetired = current.Retired
			return nil, errors.New("injected consumer failure after retirement")
		}
		return deploy.Run(p)
	})
	rec := upload("[app]\n")
	if rec.Code != http.StatusInternalServerError || !sawRetired {
		t.Fatalf("failure did not exercise retirement: retired=%v response=%d %s", sawRetired, rec.Code, rec.Body.String())
	}
	restored, err := store.GetSchedule(original.ID)
	if err != nil || restored.Retired || !restored.Enabled || restored.ManagedBy != db.ScheduleManagedByBundle {
		t.Fatalf("failed deploy did not restore ownership/state: %+v %v", restored, err)
	}
	active, err := store.GetServingDeployment(app.ID)
	if err != nil || active.ID != serving.ID {
		t.Fatalf("failed deploy changed authority: %+v %v", active, err)
	}
}
