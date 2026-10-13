package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/db"
)

func TestDisabledProducerRemainsDiagnosableAndRepairableThroughHTTP(t *testing.T) {
	for _, stop := range []bool{false, true} {
		t.Run(fmt.Sprintf("operator_stop=%t", stop), func(t *testing.T) {
			srv, store, token, runtime := newManifestE2EServerWithJobs(t)
			t.Cleanup(func() { srv.jobs.Stop(context.Background()); _ = srv.manager.StopAll(); srv.Close() })
			if _, err := store.CreateApp(db.CreateAppParams{Slug: "operator-repair", Name: "operator-repair", OwnerID: 1, Access: "private"}); err != nil {
				t.Fatal(err)
			}
			manifest := strings.Replace(groupedProducerManifest, `deploy_trigger = "first_deploy"`, `deploy_trigger = "bundle_change"`, 1)
			body, contentType := buildMultiFileBundleUpload(t, map[string]string{"app.py": "print('app')", "refresh.py": "print('data')", "shinyhub.toml": manifest})
			req := httptest.NewRequest(http.MethodPost, "/api/apps/operator-repair/deploy", body)
			req.Header.Set("Authorization", "Bearer "+token)
			req.Header.Set("Content-Type", contentType)
			rec := httptest.NewRecorder()
			srv.Router().ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("deploy: %d %s", rec.Code, rec.Body.String())
			}
			app, err := store.GetAppBySlug("operator-repair")
			if err != nil {
				t.Fatal(err)
			}
			scheduleID := scheduleIDByName(t, store, app.ID, "refresh-data")
			path := fmt.Sprintf("/api/apps/operator-repair/schedules/%d", scheduleID)
			request := func(method, path, body string, want int) *httptest.ResponseRecorder {
				t.Helper()
				req := httptest.NewRequest(method, path, strings.NewReader(body))
				req.Header.Set("Authorization", "Bearer "+token)
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				srv.Router().ServeHTTP(rec, req)
				if rec.Code != want {
					t.Fatalf("%s %s: %d %s; want %d", method, path, rec.Code, rec.Body.String(), want)
				}
				return rec
			}
			run := func(status string, auditCount int64) int64 {
				t.Helper()
				rec := request(http.MethodPost, path+"/run", "", http.StatusAccepted)
				var admitted struct {
					RunID int64 `json:"run_id"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &admitted); err != nil {
					t.Fatal(err)
				}
				deadline := time.Now().Add(5 * time.Second)
				for {
					rec := request(http.MethodGet, fmt.Sprintf("%s/runs/%d", path, admitted.RunID), "", http.StatusOK)
					var state db.ScheduleRun
					if err := json.Unmarshal(rec.Body.Bytes(), &state); err != nil {
						t.Fatal(err)
					}
					count, err := store.CountAuditEvents("schedule_run_" + status)
					if err != nil {
						t.Fatal(err)
					}
					// Terminal rows precede consumer cleanup. Wait for its audit
					// before changing operator intent or repairing the producer.
					if state.Status == status && count == auditCount {
						return admitted.RunID
					}
					if time.Now().After(deadline) {
						t.Fatalf("run did not finish %s with cleanup audit: %s count=%d", status, rec.Body.String(), count)
					}
					time.Sleep(5 * time.Millisecond)
				}
			}
			runtime.mu.Lock()
			runtime.producerErr = errors.New("transient producer failure")
			runtime.mu.Unlock()
			failedID := run("failed", 1)
			request(http.MethodPatch, path, `{"enabled":false,"deploy_trigger":"never","on_success":"none"}`, http.StatusOK)
			read := request(http.MethodGet, "/api/apps/operator-repair", "", http.StatusOK)
			var view struct {
				App db.App `json:"app"`
			}
			if err := json.Unmarshal(read.Body.Bytes(), &view); err != nil {
				t.Fatal(err)
			}
			if view.App.DesiredStatus != "failed" || !view.App.CompatibilityQuarantined || !view.App.ProducerRepairRequired || len(view.App.ProducerRepairSchedules) != 1 || view.App.ProducerRepairSchedules[0] != "refresh-data" || !strings.Contains(view.App.LastError, fmt.Sprintf("producer run %d", failedID)) {
				t.Fatalf("disabled producer disappeared from operator diagnosis: %s", read.Body.String())
			}
			list := request(http.MethodGet, "/api/apps/operator-repair/schedules", "", http.StatusOK)
			var schedules struct {
				Items []struct {
					ID             int64  `json:"id"`
					Enabled        bool   `json:"enabled"`
					DeployTrigger  string `json:"deploy_trigger"`
					OnSuccess      string `json:"on_success"`
					RepairRequired bool   `json:"producer_repair_required"`
				} `json:"items"`
			}
			if err := json.Unmarshal(list.Body.Bytes(), &schedules); err != nil {
				t.Fatal(err)
			}
			if len(schedules.Items) != 1 || schedules.Items[0].ID != scheduleID || schedules.Items[0].Enabled || schedules.Items[0].DeployTrigger != "never" || !schedules.Items[0].RepairRequired {
				t.Fatalf("schedule list hid repair requirement: %s", list.Body.String())
			}
			denied := request(http.MethodDelete, path, "", http.StatusConflict)
			if !strings.Contains(denied.Body.String(), "unrepaired failed data write") {
				t.Fatalf("deletion did not explain repair action: %s", denied.Body.String())
			}
			if stop {
				request(http.MethodPost, "/api/apps/operator-repair/stop", "", http.StatusOK)
			}
			runtime.mu.Lock()
			runtime.producerErr = nil
			runtime.mu.Unlock()
			repairedID := run("succeeded", 2)
			read = request(http.MethodGet, "/api/apps/operator-repair", "", http.StatusOK)
			view.App = db.App{}
			if err := json.Unmarshal(read.Body.Bytes(), &view); err != nil {
				t.Fatal(err)
			}
			wantStatus := "running"
			if stop {
				wantStatus = "stopped"
			}
			if view.App.DesiredStatus != wantStatus || view.App.CompatibilityQuarantined || view.App.ProducerRepairRequired || len(view.App.ProducerRepairSchedules) != 0 || view.App.LastError != "" {
				t.Fatalf("manual repair lost operator intent or uncertainty: %s", read.Body.String())
			}
			publication, err := store.GetAppDataPublication(app.ID)
			if err != nil || publication.Generation != 2 || publication.ScheduleRunID != repairedID {
				t.Fatalf("disabled manual repair did not publish: %+v err=%v", publication, err)
			}
			// Repair is an execution action, not permission to re-enable automatic writes.
			list = request(http.MethodGet, "/api/apps/operator-repair/schedules", "", http.StatusOK)
			schedules.Items = nil
			if err := json.Unmarshal(list.Body.Bytes(), &schedules); err != nil {
				t.Fatal(err)
			}
			if len(schedules.Items) != 1 || schedules.Items[0].ID != scheduleID || schedules.Items[0].Enabled || schedules.Items[0].DeployTrigger != "never" || schedules.Items[0].OnSuccess != "none" || schedules.Items[0].RepairRequired {
				t.Fatalf("manual repair changed schedule intent or left repair diagnosis: %s", list.Body.String())
			}
			request(http.MethodDelete, path, "", http.StatusNoContent)
			list = request(http.MethodGet, "/api/apps/operator-repair/schedules", "", http.StatusOK)
			schedules.Items = nil
			if err := json.Unmarshal(list.Body.Bytes(), &schedules); err != nil {
				t.Fatal(err)
			}
			if len(schedules.Items) != 0 {
				t.Fatalf("repaired schedule could not be deleted: %s", list.Body.String())
			}
			if count, err := store.CountAuditEvents("schedule_delete"); err != nil || count != 1 {
				t.Fatalf("blocked deletion created audit or successful deletion lost one: count=%d err=%v", count, err)
			}
		})
	}
}
