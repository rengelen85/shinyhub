package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/config"
	"github.com/rvben/shinyhub/internal/db"
)

func TestManifestHibernatePolicyThroughUploadAndAppAPI(t *testing.T) {
	for _, tc := range []struct {
		name             string
		value, effective int
		inherit          bool
	}{
		{"disable", 0, 0, false},
		{"inherit-server-default", -1, 10, true},
		{"explicit-timeout", 20, 20, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, store, token, manager, _, runtime := buildManifestE2EServer(t, config.RuntimeConfig{})
			t.Cleanup(func() {
				_ = manager.StopAll()
				srv.Close()
			})
			runtime.serveTraffic = true
			srv.cfg.Lifecycle.HibernateTimeout = 10 * time.Minute
			srv.cfg.Server.DrainTimeout = time.Millisecond
			srv.SetAvailableMemoryForTest(func() (int, error) { return 4096, nil })
			app := createGenerationTestApp(t, store, "hibernate-policy", 1, 16)
			upload := func(value int) {
				t.Helper()
				manifest := fmt.Sprintf("[app]\nmemory_limit_mb=16\nmin_warm_replicas=1\nhibernate_timeout_minutes=%d\n", value)
				body, contentType := buildMultiFileBundleUpload(t, map[string]string{"app.py": fmt.Sprintf("print(%d)", value), "shinyhub.toml": manifest})
				req := httptest.NewRequest(http.MethodPost, "/api/apps/"+app.Slug+"/deploy", body)
				req.Header.Set("Authorization", "Bearer "+token)
				req.Header.Set("Content-Type", contentType)
				rec := httptest.NewRecorder()
				srv.Router().ServeHTTP(rec, req)
				if rec.Code != http.StatusOK {
					t.Fatalf("upload: %d %s", rec.Code, rec.Body.String())
				}
			}
			expectPolicy := func(value, effective int, inherit bool) {
				t.Helper()
				req := httptest.NewRequest(http.MethodGet, "/api/apps/"+app.Slug, nil)
				req.Header.Set("Authorization", "Bearer "+token)
				rec := httptest.NewRecorder()
				srv.Router().ServeHTTP(rec, req)
				if rec.Code != http.StatusOK {
					t.Fatalf("show: %d %s", rec.Code, rec.Body.String())
				}
				var response struct {
					App       db.App  `json:"app"`
					Effective float64 `json:"effective_hibernate_timeout_minutes"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
					t.Fatal(err)
				}
				if response.Effective != float64(effective) || response.App.MinWarmReplicas != 1 {
					t.Fatalf("policy not exposed after upload: %s", rec.Body.String())
				}
				if inherit {
					if response.App.HibernateTimeoutMinutes != nil {
						t.Fatalf("reset left explicit timeout: %+v", response.App)
					}
				} else if response.App.HibernateTimeoutMinutes == nil || *response.App.HibernateTimeoutMinutes != value {
					t.Fatalf("explicit timeout lost: %+v", response.App)
				}
			}
			upload(tc.value)
			expectPolicy(tc.value, tc.effective, tc.inherit)
			// Replace an explicit policy so an ignored reset/zero cannot pass
			// by inheriting the initial default.
			upload(7)
			expectPolicy(7, 7, false)
			upload(tc.value)
			expectPolicy(tc.value, tc.effective, tc.inherit)
		})
	}
}
