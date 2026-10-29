package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/fleet"
)

const declaredRefreshManifest = `[[schedule]]
name = "refresh"
cron = "0 * * * *"
cmd = "python refresh.py"
`

func TestConvergeApp_VerifyScheduleManifest(t *testing.T) {
	for _, tc := range []struct {
		name      string
		manifest  *string
		resolved  []byte
		ignored   bool
		schedules string
		wantDrift bool
		wantError bool
	}{
		{name: "renamed schedule", manifest: stringPtr(declaredRefreshManifest), schedules: `[{"id":1,"name":"old-refresh","enabled":true,"stale":false},{"id":2,"name":"refresh","enabled":true,"stale":false}]`, wantDrift: true},
		{name: "all schedules removed", manifest: stringPtr("[app]\n"), schedules: `[{"id":1,"name":"old-refresh","enabled":true,"stale":false}]`, wantDrift: true},
		{name: "empty manifest", manifest: stringPtr(""), schedules: `[{"id":1,"name":"old-refresh","enabled":true,"stale":false}]`, wantDrift: true},
		{name: "disabled extra", manifest: stringPtr(declaredRefreshManifest), schedules: `[{"id":1,"name":"old-refresh","enabled":false,"stale":true},{"id":2,"name":"refresh","enabled":true,"stale":false}]`},
		{name: "matching manifest", manifest: stringPtr(declaredRefreshManifest), schedules: `[{"id":2,"name":"refresh","enabled":true,"stale":false}]`},
		{name: "no manifest preserves manual schedules", schedules: `[{"id":1,"name":"manual","enabled":true,"stale":false}]`},
		{name: "ignored manifest", manifest: stringPtr("[app]\n"), ignored: true, schedules: `[{"id":1,"name":"manual","enabled":true,"stale":false}]`},
		{name: "sparse overlay preserves manual schedules", resolved: []byte("sparse = true\n[app.env]\nMODE = 'production'\n"), schedules: `[{"id":1,"name":"manual","enabled":true,"stale":false}]`},
		{name: "resolved manifest takes precedence", manifest: stringPtr("[app]\n"), resolved: []byte(declaredRefreshManifest), schedules: `[{"id":2,"name":"refresh","enabled":true,"stale":false}]`},
		{name: "resolved empty manifest", manifest: stringPtr(declaredRefreshManifest), resolved: []byte{}, schedules: `[{"id":2,"name":"refresh","enabled":true,"stale":false}]`, wantDrift: true},
		{name: "invalid manifest fails closed", manifest: stringPtr("[[schedule]\n"), schedules: `[]`, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if tc.manifest != nil {
				mustWrite(t, filepath.Join(dir, "shinyhub.toml"), *tc.manifest)
			}
			if tc.ignored {
				mustWrite(t, filepath.Join(dir, ".shinyhubignore"), "shinyhub.toml\n")
			}
			var hits int
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits++
				if r.Method != http.MethodGet || r.URL.Path != "/api/apps/demo/schedules" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
					http.NotFound(w, r)
					return
				}
				_, _ = io.WriteString(w, `{"items":`+tc.schedules+`}`)
			}))
			t.Cleanup(srv.Close)
			res := convergeAppFromSpec(&cliConfig{Host: srv.URL},
				fleet.AppDiff{Slug: "demo", Action: fleet.ActionUnchanged},
				fleet.AppEntry{Slug: "demo"}, fleet.ObservedApp{},
				bundleBuildSpec{Dir: dir, Manifest: tc.resolved},
				convergeOpts{verifySchedules: true}, "fleet:test", io.Discard)
			switch {
			case tc.wantDrift:
				if res.status != statusFailed || res.err == nil || res.failureKind != failureScheduleManifestMismatch {
					t.Fatalf("result = %+v, want manifest drift failure", res)
				}
				if len(res.freshnessGate) != 1 || res.freshnessGate[0].State != "undeclared" {
					t.Fatalf("schedule verification = %+v", res.freshnessGate)
				}
				if !strings.Contains(res.err.Error(), "--prune only removes apps") {
					t.Fatalf("missing remediation: %v", res.err)
				}
				var out bytes.Buffer
				if err := writeFleetApplyJSON(&out, &fleet.Manifest{FleetID: "test"}, srv.URL,
					[]fleet.AppDiff{{Slug: "demo", Action: fleet.ActionUnchanged}}, nil,
					applyOutcome{apps: []applyResult{res}}, 4, "PARTIAL"); err != nil {
					t.Fatal(err)
				}
				var envelope applyJSONEnvelope
				if err := json.Unmarshal(out.Bytes(), &envelope); err != nil {
					t.Fatal(err)
				}
				app := envelope.Apps[0]
				if app.Result.FailureKind != failureScheduleManifestMismatch || len(app.ScheduleVerification) != 1 || app.ScheduleVerification[0].State != "undeclared" {
					t.Fatalf("JSON did not expose drift: %s", out.String())
				}
			case tc.wantError:
				if res.err == nil || res.failureKind != failureScheduleStateMissing || hits != 0 {
					t.Fatalf("result = %+v, hits = %d, want local manifest error", res, hits)
				}
			default:
				if res.status != statusUnchanged || res.err != nil {
					t.Fatalf("result = %+v, want unchanged", res)
				}
			}
			if !tc.wantError && hits != 1 {
				t.Fatalf("requests = %d, want one shared declaration/freshness read", hits)
			}
		})
	}
}

func TestConvergeApp_ManifestPrecheckSharesWarmDeadline(t *testing.T) {
	for _, refresh := range []bool{false, true} {
		t.Run(fmt.Sprintf("refresh=%t", refresh), func(t *testing.T) {
			dir := t.TempDir()
			mustWrite(t, filepath.Join(dir, "shinyhub.toml"), declaredRefreshManifest)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/api/apps/demo/schedules" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
					http.NotFound(w, r)
					return
				}
				<-r.Context().Done()
			}))
			t.Cleanup(srv.Close)
			// Keep a regression bounded even if the precheck ignores the warm
			// deadline and waits for the parent context instead.
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			started := time.Now()
			res := convergeApp(&cliConfig{Host: srv.URL},
				fleet.AppDiff{Slug: "demo", Action: fleet.ActionUnchanged},
				fleet.AppEntry{Slug: "demo"}, fleet.ObservedApp{}, dir,
				convergeOpts{context: ctx, verifySchedules: true, waitForWarm: !refresh, refreshStale: refresh, warmTimeout: 40 * time.Millisecond},
				"fleet:test", io.Discard)
			wantKind := failureWarmWaitTimeout
			if refresh {
				wantKind = failureScheduleRefreshTimeout
			}
			if res.status != statusFailed || res.failureKind != wantKind || !errors.Is(res.err, context.DeadlineExceeded) {
				t.Fatalf("status=%s kind=%s error=%v", res.status, res.failureKind, res.err)
			}
			if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
				t.Fatalf("40ms precheck budget took %s", elapsed)
			}
		})
	}
}

func TestConvergeApp_ManifestDriftStopsWarmAndRefresh(t *testing.T) {
	for _, opt := range []convergeOpts{
		{verifySchedules: true, waitForWarm: true},
		{refreshStale: true},
		{verifySchedules: true, refreshStale: true, waitForWarm: true},
	} {
		t.Run(fmt.Sprintf("warm=%t/refresh=%t", opt.waitForWarm, opt.refreshStale), func(t *testing.T) {
			dir := t.TempDir()
			mustWrite(t, filepath.Join(dir, "shinyhub.toml"), declaredRefreshManifest)
			var mutations int
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					mutations++
				}
				_, _ = io.WriteString(w, `{"items":[{"id":1,"name":"old-refresh","enabled":true,"stale":true,"deploy_trigger":"bundle_change"}]}`)
			}))
			t.Cleanup(srv.Close)
			res := convergeApp(&cliConfig{Host: srv.URL},
				fleet.AppDiff{Slug: "demo", Action: fleet.ActionUnchanged},
				fleet.AppEntry{Slug: "demo"}, fleet.ObservedApp{}, dir, opt, "fleet:test", io.Discard)
			if res.failureKind != failureScheduleManifestMismatch || res.err == nil || mutations != 0 {
				t.Fatalf("result = %+v, mutations = %d; removed schedules must not be triggered", res, mutations)
			}
		})
	}
}
