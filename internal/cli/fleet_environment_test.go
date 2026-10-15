package cli

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/deploy"
	"github.com/rvben/shinyhub/internal/fleet"
)

func TestEnvironmentBundleMatchesPreflightAndUpload(t *testing.T) {
	dir := t.TempDir()
	original := "[app]\nreplicas = 16\nmin_warm_replicas = 16\n[app.autoscale]\nenabled = true\nmin_replicas = 16\nmax_replicas = 16\ntarget = 0.6\n[app.env]\nMODE = 'old'\nKEEP = 'baseline'\n[[schedule]]\nname = 'refresh'\ncron = '0 5 * * *'\ncmd = 'true'\n"
	mustWrite(t, filepath.Join(dir, "shinyhub.toml"), original)
	mustWrite(t, filepath.Join(dir, "app.py"), "print(1)\n")
	one := 1
	enabled := true
	config := fleet.Config{Env: map[string]string{"MODE": "production"}, Replicas: &one, MinWarmReplicas: &one, Autoscale: &fleet.AutoscaleConfig{Enabled: &enabled, MinReplicas: 1, MaxReplicas: 1, Target: 0.6}}
	spec, manifest, err := resolveEnvironmentBundle(bundleBuildSpec{Dir: dir}, config)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.App.Env["MODE"] != "production" || manifest.App.Env["KEEP"] != "baseline" || manifest.App.Replicas != nil || manifest.App.Autoscale.MaxReplicas != 1 || len(manifest.Schedules) != 1 {
		t.Fatalf("merged manifest: %+v", manifest)
	}
	preview, err := previewBundleSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	facts := resolveFleetBundleFacts(spec, manifest, preview)
	request, err := buildDeployPreflightRequest(facts, &fleet.AppEntry{Slug: "demo", Config: config})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := zip.NewReader(bytes.NewReader(preview.Buffer.Bytes()), int64(preview.Buffer.Len()))
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, file := range reader.File {
		if file.Name == "shinyhub.toml" {
			count++
			r, _ := file.Open()
			data, _ := io.ReadAll(r)
			r.Close()
			if string(data) != request.Manifest {
				t.Fatal("rehearsal differs from upload")
			}
			parsed, err := deploy.ParseManifest(data)
			if err != nil || parsed.App.Replicas != nil {
				t.Fatalf("upload=%s err=%v", data, err)
			}
		}
	}
	if count != 1 {
		t.Fatalf("manifest count=%d", count)
	}
	unchanged, _ := os.ReadFile(filepath.Join(dir, "shinyhub.toml"))
	if string(unchanged) != original {
		t.Fatal("source was modified")
	}
	repeat, err := previewBundleSpec(spec)
	if err != nil || repeat.Digest != preview.Digest {
		t.Fatal("unstable environment digest", err)
	}
}

func TestValidateFleetEveryEnvironmentMergedWithBundle(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "app.py"), "print(1)\n")
	mustWrite(t, filepath.Join(dir, "shinyhub.toml"), "[app]\nrender_seconds = 1\n")
	file := filepath.Join(dir, "fleet.toml")
	data := []byte(`fleet_id = "test"
[environments.dev]
host = "https://dev.example.com"
[environments.prd]
host = "https://prd.example.com"
[[app]]
slug = "demo"
source = "."
[app.environments.prd.config]
render_seconds = 601
`)
	_, problems := validateFleetManifest(file, data)
	if !strings.Contains(strings.Join(problems, "\n"), "render_seconds") {
		t.Fatalf("invalid unselected environment passed: %v", problems)
	}
}

func TestFleetEnvironmentEnvConvergesWithoutSharedWrites(t *testing.T) {
	serving := "production"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("unexpected mutation %s %s", r.Method, r.URL)
			w.WriteHeader(500)
			return
		}
		switch r.URL.Path {
		case "/api/apps/demo":
			io.WriteString(w, `{"app":{"slug":"demo","status":"running"}}`)
		case "/api/apps/demo/env":
			json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{{"key": "MODE", "value": serving, "source": "bundle"}}})
		default:
			t.Errorf("unexpected request %s", r.URL)
			w.WriteHeader(500)
		}
	}))
	defer srv.Close()
	entry := fleet.AppEntry{Slug: "demo", Config: fleet.Config{Env: map[string]string{"MODE": "production"}}}
	cfg := &cliConfig{Host: srv.URL, Token: "test"}
	opt := convergeOpts{context: context.Background(), healthTimeout: time.Second}
	if _, err := convergeDeclaredConfig(cfg, "demo", entry, nil, nil, opt, io.Discard); err != nil {
		t.Fatal(err)
	}
	serving = "old"
	if _, err := convergeDeclaredConfig(cfg, "demo", entry, nil, nil, opt, io.Discard); err == nil || !strings.Contains(err.Error(), "re-plan and redeploy") {
		t.Fatalf("nonconverged bundle must require deploy: %v", err)
	}
}

func TestEnvironmentPlanRendersProvenance(t *testing.T) {
	m := &fleet.Manifest{FleetID: "test", ResolvedEnvironment: "prd"}
	diff := []fleet.AppDiff{{Slug: "demo", Action: fleet.ActionUpdateConfig, ConfigDrift: []fleet.ConfigDriftItem{{Key: "min_warm_replicas", Server: "1", Desired: "16", Environment: "prd"}}}}
	doc := fleetPlanDocument("shinyhub fleet plan", "fleet.toml", m, "https://prd.example.com", diff, nil)
	if doc.Environment != "prd" || doc.Resources[0].Changes[0].Environment != "prd" {
		t.Fatalf("missing provenance: %+v", doc)
	}
	_, planned := planHumanChangeValues(doc.Resources[0].Changes[0])
	if !strings.Contains(planned, "16 (prd)") {
		t.Fatalf("planned=%q", planned)
	}
	var out bytes.Buffer
	if err := writeFleetPlanJSON(&out, m, "https://prd.example.com", diff, nil, 0, ""); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"environment":"prd"`) {
		t.Fatal(out.String())
	}
}

func TestFleetBundleEnvRejectsOldServerBeforeMutations(t *testing.T) {
	_, requests := setupCLITestHandler(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/server-info":
			io.WriteString(w, `{"capabilities":{"fleet_preconditions":true,"content_digest":true}}`)
		case "/api/apps":
			io.WriteString(w, `[]`)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(500)
		}
	})
	file := writeFleetTree(t, "fleet_id='test'\n[[app]]\nslug='demo'\nsource='./demo'\n[app.config.env]\nMODE='production'\n", map[string]string{"demo": ""})
	_, stderr, err := execCLISplit(t, "fleet", "apply", "-f", file, "--yes")
	if exitCode(err) != 1 || !strings.Contains(stderr, "versioned bundle env") {
		t.Fatalf("err=%v stderr=%s", err, stderr)
	}
	for _, req := range *requests {
		if req.Method != http.MethodGet {
			t.Fatalf("old server mutated: %s %s", req.Method, req.Path)
		}
	}
}

func TestObserveFleetEnvReadsEveryPage(t *testing.T) {
	pages := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pages++
		items := []map[string]any{}
		if r.URL.Query().Get("offset") == "0" {
			for i := 0; i < 100; i++ {
				items = append(items, map[string]any{"key": fmt.Sprintf("KEY_%d", i), "value": "value"})
			}
		} else {
			items = append(items, map[string]any{"key": "MODE", "value": "production"})
		}
		json.NewEncoder(w).Encode(map[string]any{"items": items, "total": 101})
	}))
	defer srv.Close()
	observed, err := observeFleetEnv(&cliConfig{Host: srv.URL}, "demo", fleet.Config{Env: map[string]string{"MODE": "production"}}, fleet.ObservedApp{})
	if err != nil || pages != 2 || observed.Env["MODE"] != "production" {
		t.Fatalf("pages=%d env=%v err=%v", pages, observed.Env, err)
	}
}

func TestEnvironmentNoopRetainsOriginalDigestAndMalformedEnvFails(t *testing.T) {
	one := 1
	for _, source := range []string{"", "[app]\n", "# keep this comment\n[app]\nreplicas=1\n", "[[hook]]\non='post-deploy'\ncommand=['true']\n"} {
		dir := t.TempDir()
		mustWrite(t, filepath.Join(dir, "app.py"), "print(1)")
		if source != "" {
			mustWrite(t, filepath.Join(dir, "shinyhub.toml"), source)
		}
		spec, _, err := resolveEnvironmentBundle(bundleBuildSpec{Dir: dir}, fleet.Config{Replicas: &one})
		if err != nil || spec.Manifest != nil {
			t.Fatalf("noop source=%q manifest=%q err=%v", source, spec.Manifest, err)
		}
	}
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "shinyhub.toml"), "[app]\nenv='invalid'\n")
	if _, _, err := resolveEnvironmentBundle(bundleBuildSpec{Dir: dir}, fleet.Config{Env: map[string]string{"MODE": "new"}}); err == nil {
		t.Fatal("overlay hid malformed source env")
	}
}

func TestRemovingAllEnvChangesBundleDigest(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "app.py"), "print(1)")
	with, _, err := resolveEnvironmentBundle(bundleBuildSpec{Dir: dir}, fleet.Config{Env: map[string]string{"MODE": "old"}})
	if err != nil {
		t.Fatal(err)
	}
	without, _, err := resolveEnvironmentBundle(bundleBuildSpec{Dir: dir}, fleet.Config{})
	if err != nil {
		t.Fatal(err)
	}
	old, err := previewBundleSpec(with)
	if err != nil {
		t.Fatal(err)
	}
	new, err := previewBundleSpec(without)
	if err != nil {
		t.Fatal(err)
	}
	if old.Digest == new.Digest || without.Manifest != nil {
		t.Fatal("removing last env key did not remove generated manifest and change digest")
	}
	marker := "fleet:test"
	diff := fleet.Diff(&fleet.Manifest{FleetID: "test", Apps: []fleet.AppEntry{{Slug: "demo", Visibility: "private"}}}, map[string]string{"demo": new.Digest}, []fleet.ObservedApp{{Slug: "demo", Access: "private", ManagedBy: &marker, ContentDigest: old.Digest}})
	if len(diff) != 1 || diff[0].Action != fleet.ActionUpdateSource {
		t.Fatalf("last env removal was not planned: %+v", diff)
	}

}

func TestPlainDeployBundleEnvRejectsOldServer(t *testing.T) {
	_, requests := setupCLITestHandler(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/server-info" {
			io.WriteString(w, `{"version":"0.20.9","capabilities":{}}`)
			return
		}
		t.Errorf("unexpected request %s %s", r.Method, r.URL)
		w.WriteHeader(500)
	})
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "app.py"), "print(1)")
	mustWrite(t, filepath.Join(dir, "shinyhub.toml"), "[app.env]\nMODE='new'\n")
	_, _, err := execCLISplit(t, "deploy", dir, "--slug", "demo")
	if err == nil || !strings.Contains(err.Error(), "versioned bundle env") {
		t.Fatalf("old server accepted env: %v", err)
	}
	for _, req := range *requests {
		if req.Method != http.MethodGet {
			t.Fatalf("mutated old server: %s %s", req.Method, req.Path)
		}
	}
}

func TestOverlayReplicasUsesOnlineConfigPath(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "app.py"), "print(1)")
	sixteen, eight := 16, 8
	spec, manifest, err := resolveEnvironmentBundle(bundleBuildSpec{Dir: dir}, fleet.Config{Replicas: &sixteen}, fleet.OverlayConfig{Replicas: &sixteen})
	if err != nil || spec.Manifest != nil || manifest != nil {
		t.Fatalf("replica overlay manufactured a deployment: %q %+v %v", spec.Manifest, manifest, err)
	}
	preview, err := previewBundleSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	marker := "fleet:test"
	diff := fleet.Diff(&fleet.Manifest{FleetID: "test", Apps: []fleet.AppEntry{{Slug: "demo", Visibility: "private", Config: fleet.Config{Replicas: &sixteen}}}}, map[string]string{"demo": preview.Digest}, []fleet.ObservedApp{{Slug: "demo", ManagedBy: &marker, Access: "private", ContentDigest: preview.Digest, Replicas: &eight}})
	if len(diff) != 1 || diff[0].Action != fleet.ActionUpdateConfig {
		t.Fatalf("replica-only overlay did not use online resize: %+v", diff)
	}
	// A conflicting source value must not undo the fleet's eventual resize.
	mustWrite(t, filepath.Join(dir, "shinyhub.toml"), "[app]\nreplicas=8\n")
	_, manifest, err = resolveEnvironmentBundle(bundleBuildSpec{Dir: dir}, fleet.Config{Replicas: &sixteen}, fleet.OverlayConfig{Replicas: &sixteen})
	if err != nil || manifest.App.Replicas != nil {
		t.Fatalf("conflicting source replica count remains active: %+v %v", manifest, err)
	}
}

func TestIgnoredManifestDoesNotContributePolicyOrPreflight(t *testing.T) {
	for _, withEnv := range []bool{false, true} {
		dir := t.TempDir()
		mustWrite(t, filepath.Join(dir, "app.py"), "print(1)")
		mustWrite(t, filepath.Join(dir, ".shinyhubignore"), "shinyhub.toml\n")
		mustWrite(t, filepath.Join(dir, "shinyhub.toml"), "[app]\nidentity_headers=true\n[app.env]\nEXCLUDED='never'\n[[hook]]\non='post-deploy'\ncommand=['danger']\n")
		config := fleet.Config{}
		if withEnv {
			config.Env = map[string]string{"MODE": "production"}
		}
		spec, manifest, err := resolveEnvironmentBundle(bundleBuildSpec{Dir: dir}, config)
		if err != nil {
			t.Fatal(err)
		}
		if !withEnv && (manifest != nil || spec.Manifest != nil) {
			t.Fatal("ignored manifest was loaded")
		}
		if withEnv && (!manifest.Sparse || manifest.App.IdentityHeaders != nil || len(manifest.Hooks) > 0 || len(manifest.App.Env) != 1) {
			t.Fatalf("ignored source declarations shipped: %+v", manifest)
		}
		preview, err := previewBundleSpec(spec)
		if err != nil {
			t.Fatal(err)
		}
		facts := resolveFleetBundleFacts(spec, manifest, preview)
		request, err := buildDeployPreflightRequest(facts, &fleet.AppEntry{Slug: "demo", Config: config})
		if err != nil || request.Manifest != string(spec.Manifest) {
			t.Fatalf("preflight differs from actual bundle: %q %q %v", request.Manifest, spec.Manifest, err)
		}
	}
}
