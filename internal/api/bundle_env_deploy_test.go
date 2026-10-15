package api

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/config"
	"github.com/rvben/shinyhub/internal/deploy"
)

func TestBundleEnvDeployFailureAndRollbackRetainServingValues(t *testing.T) {
	srv, store, token, _, _, rt := buildManifestE2EServer(t, config.RuntimeConfig{})
	defer srv.Close()
	srv.SetAvailableMemoryForTest(func() (int, error) { return 4096, nil })
	app := createGenerationTestApp(t, store, "bundle-env", 1, 16)
	upload := func(value string) *httptest.ResponseRecorder {
		body, contentType := buildMultiFileBundleUpload(t, map[string]string{"app.py": "print('app')", "shinyhub.toml": "[app.env]\nMODE='" + value + "'\n"})
		req := httptest.NewRequest(http.MethodPost, "/api/apps/"+app.Slug+"/deploy", body)
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", contentType)
		rec := httptest.NewRecorder()
		srv.Router().ServeHTTP(rec, req)
		return rec
	}
	assertEnv := func(want string) {
		t.Helper()
		rt.mu.Lock()
		last := rt.starts[len(rt.starts)-1]
		rt.mu.Unlock()
		values := map[string]string{}
		for _, entry := range append(append([]string{}, last.Env...), last.SecretEnv...) {
			key, value, _ := strings.Cut(entry, "=")
			values[key] = value
		}
		if values["MODE"] != want {
			t.Fatalf("runtime MODE=%q want %q", values["MODE"], want)
		}
	}
	if rec := upload("old"); rec.Code != 200 {
		t.Fatalf("initial: %d %s", rec.Code, rec.Body)
	}
	assertEnv("old")
	old, err := store.GetServingDeployment(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	srv.SetDeployRunForTest(func(p deploy.Params) (*deploy.PoolResult, error) {
		p.HealthCheck = func(string, time.Duration, http.RoundTripper) error { return errors.New("candidate unhealthy") }
		return deploy.Run(p)
	})
	if rec := upload("failed"); rec.Code < 400 {
		t.Fatalf("unhealthy candidate accepted: %d %s", rec.Code, rec.Body)
	}
	assertEnv("failed")
	current, err := store.GetServingDeployment(app.ID)
	if err != nil || current.ID != old.ID {
		t.Fatalf("failed candidate changed authority: %+v %v", current, err)
	}
	values, err := srv.servingBundleEnv(app.ID)
	if err != nil || values["MODE"] != "old" {
		t.Fatalf("failed candidate changed effective env: %v %v", values, err)
	}
	srv.SetDeployRunForTest(func(p deploy.Params) (*deploy.PoolResult, error) {
		p.HealthCheck = func(string, time.Duration, http.RoundTripper) error { return nil }
		return deploy.Run(p)
	})
	if rec := upload("new"); rec.Code != 200 {
		t.Fatalf("replacement: %d %s", rec.Code, rec.Body)
	}
	assertEnv("new")
	active, err := store.GetServingDeployment(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.waitForPreviousGeneration(t.Context(), app, active.ID, false); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/apps/"+app.Slug+"/rollback", strings.NewReader(fmt.Sprintf(`{"deployment_id":%d}`, old.ID)))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("rollback: %d %s", rec.Code, rec.Body)
	}
	assertEnv("old")
	values, err = srv.servingBundleEnv(app.ID)
	if err != nil || values["MODE"] != "old" {
		t.Fatalf("rollback env: %v %v", values, err)
	}
}

func TestSparseEnvFirstHandoffPreservesLivePolicy(t *testing.T) {
	srv, store, token, _, _, rt := buildManifestE2EServer(t, config.RuntimeConfig{})
	defer srv.Close()
	srv.SetAvailableMemoryForTest(func() (int, error) { return 4096, nil })
	app := createGenerationTestApp(t, store, "sparse-env", 3, 16)
	if rec := deployBareGeneration(t, srv, token, app.Slug, "print('old')", false); rec.Code != 200 {
		t.Fatal(rec.Body.String())
	}
	if _, err := store.DB().Exec("UPDATE apps SET identity_headers=?,usage_identity_mode='disabled' WHERE id=?", true, app.ID); err != nil {
		t.Fatal(err)
	}
	body, ct := buildMultiFileBundleUpload(t, map[string]string{"app.py": "print('new')", "shinyhub.toml": "sparse=true\n[app]\nreplicas=3\n[app.env]\nMODE='new'\n"})
	req := httptest.NewRequest(http.MethodPost, "/api/apps/"+app.Slug+"/deploy", body)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", ct)
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("first env handoff: %d %s", rec.Code, rec.Body)
	}
	live, err := store.GetAppByID(app.ID)
	if err != nil || live.IdentityHeaders == nil || !*live.IdentityHeaders || live.UsageIdentityMode == nil || *live.UsageIdentityMode != "disabled" || live.Replicas != 3 {
		t.Fatalf("live policy reset: %+v err=%v", live, err)
	}
	rt.mu.Lock()
	last := rt.starts[len(rt.starts)-1]
	rt.mu.Unlock()
	if !last.GenerationScoped {
		t.Fatal("env-only deployment did not use parallel handoff")
	}
}
