package api

import (
	"encoding/json"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/config"
)

func TestDeployIdleCPUDoesNotOvercommitServingWorkerBudgets(t *testing.T) {
	srv, store, token, _, _, runtime := buildManifestE2EServer(t, config.RuntimeConfig{})
	defer srv.Close()
	runtime.serveTraffic = true
	srv.SetHostCapacity(3, "config", 65536, "config")
	app := createGenerationTestApp(t, store, "reserved-cpu", 2, 16)
	if rec := deployBareGeneration(t, srv, token, app.Slug, "print('old')", false); rec.Code != http.StatusOK {
		t.Fatal(rec.Body.String())
	}
	// Idle workers still own their CPU budgets: two serving workers plus two
	// candidates cannot fit on a three-core node, even with an idle sample.
	rec := deployBareGeneration(t, srv, token, app.Slug, "print('new')", false)
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "CPU budget") {
		t.Fatalf("overcommitted update = %d %s", rec.Code, rec.Body.String())
	}
	if code, body := proxyGenerationBody(srv, app.Slug, ""); code != http.StatusOK || body == "" {
		t.Fatalf("budget refusal stopped serving: %d %s", code, body)
	}
}

func TestDeployOversizedCPURequestReturnsValidCapacityError(t *testing.T) {
	srv, store, token, _, _, _ := buildManifestE2EServer(t, config.RuntimeConfig{})
	defer srv.Close()
	app := createGenerationTestApp(t, store, "oversized-budget", 2, 16)
	if rec := deployBareGeneration(t, srv, token, app.Slug, "print('old')", false); rec.Code != http.StatusOK {
		t.Fatal(rec.Body.String())
	}
	request := math.MaxFloat64
	srv.cfg.Server.UpdateCapacity.WorkerCPURequestCores = &request
	rec := deployBareGeneration(t, srv, token, app.Slug, "print('new')", false)
	var result map[string]any
	if rec.Code != http.StatusConflict || json.Unmarshal(rec.Body.Bytes(), &result) != nil || result["update_state"] != "blocked_capacity" {
		t.Fatalf("oversized budget broke capacity response: %d %s", rec.Code, rec.Body.String())
	}
}

func TestDeployExplicitFractionalRequestsFitOneCoreNode(t *testing.T) {
	srv, store, token, _, _, _ := buildManifestE2EServer(t, config.RuntimeConfig{})
	defer srv.Close()
	srv.SetHostCapacity(1, "config", 65536, "config")
	request, reserve := 0.4, 0.1
	srv.cfg.Server.UpdateCapacity.WorkerCPURequestCores = &request
	srv.cfg.Server.UpdateCapacity.ReservedCPUCores = &reserve
	app := createGenerationTestApp(t, store, "fractional-budget", 1, 16)
	if rec := deployBareGeneration(t, srv, token, app.Slug, "print('old')", false); rec.Code != http.StatusOK {
		t.Fatal(rec.Body.String())
	}
	if rec := deployBareGeneration(t, srv, token, app.Slug, "print('new')", false); rec.Code != http.StatusOK {
		t.Fatalf("fractional update = %d %s", rec.Code, rec.Body.String())
	}
}

func TestDeploySurgeCeilingPreservesCompleteServingPool(t *testing.T) {
	srv, store, token, _, _, runtime := buildManifestE2EServer(t, config.RuntimeConfig{})
	defer srv.Close()
	runtime.serveTraffic = true
	srv.cfg.Server.UpdateCapacity.MaxSurgeReplicas = 1
	app := createGenerationTestApp(t, store, "surge-ceiling", 2, 16)
	if rec := deployBareGeneration(t, srv, token, app.Slug, "print('old')", false); rec.Code != http.StatusOK {
		t.Fatal(rec.Body.String())
	}
	rec := deployBareGeneration(t, srv, token, app.Slug, "print('new')", false)
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "server.update_capacity.max_surge_replicas") {
		t.Fatalf("surge ceiling ignored: %d %s", rec.Code, rec.Body.String())
	}
	if code, body := proxyGenerationBody(srv, app.Slug, ""); code != http.StatusOK || body == "" {
		t.Fatalf("surge ceiling stopped serving: %d %s", code, body)
	}
}

func TestDeployBudgetsRetainedGenerations(t *testing.T) {
	for _, resource := range []string{"cpu", "surge"} {
		t.Run(resource, func(t *testing.T) {
			srv, store, token, _, _, runtime := buildManifestE2EServer(t, config.RuntimeConfig{})
			defer srv.Close()
			runtime.serveTraffic = true
			runtime.holdStarted = make(chan struct{}, 1)
			runtime.holdRelease = make(chan struct{})
			defer close(runtime.holdRelease)
			if resource == "cpu" {
				srv.SetHostCapacity(3.2, "config", 65536, "config")
			} else {
				srv.cfg.Server.UpdateCapacity.MaxSurgeReplicas = 1
			}
			app := createGenerationTestApp(t, store, "retained-"+resource, 1, 16)
			if rec := deployBareGeneration(t, srv, token, app.Slug, "print('v1')", false); rec.Code != http.StatusOK {
				t.Fatal(rec.Body.String())
			}
			done := make(chan struct{})
			go func() { proxyGenerationBody(srv, app.Slug, "?hold=1"); close(done) }()
			select {
			case <-runtime.holdStarted:
			case <-time.After(time.Second):
				t.Fatal("old request did not start")
			}
			if rec := deployBareGeneration(t, srv, token, app.Slug, "print('v2')", false); rec.Code != http.StatusOK {
				t.Fatal(rec.Body.String())
			}
			rec := deployBareGeneration(t, srv, token, app.Slug, "print('v3')", false)
			if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), `"resource":"`+resource+`"`) {
				t.Fatalf("retained generation omitted from %s budget: %d %s", resource, rec.Code, rec.Body.String())
			}
			select {
			case <-done:
				t.Fatal("refusal dropped the old request")
			default:
			}
			if code, _ := proxyGenerationBody(srv, app.Slug, ""); code != http.StatusOK {
				t.Fatalf("active version no longer serves: %d", code)
			}
		})
	}
}
