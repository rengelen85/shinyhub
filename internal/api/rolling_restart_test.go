package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/activation"
	"github.com/rvben/shinyhub/internal/config"
	"github.com/rvben/shinyhub/internal/db"
)

func TestRollingRestartAcceptedWhileLifecycleLockHeld(t *testing.T) {
	srv, store, token, _, _, _ := buildManifestE2EServer(t, config.RuntimeConfig{})
	defer srv.Close()
	app := createGenerationTestApp(t, store, "restart-queued", 1, 16)
	if rec := deployBareGeneration(t, srv, token, app.Slug, "print('v1')", false); rec.Code != http.StatusOK {
		t.Fatalf("deploy: %d %s", rec.Code, rec.Body.String())
	}
	app, _ = store.GetAppByID(app.ID)
	active := seedClaimedActivation(t, store, app)
	release := srv.acquireDeployLock(app.Slug)
	defer release()
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		req := httptest.NewRequest(http.MethodPost, "/api/apps/"+app.Slug+"/restart?roll=true", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		srv.Router().ServeHTTP(rec, req)
		done <- rec
	}()
	select {
	case rec := <-done:
		if rec.Code != http.StatusAccepted {
			t.Fatalf("restart: %d %s", rec.Code, rec.Body.String())
		}
		var body struct {
			ActivationID int64 `json:"activation_id"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		queued, err := store.GetScheduleActivation(body.ActivationID)
		if err != nil || queued.ID == active.ID || queued.Status != "pending" {
			t.Fatalf("independent queued activation=%+v %v", queued, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("restart acceptance blocked behind lifecycle execution")
	}
}

func TestQueuedManualRestartLeavesStoppedAppStopped(t *testing.T) {
	srv, store, token, _, _, _ := buildManifestE2EServer(t, config.RuntimeConfig{})
	defer srv.Close()
	app := createGenerationTestApp(t, store, "restart-then-stop", 1, 16)
	if rec := deployBareGeneration(t, srv, token, app.Slug, "print('v1')", false); rec.Code != http.StatusOK {
		t.Fatalf("deploy: %d %s", rec.Code, rec.Body.String())
	}
	app, _ = store.GetAppByID(app.ID)
	current, _ := store.GetServingDeployment(app.ID)
	a, err := store.EnqueueRollingRestart(app, current)
	if err != nil {
		t.Fatal(err)
	}
	a, err = store.ClaimNextScheduleActivation(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: app.Slug, Status: "stopped"}); err != nil {
		t.Fatal(err)
	}
	if err := srv.Roll(context.Background(), a); !errors.Is(err, activation.ErrNotNeeded) {
		t.Fatalf("queued restart after stop=%v", err)
	}
}

func TestRetiringProxyProtectsLifecycleButAcceptsQueuedRoll(t *testing.T) {
	srv, store, token, _, _, _ := buildManifestE2EServer(t, config.RuntimeConfig{})
	defer srv.Close()
	app := createGenerationTestApp(t, store, "restart-parent-drain", 1, 16)
	if rec := deployBareGeneration(t, srv, token, app.Slug, "print('v1')", false); rec.Code != http.StatusOK {
		t.Fatalf("deploy: %d %s", rec.Code, rec.Body.String())
	}
	done := make(chan struct{})
	srv.proxy.SetRetiringParent(done)
	if err := srv.guardActivationLifecycle(app.ID, "stop"); !errors.Is(err, errRetiringProxy) {
		t.Fatalf("parent lifecycle fence=%v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/apps/"+app.Slug+"/restart?roll=true", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("queued roll: %d %s", rec.Code, rec.Body.String())
	}
	a, err := store.ClaimNextScheduleActivation(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	var waiting *activation.WaitingError
	if err := srv.Roll(context.Background(), a); !errors.As(err, &waiting) {
		t.Fatalf("roll failed to wait for parent: %v", err)
	}
	if err := store.WaitScheduleActivation(a.ID, waiting.Reason, time.Now()); err != nil {
		t.Fatal(err)
	}
	close(done)
	if err := srv.guardActivationLifecycle(app.ID, "stop"); err != nil {
		t.Fatalf("unstarted queued roll must permit an explicit stop: %v", err)
	}
}

func TestRollingRestartProceedsWhileDeployedGenerationDrains(t *testing.T) {
	srv, store, token, mgr, _, rt := buildManifestE2EServer(t, config.RuntimeConfig{})
	defer srv.Close()
	rt.serveTraffic = true
	rt.holdStarted = make(chan struct{}, 1)
	rt.holdRelease = make(chan struct{})
	defer close(rt.holdRelease)
	app := createGenerationTestApp(t, store, "restart-beside-drain", 1, 16)
	srv.SetAvailableMemoryForTest(func() (int, error) { return 4096, nil })
	if rec := deployBareGeneration(t, srv, token, app.Slug, "print('v1')", false); rec.Code != 200 {
		t.Fatalf("first deploy: %d %s", rec.Code, rec.Body.String())
	}
	first, _ := store.GetServingDeployment(app.ID)
	go proxyGenerationBody(srv, app.Slug, "?hold=1")
	select {
	case <-rt.holdStarted:
	case <-time.After(time.Second):
		t.Fatal("old request not held")
	}
	if rec := deployBareGeneration(t, srv, token, app.Slug, "print('v2')", false); rec.Code != 200 {
		t.Fatalf("handoff: %d %s", rec.Code, rec.Body.String())
	}
	app, _ = store.GetAppByID(app.ID)
	current, _ := store.GetServingDeployment(app.ID)
	a, err := store.EnqueueRollingRestart(app, current)
	if err != nil {
		t.Fatal(err)
	}
	a, err = store.ClaimNextScheduleActivation(time.Now())
	if err != nil || a == nil {
		t.Fatalf("claim: %v", err)
	}
	if err := srv.Roll(context.Background(), a); err != nil {
		t.Fatalf("roll beside drain: %v", err)
	}
	if !srv.proxy.IsGenerationDraining(app.Slug, first.ID) {
		t.Fatal("roll prematurely retired old generation")
	}
	if info, ok := mgr.GetGenerationReplica(app.Slug, first.ID, 0); !ok || info.Status != "running" {
		t.Fatalf("roll stopped old worker: %+v", info)
	}
	// A later request with an explicit disruptive fallback must wait rather
	// than deregistering the independently retained old generation.
	if err := store.FinishScheduleActivation(a.ID, "succeeded", "", time.Now(), false); err != nil {
		t.Fatal(err)
	}
	pending, err := store.EnqueueRollingRestart(app, current)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().Exec("UPDATE schedule_activations SET roll_fallback='restart' WHERE id=?", pending.ID); err != nil {
		t.Fatal(err)
	}
	pending, err = store.ClaimNextScheduleActivation(time.Now())
	if err != nil || pending == nil {
		t.Fatalf("fallback claim: %v", err)
	}
	// The surge capacity probe uses the host sample; force a deterministic
	// refusal through an impossible host floor rather than its handoff seam.
	impossibleFloor := int(^uint(0)>>1) / 2
	srv.cfg.Server.MinAvailableMemoryMB = &impossibleFloor
	var waiting *activation.WaitingError
	if err := srv.Roll(context.Background(), pending); !errors.As(err, &waiting) {
		t.Fatalf("fallback: %v, want waiting", err)
	}
	if waiting.Reason != "stop-first fallback must wait for retained generations" {
		t.Fatalf("wrong wait: %s", waiting.Reason)
	}
	saved, err := store.GetScheduleActivation(pending.ID)
	if err != nil {
		t.Fatal(err)
	}
	if saved.Phase == "stopping_pool" || !srv.proxy.PoolHasAny(app.Slug) {
		t.Fatal("fallback entered disruptive phase")
	}
	if info, ok := mgr.GetGenerationReplica(app.Slug, first.ID, 0); !ok || info.Status != "running" {
		t.Fatal("fallback stopped retained worker")
	}

}
