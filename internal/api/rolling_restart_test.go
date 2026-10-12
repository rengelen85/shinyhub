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
