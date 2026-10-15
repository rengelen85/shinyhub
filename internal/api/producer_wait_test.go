package api

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/activation"
	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/jobs"
)

func TestScheduledActivationWaitsOnlyForRunningProducer(t *testing.T) {
	srv, store := newAuthTestServer(t)
	if err := store.CreateUser(db.CreateUserParams{Username: "producer-wait", PasswordHash: "unused", Role: "admin"}); err != nil {
		t.Fatal(err)
	}
	user, err := store.GetUserByUsername("producer-wait")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateApp(db.CreateAppParams{Slug: "producer-wait", Name: "Producer wait", OwnerID: user.ID}); err != nil {
		t.Fatal(err)
	}
	app, err := store.GetAppBySlug("producer-wait")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: app.Slug, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	id, err := store.CreateSchedule(db.CreateScheduleParams{AppID: app.ID, Name: "refresh-latest-data", CronExpr: "* * * * *", CommandJSON: `["true"]`, Enabled: true, TimeoutSeconds: 60, OverlapPolicy: "skip", MissedPolicy: "skip", DeployTrigger: "never", OnSuccess: "none"})
	if err != nil {
		t.Fatal(err)
	}
	run, err := store.InsertScheduleRun(db.InsertScheduleRunParams{ScheduleID: id, Status: "running", Trigger: "schedule", StartedAt: time.Now().UTC(), PublishesData: true})
	if err != nil {
		t.Fatal(err)
	}
	fence := srv.guardCompatibilityQuarantine(app.ID, "test")
	if !errors.Is(fence, errProducerInProgress) || !errors.Is(fence, errCompatibilityQuarantined) || !strings.Contains(fence.Error(), "refresh-latest-data") {
		t.Fatalf("running fence=%v", fence)
	}
	err = srv.Roll(context.Background(), &db.ScheduleActivation{AppID: &app.ID, AppSlug: app.Slug})
	var waiting *activation.WaitingError
	if !errors.As(err, &waiting) || errors.Is(err, activation.ErrUnsupported) {
		t.Fatalf("running producer activation=%v", err)
	}
	// The authoritative publication-gate recheck uses the same waiting policy
	// if a writer starts after the earlier guard succeeded.
	err = srv.activationPublicationFenceError(app.ID, jobs.ErrAppCompatibilityQuarantined)
	if !errors.As(err, &waiting) {
		t.Fatalf("publication fence race became terminal: %v", err)
	}

	// A busy app must not stall the coordinator, consume an attempt, or remain
	// marked running while a deployment owns its lifecycle lock.
	activationRun, err := store.InsertScheduleRun(db.InsertScheduleRunParams{ScheduleID: id, Status: "running", Trigger: "schedule", StartedAt: time.Now().UTC().Add(-time.Minute), OnSuccess: "roll"})
	if err != nil {
		t.Fatal(err)
	}
	queued, err := store.CompleteScheduleRunAndEnqueueActivation(db.CompleteScheduleRunParams{RunID: activationRun, Status: "succeeded", FinishedAt: time.Now().UTC().Add(-time.Second)})
	if err != nil || queued == nil {
		t.Fatalf("queue activation: %+v %v", queued, err)
	}
	release := srv.acquireDeployLock(app.Slug)
	coordinator := activation.New(store, srv, time.Second)
	finished := make(chan error, 1)
	go func() {
		worked, err := coordinator.ProcessNext(context.Background())
		if err == nil && !worked {
			err = fmt.Errorf("activation was not claimed")
		}
		finished <- err
	}()
	select {
	case err := <-finished:
		release()
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		release()
		<-finished
		t.Fatal("coordinator blocked behind a deployment")
	}
	stored, err := store.GetScheduleActivation(queued.ID)
	if err != nil || stored.Status != "pending" || stored.Attempts != 0 {
		t.Fatalf("waiting activation=%+v err=%v", stored, err)
	}
	inFlight, err := store.ScheduleActivationInFlight(app.ID)
	if err != nil || inFlight {
		t.Fatalf("waiting activation retained runtime ownership: %t %v", inFlight, err)
	}
	// Retry the same durable activation while only the producer owns the fence.
	if _, err := store.DB().Exec(`UPDATE schedule_activations SET due_at = ? WHERE id = ?`, time.Now().UTC().Add(-time.Second), queued.ID); err != nil {
		t.Fatal(err)
	}
	if worked, err := coordinator.ProcessNext(context.Background()); !worked || err != nil {
		t.Fatalf("producer retry: worked=%t err=%v", worked, err)
	}
	stored, err = store.GetScheduleActivation(queued.ID)
	if err != nil || stored.Status != "pending" || stored.Attempts != 0 || !strings.Contains(stored.DeferReason, "refresh-latest-data") {
		t.Fatalf("producer wait=%+v err=%v", stored, err)
	}
	if _, err := store.CompleteScheduleRunAndEnqueueActivation(db.CompleteScheduleRunParams{RunID: run, Status: "failed", FinishedAt: time.Now().UTC(), DataWriteAttempted: true}); err != nil {
		t.Fatal(err)
	}
	// A running repair must not hide an already-failed physical write.
	if _, err := store.InsertScheduleRun(db.InsertScheduleRunParams{ScheduleID: id, Status: "running", Trigger: "manual", StartedAt: time.Now().UTC(), PublishesData: true}); err != nil {
		t.Fatal(err)
	}
	fence = srv.guardCompatibilityQuarantine(app.ID, "test")
	if fence == nil || errors.Is(fence, errProducerInProgress) || !strings.Contains(fence.Error(), "repair producers") {
		t.Fatalf("failed producer fence=%v", fence)
	}
	err = srv.activationPublicationFenceError(app.ID, jobs.ErrAppCompatibilityQuarantined)
	if !errors.Is(err, activation.ErrUnsupported) {
		t.Fatalf("failed producer became waitable: %v", err)
	}

}
