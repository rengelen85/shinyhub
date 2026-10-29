package db_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/db"
)

func TestScheduleReconciliationHistoryRevivalAndRecovery(t *testing.T) {
	store := newScheduleStore(t)
	appID := newScheduleAppFixture(t, store, "reconcile")
	p := db.UpsertScheduleByNameParams{AppID: appID, Name: "old", CronExpr: "0 5 * * *", CommandJSON: `["true"]`, Enabled: true, TimeoutSeconds: 60, OverlapPolicy: "skip", MissedPolicy: "skip", ManagedBy: db.ScheduleManagedByBundle}
	id, _, err := store.UpsertScheduleByName(p)
	if err != nil {
		t.Fatal(err)
	}
	original, err := store.GetSchedule(id)
	if err != nil {
		t.Fatal(err)
	}
	runID, err := store.InsertScheduleRun(db.InsertScheduleRunParams{ScheduleID: id, Status: "succeeded", Trigger: "manual", StartedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	pending, err := store.BeginDeployment(appID, "v2", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReconcileManifestSchedules(appID, nil, []*db.Schedule{original}); err != nil {
		t.Fatal(err)
	}
	retired, err := store.GetSchedule(id)
	if err != nil {
		t.Fatal(err)
	}
	if !retired.Retired || retired.Enabled || retired.ManagedBy != db.ScheduleManagedByBundle {
		t.Fatalf("retired=%+v", retired)
	}
	if _, err := store.GetScheduleRun(runID); err != nil {
		t.Fatalf("lost history: %v", err)
	}
	if _, err := store.InsertScheduleRun(db.InsertScheduleRunParams{ScheduleID: id, Status: "running", Trigger: "manual", StartedAt: time.Now()}); err == nil {
		t.Fatal("retired manual run admitted")
	}
	if err := store.RecordDeploymentScheduleSnapshot(pending.ID, appID); err != nil {
		t.Fatal(err)
	}
	snap, err := store.DeploymentScheduleSnapshot(pending.ID)
	if err != nil || len(snap) != 1 || !snap[0].Retired {
		t.Fatalf("snapshot=%+v err=%v", snap, err)
	}
	if _, err := store.RestoreDeploymentPriorScheduleSnapshot(pending.ID, appID); err != nil {
		t.Fatal(err)
	}
	restored, err := store.GetSchedule(id)
	if err != nil || restored.Retired || !restored.Enabled || restored.ManagedBy != db.ScheduleManagedByBundle {
		t.Fatalf("recovery=%+v err=%v", restored, err)
	}
	if _, err := store.RestoreDeploymentScheduleSnapshot(pending.ID, appID); err != nil {
		t.Fatal(err)
	}
	sameID, created, err := store.UpsertScheduleByName(p)
	if err != nil || created || sameID != id {
		t.Fatalf("revival id=%d created=%v err=%v", sameID, created, err)
	}
	revived, err := store.GetSchedule(id)
	if err != nil || revived.Retired || !revived.Enabled {
		t.Fatalf("revived=%+v err=%v", revived, err)
	}
}

func TestScheduleReconciliationIsAtomicAndRefusesActiveWriter(t *testing.T) {
	store := newScheduleStore(t)
	appID := newScheduleAppFixture(t, store, "atomic")
	p := db.UpsertScheduleByNameParams{AppID: appID, Name: "old", CronExpr: "0 5 * * *", CommandJSON: `["true"]`, Enabled: true, TimeoutSeconds: 60, OverlapPolicy: "skip", MissedPolicy: "skip", ManagedBy: db.ScheduleManagedByBundle}
	id, _, err := store.UpsertScheduleByName(p)
	if err != nil {
		t.Fatal(err)
	}
	old, err := store.GetSchedule(id)
	if err != nil {
		t.Fatal(err)
	}
	invalid := p
	invalid.Name = "new"
	invalid.ManagedBy = "invalid"
	if _, err := store.ReconcileManifestSchedules(appID, []db.UpsertScheduleByNameParams{invalid}, []*db.Schedule{old}); err == nil {
		t.Fatal("invalid declaration accepted")
	}
	unchanged, err := store.GetSchedule(id)
	if err != nil || unchanged.Retired || !unchanged.Enabled {
		t.Fatalf("partial retirement: %+v %v", unchanged, err)
	}
	_, err = store.InsertScheduleRun(db.InsertScheduleRunParams{ScheduleID: id, Status: "running", Trigger: "manual", StartedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	next := p
	next.Name = "new"
	if _, err := store.ReconcileManifestSchedules(appID, []db.UpsertScheduleByNameParams{next}, []*db.Schedule{old}); err == nil {
		t.Fatal("active writer retired")
	}
	rows, err := store.ListSchedulesByApp(appID)
	if err != nil || len(rows) != 1 || rows[0].Retired {
		t.Fatalf("non-atomic retirement: %+v %v", rows, err)
	}
}

func TestScheduleReconciliationHandoffCompensation(t *testing.T) {
	store := newScheduleStore(t)
	appID := newScheduleAppFixture(t, store, "handoff-retire")
	app, err := store.GetAppByID(appID)
	if err != nil {
		t.Fatal(err)
	}
	p := db.UpsertScheduleByNameParams{AppID: appID, Name: "old", CronExpr: "0 5 * * *", CommandJSON: `["true"]`, Enabled: true, TimeoutSeconds: 60, OverlapPolicy: "skip", MissedPolicy: "skip", ManagedBy: db.ScheduleManagedByBundle}
	id, _, err := store.UpsertScheduleByName(p)
	if err != nil {
		t.Fatal(err)
	}
	original, err := store.GetSchedule(id)
	if err != nil {
		t.Fatal(err)
	}
	old, err := store.BeginDeployment(appID, "v1", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PromoteDeployment(old.ID); err != nil {
		t.Fatal(err)
	}
	next, err := store.BeginDeployment(appID, "v2", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	retired := *original
	retired.Enabled = false
	retired.Retired = true
	if err := store.StageHandoffSettings(next.ID, app, app, []*db.Schedule{original}, []*db.Schedule{&retired}); err != nil {
		t.Fatal(err)
	}
	before, err := store.GetSchedule(id)
	if err != nil || before.Retired {
		t.Fatalf("staging mutated live policy: %+v %v", before, err)
	}
	if err := store.PromoteDeployment(next.ID); err != nil {
		t.Fatal(err)
	}
	after, err := store.GetSchedule(id)
	if err != nil || !after.Retired || after.Enabled {
		t.Fatalf("promotion=%+v %v", after, err)
	}
	if err := store.RevertDeploymentActivation(next.ID, old.ID, "proxy publication failed"); err != nil {
		t.Fatal(err)
	}
	restored, err := store.GetSchedule(id)
	if err != nil || restored.Retired || !restored.Enabled || restored.ManagedBy != db.ScheduleManagedByBundle {
		t.Fatalf("compensation=%+v %v", restored, err)
	}
}

func TestScheduleReconciliationRefusesUncertainWriter(t *testing.T) {
	store := newScheduleStore(t)
	appID := newScheduleAppFixture(t, store, "uncertain")
	id, _, err := store.UpsertScheduleByName(db.UpsertScheduleByNameParams{AppID: appID, Name: "old", CronExpr: "0 5 * * *", CommandJSON: `["true"]`, Enabled: true, TimeoutSeconds: 60, OverlapPolicy: "skip", MissedPolicy: "skip", ManagedBy: db.ScheduleManagedByBundle})
	if err != nil {
		t.Fatal(err)
	}
	old, err := store.GetSchedule(id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.InsertScheduleRun(db.InsertScheduleRunParams{ScheduleID: id, Status: "running", Trigger: "manual", StartedAt: time.Now(), PublishesData: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkRunningSchedulesInterrupted(); err != nil {
		t.Fatal(err)
	}
	if err := store.CheckScheduleRetirement(id); err == nil {
		t.Fatal("uncertain writer passed preflight")
	}
	if _, err := store.ReconcileManifestSchedules(appID, nil, []*db.Schedule{old}); err == nil {
		t.Fatal("uncertain writer retired")
	}
}

func TestScheduleReconciliationSerializesAdmission(t *testing.T) {
	store := newScheduleStore(t)
	appID := newScheduleAppFixture(t, store, "race")
	for i := 0; i < 12; i++ {
		id, _, err := store.UpsertScheduleByName(db.UpsertScheduleByNameParams{AppID: appID, Name: fmt.Sprintf("job-%d", i), CronExpr: "0 5 * * *", CommandJSON: `["true"]`, Enabled: true, TimeoutSeconds: 60, OverlapPolicy: "skip", MissedPolicy: "skip", ManagedBy: db.ScheduleManagedByBundle})
		if err != nil {
			t.Fatal(err)
		}
		old, err := store.GetSchedule(id)
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		results := make(chan error, 2)
		go func() {
			<-start
			_, err := store.InsertScheduleRun(db.InsertScheduleRunParams{ScheduleID: id, Status: "running", Trigger: "manual", StartedAt: time.Now()})
			results <- err
		}()
		go func() {
			<-start
			_, err := store.ReconcileManifestSchedules(appID, nil, []*db.Schedule{old})
			results <- err
		}()
		close(start)
		first, second := <-results, <-results
		if (first == nil) == (second == nil) {
			t.Fatalf("exactly one of admission/retirement must win: %v / %v", first, second)
		}
	}
}
