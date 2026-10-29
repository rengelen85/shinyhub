package db_test

import (
	"testing"

	"github.com/rvben/shinyhub/internal/db"
)

func manifestScheduleParams(appID int64, names ...string) []db.UpsertScheduleByNameParams {
	params := make([]db.UpsertScheduleByNameParams, 0, len(names))
	for _, name := range names {
		params = append(params, db.UpsertScheduleByNameParams{
			AppID: appID, Name: name, CronExpr: "0 5 * * *", CommandJSON: `["true"]`,
			Enabled: true, TimeoutSeconds: 30, OverlapPolicy: "skip", MissedPolicy: "skip",
			DeployTrigger: "never", OnSuccess: "none", RollFallback: "defer",
		})
	}
	return params
}

func scheduleByName(t *testing.T, store *db.Store, appID int64, name string) *db.Schedule {
	t.Helper()
	schedule, err := store.GetScheduleByName(appID, name)
	if err != nil {
		t.Fatalf("get schedule %q: %v", name, err)
	}
	return schedule
}

func TestCreateSchedule_IsManualByDefault(t *testing.T) {
	store := newScheduleStore(t)
	appID := newScheduleAppFixture(t, store, "manual-app")
	if _, err := store.CreateSchedule(db.CreateScheduleParams{
		AppID: appID, Name: "adhoc", CronExpr: "0 * * * *", CommandJSON: `["true"]`,
		Enabled: true, TimeoutSeconds: 30, OverlapPolicy: "skip", MissedPolicy: "skip",
	}); err != nil {
		t.Fatal(err)
	}
	if got := scheduleByName(t, store, appID, "adhoc").Source; got != db.ScheduleSourceManual {
		t.Fatalf("source = %q, want manual", got)
	}
}

func TestApplyManifestScheduleDeclarations_DisablesOnlyUndeclaredManifestSchedules(t *testing.T) {
	store := newScheduleStore(t)
	appID := newScheduleAppFixture(t, store, "prune-app")
	if _, _, err := store.ApplyManifestScheduleDeclarations(appID, manifestScheduleParams(appID, "refresh-data", "keep"), true); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSchedule(db.CreateScheduleParams{
		AppID: appID, Name: "adhoc", CronExpr: "0 * * * *", CommandJSON: `["true"]`,
		Enabled: true, TimeoutSeconds: 30, OverlapPolicy: "skip", MissedPolicy: "skip",
	}); err != nil {
		t.Fatal(err)
	}
	if got := scheduleByName(t, store, appID, "refresh-data").Source; got != db.ScheduleSourceManifest {
		t.Fatalf("manifest upsert source = %q, want manifest", got)
	}
	before := scheduleByName(t, store, appID, "refresh-data")

	// The issue's rename: refresh-data is replaced by two new declarations.
	results, disabled, err := store.ApplyManifestScheduleDeclarations(appID,
		manifestScheduleParams(appID, "keep", "refresh-latest-data", "refresh-timechart-data"), true)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Fatalf("upsert results = %d, want 3", len(results))
	}
	if len(disabled) != 1 || disabled[0].Name != "refresh-data" || disabled[0].ID != before.ID {
		t.Fatalf("disabled = %+v, want only refresh-data", disabled)
	}
	after := scheduleByName(t, store, appID, "refresh-data")
	if after.Enabled || after.ID != before.ID {
		t.Fatalf("refresh-data after prune: enabled=%v id=%d, want disabled row %d", after.Enabled, after.ID, before.ID)
	}
	for _, name := range []string{"keep", "adhoc", "refresh-latest-data", "refresh-timechart-data"} {
		if !scheduleByName(t, store, appID, name).Enabled {
			t.Fatalf("%s was disabled", name)
		}
	}

	// Already disabled rows are not reported again.
	if _, disabled, err := store.ApplyManifestScheduleDeclarations(appID,
		manifestScheduleParams(appID, "keep", "refresh-latest-data", "refresh-timechart-data"), true); err != nil || len(disabled) != 0 {
		t.Fatalf("idempotent re-apply disabled %+v (err %v), want none", disabled, err)
	}

	// Declaring it again brings back the same row enabled.
	if _, _, err := store.ApplyManifestScheduleDeclarations(appID, manifestScheduleParams(appID, "refresh-data"), true); err != nil {
		t.Fatal(err)
	}
	if again := scheduleByName(t, store, appID, "refresh-data"); !again.Enabled || again.ID != before.ID {
		t.Fatalf("redeclared refresh-data: enabled=%v id=%d, want enabled row %d", again.Enabled, again.ID, before.ID)
	}
}

func TestApplyManifestScheduleDeclarations_WithoutPruneKeepsUndeclared(t *testing.T) {
	store := newScheduleStore(t)
	appID := newScheduleAppFixture(t, store, "noprune-app")
	if _, _, err := store.ApplyManifestScheduleDeclarations(appID, manifestScheduleParams(appID, "old"), true); err != nil {
		t.Fatal(err)
	}
	_, disabled, err := store.ApplyManifestScheduleDeclarations(appID, manifestScheduleParams(appID, "new"), false)
	if err != nil || len(disabled) != 0 {
		t.Fatalf("disabled %+v (err %v) without prune", disabled, err)
	}
	if !scheduleByName(t, store, appID, "old").Enabled {
		t.Fatal("old was disabled without prune")
	}
}

func TestApplyManifestScheduleDeclarations_EmptyManifestDisablesAllManifestSchedules(t *testing.T) {
	store := newScheduleStore(t)
	appID := newScheduleAppFixture(t, store, "empty-app")
	if _, _, err := store.ApplyManifestScheduleDeclarations(appID, manifestScheduleParams(appID, "a", "b"), true); err != nil {
		t.Fatal(err)
	}
	_, disabled, err := store.ApplyManifestScheduleDeclarations(appID, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(disabled) != 2 || disabled[0].Name != "a" || disabled[1].Name != "b" {
		t.Fatalf("disabled = %+v, want a and b in name order", disabled)
	}
}

func TestApplyManifestScheduleDeclarations_AdoptsDeclaredManualSchedule(t *testing.T) {
	store := newScheduleStore(t)
	appID := newScheduleAppFixture(t, store, "adopt-app")
	if _, err := store.CreateSchedule(db.CreateScheduleParams{
		AppID: appID, Name: "nightly", CronExpr: "0 * * * *", CommandJSON: `["true"]`,
		Enabled: true, TimeoutSeconds: 30, OverlapPolicy: "skip", MissedPolicy: "skip",
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.ApplyManifestScheduleDeclarations(appID, manifestScheduleParams(appID, "nightly"), true); err != nil {
		t.Fatal(err)
	}
	if got := scheduleByName(t, store, appID, "nightly").Source; got != db.ScheduleSourceManifest {
		t.Fatalf("declared manual schedule source = %q, want manifest", got)
	}
}

func TestApplyManifestScheduleDeclarations_RejectsForeignAppWhenPruning(t *testing.T) {
	store := newScheduleStore(t)
	appID := newScheduleAppFixture(t, store, "own-app")
	otherID := newScheduleAppFixture(t, store, "other-app")
	if _, _, err := store.ApplyManifestScheduleDeclarations(appID, manifestScheduleParams(otherID, "x"), true); err == nil {
		t.Fatal("prune accepted a declaration for another app")
	}
	if _, err := store.GetScheduleByName(otherID, "x"); err == nil {
		t.Fatal("rejected batch was committed")
	}
}

// Failure recovery restores the pre-deploy declarations, including owner and
// enabled state, so a failed deploy neither keeps a prune nor an adoption.
func TestRestoreScheduleDeclarations_RestoresSourceAndEnabled(t *testing.T) {
	store := newScheduleStore(t)
	appID := newScheduleAppFixture(t, store, "restore-app")
	if _, _, err := store.ApplyManifestScheduleDeclarations(appID, manifestScheduleParams(appID, "owned"), true); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSchedule(db.CreateScheduleParams{
		AppID: appID, Name: "adhoc", CronExpr: "0 * * * *", CommandJSON: `["true"]`,
		Enabled: true, TimeoutSeconds: 30, OverlapPolicy: "skip", MissedPolicy: "skip",
	}); err != nil {
		t.Fatal(err)
	}
	previous, err := store.ListSchedulesByApp(appID)
	if err != nil {
		t.Fatal(err)
	}
	// The failed deploy disabled owned and adopted adhoc.
	if _, _, err := store.ApplyManifestScheduleDeclarations(appID, manifestScheduleParams(appID, "adhoc"), true); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RestoreScheduleDeclarations(appID, previous); err != nil {
		t.Fatal(err)
	}
	if owned := scheduleByName(t, store, appID, "owned"); !owned.Enabled || owned.Source != db.ScheduleSourceManifest {
		t.Fatalf("owned after restore: enabled=%v source=%q", owned.Enabled, owned.Source)
	}
	if adhoc := scheduleByName(t, store, appID, "adhoc"); adhoc.Source != db.ScheduleSourceManual {
		t.Fatalf("adhoc after restore: source=%q, want manual", adhoc.Source)
	}

	// A snapshot that predates ownership carries no source and keeps the
	// stored owner.
	legacy := *scheduleByName(t, store, appID, "owned")
	legacy.Source = ""
	if _, err := store.RestoreScheduleDeclarations(appID, []*db.Schedule{&legacy}); err != nil {
		t.Fatal(err)
	}
	if got := scheduleByName(t, store, appID, "owned").Source; got != db.ScheduleSourceManifest {
		t.Fatalf("legacy snapshot restore changed source to %q", got)
	}
}
