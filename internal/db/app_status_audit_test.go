package db_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/rvben/shinyhub/internal/db"
)

func TestUpdateAppStatus_AuditFailurePreservesRecoveryIntent(t *testing.T) {
	store := newScheduleStore(t)
	id := newScheduleAppFixture(t, store, "status-audit")
	if _, err := store.DB().Exec(`UPDATE apps SET status = 'failed', producer_recovery_status = 'hibernated' WHERE id = ?`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().Exec(`ALTER TABLE audit_events RENAME TO unavailable_audit_events`); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: "status-audit", Status: "hibernated", Reason: "deploy_succeeded"}); err == nil {
		t.Fatal("status changed without required audit")
	}
	app, err := store.GetAppByID(id)
	if err != nil || app.Status != "failed" || app.ProducerRecoveryStatus != "hibernated" {
		t.Fatalf("status escaped rollback: %+v err=%v", app, err)
	}
}

func TestQuarantineDeployment_AuditBestEffortAndAttributed(t *testing.T) {
	for _, unavailable := range []bool{false, true} {
		t.Run(fmt.Sprint(unavailable), func(t *testing.T) {
			store := newScheduleStore(t)
			id := newScheduleAppFixture(t, store, "quarantine-audit")
			if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: "quarantine-audit", Status: "running"}); err != nil {
				t.Fatal(err)
			}
			pending, err := store.BeginDeployment(id, "v1", t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := store.MarkDeploymentProducerBarrierEntered(pending.ID); err != nil {
				t.Fatal(err)
			}
			app, err := store.GetAppByID(id)
			if err != nil {
				t.Fatal(err)
			}
			audit := db.AuditEventParams{UserID: &app.OwnerID, IPAddress: "192.0.2.1", CredentialType: "session_token"}
			if unavailable {
				if _, err := store.DB().Exec(`ALTER TABLE audit_events RENAME TO unavailable_audit_events`); err != nil {
					t.Fatal(err)
				}
			}
			failures := 0
			store.SetAuditErrorHook(func() { failures++ })
			if err := store.QuarantineAndFailDeployment(pending.ID, "producer failed", audit); err != nil {
				t.Fatal(err)
			}
			app, err = store.GetAppByID(id)
			if err != nil || app.Status != "failed" || app.LastError != "producer failed" {
				t.Fatalf("quarantine suppressed by audit: %+v err=%v", app, err)
			}
			var status, reason string
			if err := store.DB().QueryRow(`SELECT status, failure_reason FROM deployments WHERE id = ?`, pending.ID).Scan(&status, &reason); err != nil || status != db.DeploymentFailed || reason != "producer failed" {
				t.Fatalf("quarantine diagnostics not committed: status=%q reason=%q err=%v", status, reason, err)
			}
			if unavailable {
				if failures != 1 {
					t.Fatalf("audit failures=%d want=1", failures)
				}
				return // LogAuditEvent emitted the attributed event to structured logs.
			}
			events, err := store.ListAuditEvents("app_status_changed", 20, 0)
			if err != nil || len(events) != 1 {
				t.Fatalf("transition events=%+v err=%v", events, err)
			}
			event := events[0]
			var detail map[string]string
			if err := json.Unmarshal([]byte(event.Detail), &detail); err != nil {
				t.Fatal(err)
			}
			if detail["from"] != "running" || detail["to"] != "failed" || detail["reason"] != "deploy_quarantined" || event.UserID == nil || *event.UserID != app.OwnerID || event.IPAddress != audit.IPAddress || event.CredentialType != audit.CredentialType {
				t.Fatalf("missing transition or attribution: %+v detail=%v", event, detail)
			}
		})
	}
}

func TestUpdateAppStatus_BestEffortAuditDoesNotBlockRecovery(t *testing.T) {
	store := newScheduleStore(t)
	id := newScheduleAppFixture(t, store, "recovery-audit")
	if _, err := store.DB().Exec(`UPDATE apps SET status = 'running', producer_recovery_status = 'hibernated' WHERE id = ?`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().Exec(`ALTER TABLE audit_events RENAME TO unavailable_audit_events`); err != nil {
		t.Fatal(err)
	}
	failures := 0
	store.SetAuditErrorHook(func() { failures++ })
	if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: "recovery-audit", Status: "failed", Reason: "deploy_recovery", BestEffortAudit: true, PreserveRecoveryIntent: true, LastError: "restore failed"}); err != nil {
		t.Fatal(err)
	}
	app, err := store.GetAppByID(id)
	if err != nil || app.Status != "failed" || app.ProducerRecoveryStatus != "hibernated" || app.LastError != "restore failed" || failures != 1 {
		t.Fatalf("recovery status blocked by audit: %+v failures=%d err=%v", app, failures, err)
	}
}
