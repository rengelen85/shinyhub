package db

import "testing"

func TestMigration100ReclassifiesOnlyPreservedCapacityRefusals(t *testing.T) {
	skipIfPostgres(t)
	s := migratedThrough(t, 99)
	mustExec(t, s, `INSERT INTO users (username, password_hash, role) VALUES ('owner','x','admin')`)
	mustExec(t, s, `INSERT INTO apps (slug, name, owner_id, access) VALUES ('blocked','Blocked',1,'private')`)
	for _, tc := range []struct {
		version, reason string
		barrier         int
	}{
		{"capacity", "capacity-blocked/cpu: budget", 0},
		{"boot", "startup failed", 0},
		{"unsafe", "capacity-blocked/cpu: budget", 1},
	} {
		mustExec(t, s, `INSERT INTO deployments (app_id, version, bundle_dir, status, failure_reason, producer_barrier_entered)
			VALUES (1, ?, '/bundles/test', 'failed', ?, ?)`, tc.version, tc.reason, tc.barrier)
	}
	if err := s.Migrate(); err != nil {
		t.Fatal(err)
	}
	for version, want := range map[string]string{"capacity": DeploymentBlocked, "boot": DeploymentFailed, "unsafe": DeploymentFailed} {
		var status string
		if err := s.DB().QueryRow(`SELECT status FROM deployments WHERE version = ?`, version).Scan(&status); err != nil || status != want {
			t.Fatalf("%s status=%s want=%s: %v", version, status, want, err)
		}
	}
}
