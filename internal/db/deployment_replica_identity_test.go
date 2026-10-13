package db_test

import (
	"testing"

	"github.com/rvben/shinyhub/internal/db"
)

func TestDeploymentReplicaMeasurementsBelongToRuntimeIncarnation(t *testing.T) {
	for _, change := range []string{"unchanged", "pid", "port", "worker", "endpoint", "start-identity"} {
		t.Run(change, func(t *testing.T) {
			store := newScheduleStore(t)
			appID := newScheduleAppFixture(t, store, "ledger-incarnation")
			dep, err := store.BeginDeployment(appID, "v1", t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			oldPID, newPID := 101, 202
			oldPort, newPort := 20001, 20002
			p := db.UpsertDeploymentReplicaParams{AppID: appID, DeploymentID: dep.ID, Index: 0, PID: &oldPID, Port: &oldPort, Status: "running", Provider: "native", WorkerID: "worker-a", EndpointURL: "http://worker-a", DataGeneration: 7, StartupPeakRSSBytes: 512 << 20, ProcessStartIdentity: 1000}
			if err = store.UpsertDeploymentReplica(p); err != nil {
				t.Fatal(err)
			}
			p.DataGeneration, p.StartupPeakRSSBytes, p.ProcessStartIdentity = 0, 0, 0
			switch change {
			case "pid":
				p.PID = &newPID
			case "port":
				p.Port = &newPort
			case "worker":
				p.WorkerID = "worker-b"
			case "endpoint":
				p.EndpointURL = "http://worker-b"
			case "start-identity":
				p.ProcessStartIdentity = 2000
			}
			if err = store.UpsertDeploymentReplica(p); err != nil {
				t.Fatal(err)
			}
			rows, err := store.ListDeploymentReplicas(appID)
			if err != nil || len(rows) != 1 {
				t.Fatalf("rows=%v err=%v", rows, err)
			}
			wantGeneration, wantPeak, wantIdentity := int64(0), int64(0), int64(0)
			if change == "unchanged" {
				wantGeneration, wantPeak, wantIdentity = 7, 512<<20, 1000
			}
			if change == "start-identity" {
				wantIdentity = 2000
			}
			if rows[0].DataGeneration != wantGeneration || rows[0].StartupPeakRSSBytes != wantPeak || rows[0].ProcessStartIdentity != wantIdentity {
				t.Fatalf("%s runtime inherited another incarnation's measurements: %+v; want generation=%d peak=%d start=%d", change, rows[0], wantGeneration, wantPeak, wantIdentity)
			}
		})
	}
}
