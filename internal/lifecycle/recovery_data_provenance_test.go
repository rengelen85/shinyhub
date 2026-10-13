package lifecycle_test

import (
	"fmt"
	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/dbtest"
	"github.com/rvben/shinyhub/internal/lifecycle"
	"github.com/rvben/shinyhub/internal/process"
	"github.com/rvben/shinyhub/internal/proxy"
	"github.com/rvben/shinyhub/internal/schedulespec"
	"testing"
	"time"
)

func publishRecoveryData(t *testing.T, store *db.Store, appID int64, dep *db.Deployment, command string) *db.AppDataPublication {
	t.Helper()
	id, err := store.CreateSchedule(db.CreateScheduleParams{AppID: appID, Name: command, CronExpr: "0 5 * * *", CommandJSON: command, Enabled: true, OnSuccess: "none", RollFallback: "defer"})
	if err != nil {
		t.Fatal(err)
	}
	canonical, fingerprint, err := schedulespec.ProducerIdentity(command)
	if err != nil {
		t.Fatal(err)
	}
	run, err := store.InsertScheduleRun(db.InsertScheduleRunParams{ScheduleID: id, Status: "running", StartedAt: time.Now().UTC(), Trigger: "manual", OnSuccess: "none", RollFallback: "defer", DeploymentID: &dep.ID, AppVersion: dep.Version, ContentDigest: "sha256:fixture", ProducerCommandJSON: canonical, ProducerFingerprint: fingerprint, PublishesData: true})
	if err != nil {
		t.Fatal(err)
	}
	zero := 0
	if _, err := store.CompleteScheduleRunAndEnqueueActivation(db.CompleteScheduleRunParams{RunID: run, Status: "succeeded", ExitCode: &zero, FinishedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	publication, err := store.GetAppDataPublication(appID)
	if err != nil {
		t.Fatal(err)
	}
	return publication
}

func TestRecoveryPreservesConsumerBootDataGeneration(t *testing.T) {
	for _, exists := range []bool{true, false} {
		t.Run(fmt.Sprintf("existing-projection=%t", exists), func(t *testing.T) {
			store := dbtest.New(t)
			app := mustCreateApp(t, store, "recovery-data")
			dep, err := store.BeginDeployment(app.ID, "v1", t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err = store.PromoteDeployment(dep.ID); err != nil {
				t.Fatal(err)
			}
			loaded := publishRecoveryData(t, store, app.ID, dep, `["loaded"]`)
			if exists {
				if err = store.UpsertReplica(db.UpsertReplicaParams{AppID: app.ID, Index: 0, Status: "running", Provider: "native", DeploymentID: &dep.ID, DataGeneration: loaded.Generation, DataProducerDeploymentID: loaded.ProducerDeploymentID, DataProducerAppVersion: loaded.ProducerAppVersion, DataProducerContentDigest: loaded.ProducerContentDigest, DataProducerFingerprint: loaded.ProducerFingerprint}); err != nil {
					t.Fatal(err)
				}
			}
			if err = store.UpsertDeploymentReplica(db.UpsertDeploymentReplicaParams{AppID: app.ID, DeploymentID: dep.ID, Index: 0, Status: "running", Provider: "native", DataGeneration: loaded.Generation}); err != nil {
				t.Fatal(err)
			}
			latest := publishRecoveryData(t, store, app.ID, dep, `["latest"]`)
			if latest.Generation == loaded.Generation {
				t.Fatal("fixture did not advance publication")
			}
			if err = store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: app.Slug, Status: "running"}); err != nil {
				t.Fatal(err)
			}
			inputs, err := lifecycle.PrepareRecovery(store)
			if err != nil {
				t.Fatal(err)
			}
			lifecycle.RecoverProcesses(store, process.NewManager(t.TempDir(), process.NewNativeRuntime()), proxy.New(), 0, false, "multiplex", nil, inputs)
			replicas, err := store.ListReplicas(app.ID)
			if err != nil || len(replicas) != 1 {
				t.Fatalf("replicas=%v err=%v", replicas, err)
			}
			if replicas[0].DataGeneration != loaded.Generation || (exists && replicas[0].DataProducerFingerprint != loaded.ProducerFingerprint) || replicas[0].DataProducerFingerprint == latest.ProducerFingerprint {
				t.Fatalf("recovery falsely restamped consumer data: generation=%d fingerprint=%s; loaded generation=%d fingerprint=%s", replicas[0].DataGeneration, replicas[0].DataProducerFingerprint, loaded.Generation, loaded.ProducerFingerprint)
			}
		})
	}
}

func TestRecoveryDoesNotBorrowUnknownCandidateDataFromPreviousDeployment(t *testing.T) {
	store := dbtest.New(t)
	app := mustCreateApp(t, store, "unknown-candidate-data")
	old, err := store.BeginDeployment(app.ID, "old", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = store.PromoteDeployment(old.ID); err != nil {
		t.Fatal(err)
	}
	if err = store.UpsertReplica(db.UpsertReplicaParams{AppID: app.ID, Index: 0, Status: "running", Provider: "native", DeploymentID: &old.ID, DataGeneration: 7, DataProducerFingerprint: "previous-consumer"}); err != nil {
		t.Fatal(err)
	}
	candidate, err := store.BeginDeployment(app.ID, "new", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = store.UpsertDeploymentReplica(db.UpsertDeploymentReplicaParams{AppID: app.ID, DeploymentID: candidate.ID, Index: 0, Status: "running", Provider: "native"}); err != nil {
		t.Fatal(err)
	}
	if err = store.PromoteDeployment(candidate.ID); err != nil {
		t.Fatal(err)
	}
	if err = store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: app.Slug, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	inputs, err := lifecycle.PrepareRecovery(store)
	if err != nil {
		t.Fatal(err)
	}
	lifecycle.RecoverProcesses(store, process.NewManager(t.TempDir(), process.NewNativeRuntime()), proxy.New(), 0, false, "multiplex", nil, inputs)
	replicas, err := store.ListReplicas(app.ID)
	if err != nil || len(replicas) != 1 {
		t.Fatalf("replicas=%v err=%v", replicas, err)
	}
	if replicas[0].DataGeneration != 0 || replicas[0].DataProducerFingerprint != "" {
		t.Fatalf("unknown candidate borrowed previous deployment provenance: %+v", replicas[0])
	}
}

func TestRecoveryPreservesCandidateStartupPeak(t *testing.T) {
	for _, peak := range []int64{512 << 20, 0} {
		t.Run(fmt.Sprintf("peak=%d", peak), func(t *testing.T) {
			store := dbtest.New(t)
			app := mustCreateApp(t, store, "candidate-startup-peak")
			old, err := store.BeginDeployment(app.ID, "old", t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err = store.PromoteDeployment(old.ID); err != nil {
				t.Fatal(err)
			}
			if err = store.UpsertReplica(db.UpsertReplicaParams{AppID: app.ID, Index: 0, Status: "running", Provider: "native", DeploymentID: &old.ID, StartupPeakRSSBytes: 64 << 20}); err != nil {
				t.Fatal(err)
			}
			candidate, err := store.BeginDeployment(app.ID, "new", t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err = store.UpsertDeploymentReplica(db.UpsertDeploymentReplicaParams{AppID: app.ID, DeploymentID: candidate.ID, Index: 0, Status: "running", Provider: "native", StartupPeakRSSBytes: peak}); err != nil {
				t.Fatal(err)
			}
			if err = store.PromoteDeployment(candidate.ID); err != nil {
				t.Fatal(err)
			}
			if err = store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: app.Slug, Status: "running"}); err != nil {
				t.Fatal(err)
			}
			inputs, err := lifecycle.PrepareRecovery(store)
			if err != nil {
				t.Fatal(err)
			}
			lifecycle.RecoverProcesses(store, process.NewManager(t.TempDir(), process.NewNativeRuntime()), proxy.New(), 0, false, "multiplex", nil, inputs)
			replicas, err := store.ListReplicas(app.ID)
			if err != nil || len(replicas) != 1 {
				t.Fatalf("replicas=%v err=%v", replicas, err)
			}
			if replicas[0].StartupPeakRSSBytes != peak {
				t.Fatalf("recovery lost candidate memory high-water mark: got %d want %d", replicas[0].StartupPeakRSSBytes, peak)
			}
		})
	}
}

func TestRecoveryRestoresMatchingPublicationAttribution(t *testing.T) {
	store := dbtest.New(t)
	app := mustCreateApp(t, store, "matching-publication")
	dep, err := store.BeginDeployment(app.ID, "v1", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = store.PromoteDeployment(dep.ID); err != nil {
		t.Fatal(err)
	}
	loaded := publishRecoveryData(t, store, app.ID, dep, `["loaded"]`)
	if err = store.UpsertDeploymentReplica(db.UpsertDeploymentReplicaParams{AppID: app.ID, DeploymentID: dep.ID, Index: 0, Status: "running", Provider: "native", DataGeneration: loaded.Generation}); err != nil {
		t.Fatal(err)
	}
	if err = store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: app.Slug, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	inputs, err := lifecycle.PrepareRecovery(store)
	if err != nil {
		t.Fatal(err)
	}
	lifecycle.RecoverProcesses(store, process.NewManager(t.TempDir(), process.NewNativeRuntime()), proxy.New(), 0, false, "multiplex", nil, inputs)
	replicas, err := store.ListReplicas(app.ID)
	if err != nil || len(replicas) != 1 {
		t.Fatalf("replicas=%v err=%v", replicas, err)
	}
	got := replicas[0]
	if got.DataGeneration != loaded.Generation || got.DataProducerDeploymentID == nil || *got.DataProducerDeploymentID != *loaded.ProducerDeploymentID || got.DataProducerAppVersion != loaded.ProducerAppVersion || got.DataProducerContentDigest != loaded.ProducerContentDigest || got.DataProducerFingerprint != loaded.ProducerFingerprint {
		t.Fatalf("matching durable publication attribution lost during recovery: %+v", got)
	}
}

func TestRecoveryPreservesLegacyProvenanceOnlyForSameRuntime(t *testing.T) {
	for _, tc := range []struct {
		name, provider, worker, endpoint string
		want                             int64
	}{
		{"same-runtime", "docker", "worker-a", "http://worker-a", 7},
		{"different-worker", "docker", "worker-b", "http://worker-a", 0},
		{"different-endpoint", "docker", "worker-a", "http://worker-b", 0},
		{"missing-runtime-identity", "docker", "", "", 0},
		{"task-worker-alias", "fargate", "worker-a", "http://worker-a", 0},
		{"service-worker-alias", "scaleway_serverless", "worker-a", "http://worker-a", 0},
		{"pidless-native-unit", "native", "worker-a", "http://worker-a", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := dbtest.New(t)
			app := mustCreateApp(t, store, "legacy-runtime")
			dep, err := store.BeginDeployment(app.ID, "v1", t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err = store.PromoteDeployment(dep.ID); err != nil {
				t.Fatal(err)
			}
			if err = store.UpsertReplica(db.UpsertReplicaParams{AppID: app.ID, Index: 0, Status: "running", Provider: tc.provider, WorkerID: "worker-a", EndpointURL: "http://worker-a", DeploymentID: &dep.ID, DataGeneration: 7, DataProducerFingerprint: "known-boot"}); err != nil {
				t.Fatal(err)
			}
			if err = store.UpsertDeploymentReplica(db.UpsertDeploymentReplicaParams{AppID: app.ID, DeploymentID: dep.ID, Index: 0, Status: "running", Provider: tc.provider, WorkerID: tc.worker, EndpointURL: tc.endpoint}); err != nil {
				t.Fatal(err)
			}
			if err = store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: app.Slug, Status: "running"}); err != nil {
				t.Fatal(err)
			}
			inputs, err := lifecycle.PrepareRecovery(store)
			if err != nil {
				t.Fatal(err)
			}
			lifecycle.RecoverProcesses(store, process.NewManager(t.TempDir(), &fakeDockerRuntime{}), proxy.New(), 0, false, "multiplex", nil, inputs)
			replicas, err := store.ListReplicas(app.ID)
			if err != nil || len(replicas) != 1 {
				t.Fatalf("replicas=%v err=%v", replicas, err)
			}
			if replicas[0].DataGeneration != tc.want {
				t.Fatalf("runtime identity %s yielded data generation %d want %d", tc.name, replicas[0].DataGeneration, tc.want)
			}
			if tc.want == 0 && replicas[0].DataProducerFingerprint != "" {
				t.Fatalf("another runtime inherited old producer attribution: %+v", replicas[0])
			}
		})
	}
}

func TestRecoveryRejectsLegacyProvenanceWhenPIDChangesOnSameWorker(t *testing.T) {
	store := dbtest.New(t)
	app := mustCreateApp(t, store, "changed-runtime-pid")
	dep, err := store.BeginDeployment(app.ID, "v1", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = store.PromoteDeployment(dep.ID); err != nil {
		t.Fatal(err)
	}
	oldPID, newPID := 101, 202
	if err = store.UpsertReplica(db.UpsertReplicaParams{AppID: app.ID, Index: 0, PID: &oldPID, Status: "running", Provider: "docker", WorkerID: "worker-a", EndpointURL: "http://worker-a", DeploymentID: &dep.ID, DataGeneration: 7, DataProducerFingerprint: "known-boot"}); err != nil {
		t.Fatal(err)
	}
	if err = store.UpsertDeploymentReplica(db.UpsertDeploymentReplicaParams{AppID: app.ID, DeploymentID: dep.ID, Index: 0, PID: &newPID, Status: "running", Provider: "docker", WorkerID: "worker-a", EndpointURL: "http://worker-a"}); err != nil {
		t.Fatal(err)
	}
	if err = store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: app.Slug, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	inputs, err := lifecycle.PrepareRecovery(store)
	if err != nil {
		t.Fatal(err)
	}
	lifecycle.RecoverProcesses(store, process.NewManager(t.TempDir(), &fakeDockerRuntime{}), proxy.New(), 0, false, "multiplex", nil, inputs)
	replicas, err := store.ListReplicas(app.ID)
	if err != nil || len(replicas) != 1 {
		t.Fatalf("replicas=%v err=%v", replicas, err)
	}
	if replicas[0].DataGeneration != 0 || replicas[0].DataProducerFingerprint != "" {
		t.Fatalf("new PID inherited old process provenance on same worker: %+v", replicas[0])
	}
}

func TestRecoveryPreservesLegacyProvenanceForSameNativeProcess(t *testing.T) {
	checkNativeRecoveryProvenance(t, false)
}

func TestRecoveryRejectsLegacyProvenanceWhenNativeListenerChanges(t *testing.T) {
	checkNativeRecoveryProvenance(t, true)
}

func checkNativeRecoveryProvenance(t *testing.T, changedPort bool) {
	t.Helper()
	store := dbtest.New(t)
	app := mustCreateApp(t, store, "legacy-native-process")
	dep, err := store.BeginDeployment(app.ID, "v1", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = store.PromoteDeployment(dep.ID); err != nil {
		t.Fatal(err)
	}
	pid, _ := startNativeProcess(t, dep.BundleDir)
	port := liveListener(t)
	// Legacy rows identify native listeners by port alone.
	endpoint := ""
	if err = store.UpsertReplica(db.UpsertReplicaParams{AppID: app.ID, Index: 0, PID: &pid, Port: &port, Status: "running", Provider: "native", EndpointURL: endpoint, DeploymentID: &dep.ID, DataGeneration: 7, DataProducerFingerprint: "known-boot", StartupPeakRSSBytes: 512 << 20}); err != nil {
		t.Fatal(err)
	}
	recoveredPort := port
	if changedPort {
		recoveredPort = liveListener(t)
	}
	if err = store.UpsertDeploymentReplica(db.UpsertDeploymentReplicaParams{AppID: app.ID, DeploymentID: dep.ID, Index: 0, PID: &pid, Port: &recoveredPort, Status: "running", Provider: "native", EndpointURL: endpoint}); err != nil {
		t.Fatal(err)
	}
	if err = store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: app.Slug, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	inputs, err := lifecycle.PrepareRecovery(store)
	if err != nil {
		t.Fatal(err)
	}
	mgr := process.NewManager(t.TempDir(), process.NewNativeRuntime())
	t.Cleanup(func() {
		if err := mgr.Stop(app.Slug); err != nil {
			t.Errorf("stop recovered native process: %v", err)
		}
	})
	lifecycle.RecoverProcesses(store, mgr, proxy.New(), 0, false, "multiplex", nil, inputs)
	replicas, err := store.ListReplicas(app.ID)
	if err != nil || len(replicas) != 1 {
		t.Fatalf("replicas=%v err=%v", replicas, err)
	}
	wantGeneration, wantPeak, wantFingerprint := int64(7), int64(512<<20), "known-boot"
	if changedPort {
		wantGeneration, wantPeak, wantFingerprint = 0, 0, ""
	}
	if replicas[0].Status != "running" || replicas[0].DataGeneration != wantGeneration || replicas[0].DataProducerFingerprint != wantFingerprint || replicas[0].StartupPeakRSSBytes != wantPeak {
		t.Fatalf("changed port=%v: want generation=%d peak=%d fingerprint=%q, got %+v", changedPort, wantGeneration, wantPeak, wantFingerprint, replicas[0])
	}
}

func TestRecoveryRepairsMissingMatchingPublicationAttribution(t *testing.T) {
	store := dbtest.New(t)
	app := mustCreateApp(t, store, "missing-publication-attribution")
	dep, err := store.BeginDeployment(app.ID, "v1", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = store.PromoteDeployment(dep.ID); err != nil {
		t.Fatal(err)
	}
	loaded := publishRecoveryData(t, store, app.ID, dep, `["loaded"]`)
	if err = store.UpsertReplica(db.UpsertReplicaParams{AppID: app.ID, Index: 0, Status: "running", Provider: "native", DeploymentID: &dep.ID, DataGeneration: loaded.Generation}); err != nil {
		t.Fatal(err)
	}
	if err = store.UpsertDeploymentReplica(db.UpsertDeploymentReplicaParams{AppID: app.ID, DeploymentID: dep.ID, Index: 0, Status: "running", Provider: "native", DataGeneration: loaded.Generation}); err != nil {
		t.Fatal(err)
	}
	if err = store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: app.Slug, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	inputs, err := lifecycle.PrepareRecovery(store)
	if err != nil {
		t.Fatal(err)
	}
	lifecycle.RecoverProcesses(store, process.NewManager(t.TempDir(), process.NewNativeRuntime()), proxy.New(), 0, false, "multiplex", nil, inputs)
	replicas, err := store.ListReplicas(app.ID)
	if err != nil || len(replicas) != 1 {
		t.Fatalf("replicas=%v err=%v", replicas, err)
	}
	got := replicas[0]
	if got.DataGeneration != loaded.Generation || got.DataProducerDeploymentID == nil || *got.DataProducerDeploymentID != *loaded.ProducerDeploymentID || got.DataProducerAppVersion != loaded.ProducerAppVersion || got.DataProducerContentDigest != loaded.ProducerContentDigest || got.DataProducerFingerprint != loaded.ProducerFingerprint {
		t.Fatalf("matching durable publication attribution lost during recovery: %+v", got)
	}
}

func TestRecoveryDoesNotTransferActivationOwnershipToCandidate(t *testing.T) {
	store := dbtest.New(t)
	app := mustCreateApp(t, store, "candidate-activation-owner")
	old, err := store.BeginDeployment(app.ID, "old", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = store.PromoteDeployment(old.ID); err != nil {
		t.Fatal(err)
	}
	scheduleID, err := store.CreateSchedule(db.CreateScheduleParams{AppID: app.ID, Name: "refresh", CronExpr: "0 5 * * *", CommandJSON: `["produce"]`, Enabled: true, OnSuccess: "roll", RollFallback: "defer"})
	if err != nil {
		t.Fatal(err)
	}
	runID, err := store.InsertScheduleRun(db.InsertScheduleRunParams{ScheduleID: scheduleID, Status: "running", StartedAt: time.Now().UTC(), Trigger: "manual", OnSuccess: "roll", RollFallback: "defer", DeploymentID: &old.ID, AppVersion: old.Version, PublishesData: true})
	if err != nil {
		t.Fatal(err)
	}
	zero := 0
	activation, err := store.CompleteScheduleRunAndEnqueueActivation(db.CompleteScheduleRunParams{RunID: runID, Status: "succeeded", ExitCode: &zero, FinishedAt: time.Now().UTC()})
	if err != nil || activation == nil {
		t.Fatalf("activation=%v err=%v", activation, err)
	}
	if err = store.UpsertActivationReplica(db.UpsertReplicaParams{AppID: app.ID, Index: 0, Status: "running", Provider: "native", DeploymentID: &old.ID}, activation.TargetGeneration, activation.ID); err != nil {
		t.Fatal(err)
	}
	candidate, err := store.BeginDeployment(app.ID, "candidate", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = store.UpsertDeploymentReplica(db.UpsertDeploymentReplicaParams{AppID: app.ID, DeploymentID: candidate.ID, Index: 0, Status: "running", Provider: "native", DataGeneration: activation.TargetGeneration}); err != nil {
		t.Fatal(err)
	}
	if err = store.PromoteDeployment(candidate.ID); err != nil {
		t.Fatal(err)
	}
	if err = store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: app.Slug, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	inputs, err := lifecycle.PrepareRecovery(store)
	if err != nil {
		t.Fatal(err)
	}
	lifecycle.RecoverProcesses(store, process.NewManager(t.TempDir(), process.NewNativeRuntime()), proxy.New(), 0, false, "multiplex", nil, inputs)
	replicas, err := store.ListReplicas(app.ID)
	if err != nil || len(replicas) != 1 {
		t.Fatalf("replicas=%v err=%v", replicas, err)
	}
	if replicas[0].ActivationID != nil || replicas[0].DataGeneration != activation.TargetGeneration {
		t.Fatalf("candidate inherited another consumer's activation ownership: %+v", replicas[0])
	}
}

func TestRecoveryCarriesHistoricalActivationSourceWithoutOwnership(t *testing.T) {
	store := dbtest.New(t)
	app := mustCreateApp(t, store, "candidate-historical-source")
	old, err := store.BeginDeployment(app.ID, "old", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = store.PromoteDeployment(old.ID); err != nil {
		t.Fatal(err)
	}
	scheduleID, err := store.CreateSchedule(db.CreateScheduleParams{AppID: app.ID, Name: "refresh", CronExpr: "0 5 * * *", CommandJSON: `["produce"]`, Enabled: true, OnSuccess: "roll", RollFallback: "defer"})
	if err != nil {
		t.Fatal(err)
	}
	runID, err := store.InsertScheduleRun(db.InsertScheduleRunParams{ScheduleID: scheduleID, Status: "running", StartedAt: time.Now().UTC(), Trigger: "manual", OnSuccess: "roll", RollFallback: "defer", DeploymentID: &old.ID, AppVersion: old.Version, ContentDigest: "sha256:loaded", ProducerFingerprint: "loaded-producer", PublishesData: true})
	if err != nil {
		t.Fatal(err)
	}
	zero := 0
	activation, err := store.CompleteScheduleRunAndEnqueueActivation(db.CompleteScheduleRunParams{RunID: runID, Status: "succeeded", ExitCode: &zero, FinishedAt: time.Now().UTC()})
	if err != nil || activation == nil {
		t.Fatalf("activation=%v err=%v", activation, err)
	}
	if err = store.UpsertActivationReplica(db.UpsertReplicaParams{AppID: app.ID, Index: 0, Status: "running", Provider: "native", DeploymentID: &old.ID}, activation.TargetGeneration, activation.ID); err != nil {
		t.Fatal(err)
	}
	publishRecoveryData(t, store, app.ID, old, `["newer-publication"]`)
	candidate, err := store.BeginDeployment(app.ID, "candidate", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = store.UpsertDeploymentReplica(db.UpsertDeploymentReplicaParams{AppID: app.ID, DeploymentID: candidate.ID, Index: 0, Status: "running", Provider: "native", DataGeneration: activation.TargetGeneration}); err != nil {
		t.Fatal(err)
	}
	if err = store.PromoteDeployment(candidate.ID); err != nil {
		t.Fatal(err)
	}
	if err = store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: app.Slug, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	inputs, err := lifecycle.PrepareRecovery(store)
	if err != nil {
		t.Fatal(err)
	}
	lifecycle.RecoverProcesses(store, process.NewManager(t.TempDir(), process.NewNativeRuntime()), proxy.New(), 0, false, "multiplex", nil, inputs)
	replicas, err := store.ListReplicas(app.ID)
	if err != nil || len(replicas) != 1 {
		t.Fatalf("replicas=%v err=%v", replicas, err)
	}
	if replicas[0].ActivationID != nil || replicas[0].DataGeneration != activation.TargetGeneration || replicas[0].DataProducerFingerprint != "loaded-producer" || replicas[0].DataProducerContentDigest != "sha256:loaded" || replicas[0].DataProducerDeploymentID == nil || *replicas[0].DataProducerDeploymentID != old.ID || replicas[0].DataProducerAppVersion != old.Version {
		t.Fatalf("candidate inherited another consumer's activation ownership: %+v", replicas[0])
	}
}

func TestRecoveryPreservesActivationOwnershipForSurvivingConsumer(t *testing.T) {
	store := dbtest.New(t)
	app := mustCreateApp(t, store, "surviving-activation-owner")
	old, err := store.BeginDeployment(app.ID, "old", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = store.PromoteDeployment(old.ID); err != nil {
		t.Fatal(err)
	}
	scheduleID, err := store.CreateSchedule(db.CreateScheduleParams{AppID: app.ID, Name: "refresh", CronExpr: "0 5 * * *", CommandJSON: `["produce"]`, Enabled: true, OnSuccess: "roll", RollFallback: "defer"})
	if err != nil {
		t.Fatal(err)
	}
	runID, err := store.InsertScheduleRun(db.InsertScheduleRunParams{ScheduleID: scheduleID, Status: "running", StartedAt: time.Now().UTC(), Trigger: "manual", OnSuccess: "roll", RollFallback: "defer", DeploymentID: &old.ID, AppVersion: old.Version, PublishesData: true})
	if err != nil {
		t.Fatal(err)
	}
	zero := 0
	activation, err := store.CompleteScheduleRunAndEnqueueActivation(db.CompleteScheduleRunParams{RunID: runID, Status: "succeeded", ExitCode: &zero, FinishedAt: time.Now().UTC()})
	if err != nil || activation == nil {
		t.Fatalf("activation=%v err=%v", activation, err)
	}
	pid, _ := startNativeProcess(t, old.BundleDir)
	port := liveListener(t)
	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
	if err = store.UpsertActivationReplica(db.UpsertReplicaParams{AppID: app.ID, Index: 0, PID: &pid, Port: &port, EndpointURL: endpoint, Status: "running", Provider: "native", DeploymentID: &old.ID}, activation.TargetGeneration, activation.ID); err != nil {
		t.Fatal(err)
	}
	candidate := old
	if err = store.UpsertDeploymentReplica(db.UpsertDeploymentReplicaParams{AppID: app.ID, DeploymentID: candidate.ID, Index: 0, PID: &pid, Port: &port, EndpointURL: endpoint, Status: "running", Provider: "native", DataGeneration: activation.TargetGeneration}); err != nil {
		t.Fatal(err)
	}
	if err = store.PromoteDeployment(candidate.ID); err != nil {
		t.Fatal(err)
	}
	if err = store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: app.Slug, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	inputs, err := lifecycle.PrepareRecovery(store)
	if err != nil {
		t.Fatal(err)
	}
	mgr := process.NewManager(t.TempDir(), process.NewNativeRuntime())
	t.Cleanup(func() {
		if err := mgr.Stop(app.Slug); err != nil {
			t.Errorf("stop recovered process: %v", err)
		}
	})
	lifecycle.RecoverProcesses(store, mgr, proxy.New(), 0, false, "multiplex", nil, inputs)
	replicas, err := store.ListReplicas(app.ID)
	if err != nil || len(replicas) != 1 {
		t.Fatalf("replicas=%v err=%v", replicas, err)
	}
	if replicas[0].ActivationID == nil || *replicas[0].ActivationID != activation.ID || replicas[0].DataGeneration != activation.TargetGeneration || replicas[0].Status != "running" {
		t.Fatalf("surviving consumer lost its activation ownership: %+v", replicas[0])
	}
}
