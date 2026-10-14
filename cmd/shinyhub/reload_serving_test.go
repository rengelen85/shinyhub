package main

import (
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/process"
	"github.com/rvben/shinyhub/internal/proxy"
	"github.com/rvben/shinyhub/internal/upgrade"
)

func TestReloadServingReadinessRejectsMissingStoppedAndUndialableReplica(t *testing.T) {
	store, app := storedProducerFixture(t, "roll", "first_deploy", true)
	dep, err := store.BeginDeployment(app.ID, "v1", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PromoteDeployment(dep.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateAppStatus(db.UpdateAppStatusParams{Slug: app.Slug, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	_, portText, _ := net.SplitHostPort(listener.Addr().String())
	port, _ := strconv.Atoi(portText)
	mgr := process.NewManager(t.TempDir(), process.NewNativeRuntime())
	t.Cleanup(func() { _ = mgr.StopAll() })
	info, err := mgr.Start(process.StartParams{Slug: app.Slug, Dir: dep.BundleDir, Command: []string{"sleep", "30"}, Port: port, DeploymentID: dep.ID})
	if err != nil {
		t.Fatal(err)
	}
	prx := proxy.New()
	prx.SetPoolSize(app.Slug, 1)
	if err := prx.RegisterReplica(app.Slug, 0, info.EndpointURL, nil, dep.ID); err != nil {
		t.Fatal(err)
	}
	check := reloadServingCheck(store, mgr, prx)
	apps, err := check.Snapshot()
	if err != nil || len(apps) != 0 {
		t.Fatalf("serving snapshot = %v, %v", apps, err)
	}
	apps = []upgrade.ReloadApp{{ID: app.ID, Slug: app.Slug, MinimumReady: 1}}
	if reason := check.Check(apps); !strings.Contains(reason, "0 ready replicas") {
		t.Fatalf("missing row passed: %q", reason)
	}
	params := db.UpsertReplicaParams{AppID: app.ID, Index: 0, PID: &info.PID, Port: &port, EndpointURL: info.EndpointURL, Provider: "native", Status: "running", DeploymentID: &dep.ID, DesiredState: "running"}
	if err := store.UpsertReplica(params); err != nil {
		t.Fatal(err)
	}
	if reason := check.Check(apps); reason != "" {
		t.Fatalf("healthy replica refused: %q", reason)
	}
	for _, floor := range []int{0, 1, 16} {
		if err := store.UpdateAppMinWarmReplicas(app.ID, floor); err != nil {
			t.Fatal(err)
		}
		snapshot, err := check.Snapshot()
		if err != nil || len(snapshot) != 1 || snapshot[0].MinimumReady != 1 {
			t.Fatalf("floor %d with one ready replica: %v, %v", floor, snapshot, err)
		}
		if reason := check.Check(snapshot); reason != "" {
			t.Fatalf("unchanged serving below floor %d refused: %q", floor, reason)
		}
	}
	apps[0].MinimumReady = 2
	if reason := check.Check(apps); !strings.Contains(reason, "1 ready replicas after reload, need 2") {
		t.Fatalf("missing warm floor passed: %q", reason)
	}
	apps[0].MinimumReady = 1
	params.Status = "stopped"
	if err := store.UpsertReplica(params); err != nil {
		t.Fatal(err)
	}
	if reason := check.Check(apps); !strings.Contains(reason, "0 ready replicas") {
		t.Fatalf("stopped replica passed: %q", reason)
	}
	params.Status = "running"
	if err := store.UpsertReplica(params); err != nil {
		t.Fatal(err)
	}
	listener.Close()
	if reason := check.Check(apps); !strings.Contains(reason, "0 ready replicas") {
		t.Fatalf("stale endpoint passed: %q", reason)
	}
	if snapshot, err := check.Snapshot(); err != nil || len(snapshot) != 0 {
		t.Fatalf("unreachable predecessor created an obligation: %v, %v", snapshot, err)
	}
}

func TestReloadServingSnapshotCapsFloorAtReadyReplicas(t *testing.T) {
	store, app := storedProducerFixture(t, "roll", "first_deploy", true)
	dep, err := store.BeginDeployment(app.ID, "v1", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PromoteDeployment(dep.ID); err != nil {
		t.Fatal(err)
	}
	mgr := process.NewManager(t.TempDir(), process.NewNativeRuntime())
	t.Cleanup(func() { _ = mgr.StopAll() })
	prx := proxy.New()
	if err := store.UpdateAppReplicas(app.ID, 16); err != nil {
		t.Fatal(err)
	}
	prx.SetPoolSize(app.Slug, 17)
	var params []db.UpsertReplicaParams
	for index := range 17 {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = listener.Close() })
		port := listener.Addr().(*net.TCPAddr).Port
		info, err := mgr.Start(process.StartParams{Slug: app.Slug, Index: index, Dir: dep.BundleDir, Command: []string{"sleep", "300"}, Port: port, DeploymentID: dep.ID})
		if err != nil {
			t.Fatal(err)
		}
		if err := prx.RegisterReplica(app.Slug, index, info.EndpointURL, nil, dep.ID); err != nil {
			t.Fatal(err)
		}
		params = append(params, db.UpsertReplicaParams{AppID: app.ID, Index: index, PID: &info.PID, Port: &port, EndpointURL: info.EndpointURL, Provider: "native", Status: "running", DeploymentID: &dep.ID, DesiredState: "running"})
	}
	check := reloadServingCheck(store, mgr, prx)
	for _, tc := range []struct {
		name               string
		floor, ready, want int
		surge              bool
	}{
		{"small host", 16, 1, 1, false},
		{"one replica restarting", 16, 15, 15, false},
		{"temporary surge with one normal replica missing", 16, 15, 15, true},
		{"full floor", 16, 16, 16, false},
		{"above floor", 2, 16, 2, false},
		{"no warm floor", 0, 16, 1, false},
		{"no ready replicas", 16, 0, 0, false},
		{"only a temporary surge ready", 16, 0, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := store.UpdateAppMinWarmReplicas(app.ID, tc.floor); err != nil {
				t.Fatal(err)
			}
			for i := range params {
				params[i].Status = "stopped"
				if i < tc.ready || (tc.surge && i == 16) {
					params[i].Status = "running"
				}
				if err := store.UpsertReplica(params[i]); err != nil {
					t.Fatal(err)
				}
			}
			apps, err := check.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			if tc.want == 0 {
				if len(apps) != 0 {
					t.Fatalf("no ready replicas: %+v", apps)
				}
				return
			}
			if len(apps) != 1 || apps[0].MinimumReady != tc.want {
				t.Fatalf("snapshot = %+v, want %d", apps, tc.want)
			}
			if reason := check.Check(apps); reason != "" {
				t.Fatalf("unchanged readiness refused: %s", reason)
			}
			for i := range params {
				params[i].Status = "stopped"
				if i < tc.want-1 {
					params[i].Status = "running"
				}
				if err := store.UpsertReplica(params[i]); err != nil {
					t.Fatal(err)
				}
			}
			if tc.surge {
				// A successor may use a live surge to satisfy its obligation,
				// even though the predecessor never requires a temporary slot.
				params[16].Status = "running"
				if err := store.UpsertReplica(params[16]); err != nil {
					t.Fatal(err)
				}
				if reason := check.Check(apps); reason != "" {
					t.Fatalf("ready successor surge refused: %q", reason)
				}
				params[16].Status = "stopped"
				if err := store.UpsertReplica(params[16]); err != nil {
					t.Fatal(err)
				}
			}
			if reason := check.Check(apps); !strings.Contains(reason, "ready replicas after reload, need") {
				t.Fatalf("serving regression passed: %q", reason)
			}
		})
	}
}
