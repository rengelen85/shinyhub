package main

import (
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/process"
	"github.com/rvben/shinyhub/internal/proxy"
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
	if err != nil || len(apps) != 1 {
		t.Fatalf("serving snapshot = %v, %v", apps, err)
	}
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
}
