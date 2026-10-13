package proxy

import (
	"net"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/config"
	"github.com/rvben/shinyhub/internal/upgrade"
)

func TestSealedTrackerRejectsLateHijack(t *testing.T) {
	p := New()
	p.SetPoolSize("dashboard", 1)
	if err := p.RegisterReplica("dashboard", 0, "http://localhost:1234", nil, 7); err != nil {
		t.Fatal(err)
	}
	first, peer := net.Pipe()
	defer peer.Close()
	backend := upgrade.RelayBackend{Slug: "dashboard", DeploymentID: 7, Index: 0, TargetURL: "http://localhost:1234"}
	tracked := p.conns.trackWithSession(first, ConnPrincipal{Slug: "dashboard"}, nil, nil, backend)
	defer tracked.Close()
	snapshot, ok := p.SealRelay()
	if !ok || len(snapshot) != 1 || snapshot[0].Sessions != 1 {
		t.Fatalf("seal=%v %t", snapshot, ok)
	}
	late, latePeer := net.Pipe()
	defer latePeer.Close()
	p.conns.trackWithSession(late, ConnPrincipal{}, nil, nil, backend)
	if p.ActiveUpgradedConns() != 1 {
		t.Fatal("late hijack enlarged sealed lease")
	}
	// Snapshot remains accurate even if the old manager changes its pool.
	p.Deregister("dashboard")
	if current := p.sealedRelaySnapshot(); len(current) != 1 || current[0].Sessions != 1 {
		t.Fatal("pool mutation froze or erased relay accounting")
	}
	tracked.Close()
	if len(p.sealedRelaySnapshot()) != 0 {
		t.Fatal("closed connection still held a lease")
	}
}

func TestRelayCountsProtectGenerationAndExactReplacement(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	lease, err := upgrade.NewRelayLease(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	backend := upgrade.RelayBackend{Slug: "dashboard", DeploymentID: 7, Index: 0, TargetURL: "http://localhost:1234", Sessions: 2}
	if err := lease.Publish([]upgrade.RelayBackend{backend}); err != nil {
		t.Fatal(err)
	}
	p := New()
	p.SetRelayLeases(&upgrade.RelayLeases{Dir: dir})
	p.SetPoolSize("dashboard", 1)
	if err := p.RegisterReplica("dashboard", 0, backend.TargetURL, nil, 7); err != nil {
		t.Fatal(err)
	}
	seedFailedWSAt(p, "dashboard", 2*wsWarnGrace)
	p.RecordActivity("dashboard")
	if ever, warning := p.ConnectivityHealth("dashboard"); !ever || warning {
		t.Fatalf("relayed connectivity = %t, %t", ever, warning)
	}
	if p.ReplicaSessionCounts("dashboard")[0] != 2 {
		t.Fatal("relay sessions omitted from admission")
	}
	if p.PoolSessionSnapshot()["dashboard"].Sessions != 2 {
		t.Fatal("relay sessions omitted from metrics")
	}
	if p.BeginHibernate("dashboard", time.Now().Add(time.Hour)) {
		t.Fatal("hibernation stopped a relayed session")
	}
	if err := p.StageGeneration("dashboard", 8, 1); err != nil {
		t.Fatal(err)
	}
	if err := p.RegisterGenerationReplica("dashboard", 8, 0, "http://localhost:5678", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ActivateGeneration("dashboard", 8); err != nil {
		t.Fatal(err)
	}
	if blockers, temporary := p.RelayUpgradeBlockers(); !temporary || len(blockers) != 1 || blockers[0] != "dashboard: draining generation" {
		t.Fatalf("blockers = %v", blockers)
	}
	if p.TryRetireGeneration("dashboard", 7) {
		t.Fatal("old generation retired over relay sessions")
	}
	if p.ReplicaSessionCounts("dashboard")[0] != 0 {
		t.Fatal("new generation inherited old relay sessions")
	}
	if err := lease.Publish(nil); err != nil {
		t.Fatal(err)
	}
	if !p.TryRetireGeneration("dashboard", 7) {
		t.Fatal("released relay blocked retirement")
	}
}

func TestRelayUpgradeBlockersNamesAndClassifiesAllPools(t *testing.T) {
	p := New()
	p.SetPoolMode("elastic", config.IsolationPerSession, 1, 4)
	p.SetPoolSize("candidate", 1)
	if err := p.RegisterReplica("candidate", 0, "http://localhost:1234", nil, 1); err != nil {
		t.Fatal(err)
	}
	if err := p.StageGeneration("candidate", 2, 1); err != nil {
		t.Fatal(err)
	}
	blockers, temporary := p.RelayUpgradeBlockers()
	if temporary || !reflect.DeepEqual(blockers, []string{"candidate: candidate generation", "elastic: elastic pool"}) {
		t.Fatalf("blockers = %v, temporary=%t", blockers, temporary)
	}
}

func TestRelayConnectionBlockersNameExactRetiredConnection(t *testing.T) {
	p := New()
	p.SetPoolSize("dashboard", 1)
	if err := p.RegisterReplica("dashboard", 0, "http://127.0.0.1:1234", nil, 7); err != nil {
		t.Fatal(err)
	}
	local, peer := net.Pipe()
	defer peer.Close()
	session := &wsSession{connectionID: "0123456789abcdef01234567"}
	tracked := p.conns.trackWithSession(local, ConnPrincipal{Slug: "dashboard"}, nil, session, upgrade.RelayBackend{Slug: "dashboard", DeploymentID: 7, Index: 0, TargetURL: "http://127.0.0.1:1234"})
	defer tracked.Close()
	if blockers := p.RelayConnectionBlockers(); len(blockers) != 0 {
		t.Fatal(blockers)
	}
	if err := p.RegisterReplica("dashboard", 0, "http://127.0.0.1:5678", nil, 8); err != nil {
		t.Fatal(err)
	}
	blockers := p.RelayConnectionBlockers()
	if len(blockers) != 1 {
		t.Fatal(blockers)
	}
	for _, want := range []string{"dashboard", session.connectionID, "deployment 7", "replica 0"} {
		if !strings.Contains(blockers[0], want) {
			t.Fatalf("missing %q in %q", want, blockers[0])
		}
	}
}
