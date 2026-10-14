package proxy

import (
	"context"
	"encoding/binary"
	"github.com/rvben/shinyhub/internal/upgrade"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"
)

func TestRetiredLifetimeClosesWithServiceRestartCode(t *testing.T) {
	p := New()
	client, peer := net.Pipe()
	defer peer.Close()
	backend := upgrade.RelayBackend{Slug: "app", DeploymentID: 1, Index: 0, TargetURL: "http://localhost:1234"}
	p.SetPoolSize("app", 1)
	if err := p.RegisterReplica("app", 0, backend.TargetURL, nil, 1); err != nil {
		t.Fatal(err)
	}
	session := &wsSession{}
	tracked := p.conns.trackWithSession(client, ConnPrincipal{Slug: "app"}, nil, session, backend)
	defer tracked.Close()
	snapshot, ok := p.SealRelay()
	if !ok {
		t.Fatal("relay did not seal")
	}
	dir := filepath.Join(t.TempDir(), "relays")
	lease, err := upgrade.NewRelayLease(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	if err := lease.Publish(snapshot); err != nil {
		t.Fatal(err)
	}
	read := make(chan []byte, 1)
	go func() { data, _ := io.ReadAll(peer); read <- data }()
	if forced := p.PreserveUpgraded(context.Background(), lease, time.Second, 20*time.Millisecond); forced != 1 {
		t.Fatalf("forced=%d", forced)
	}
	select {
	case data := <-read:
		if len(data) < 4 || data[0] != 0x88 || binary.BigEndian.Uint16(data[2:4]) != 1012 {
			t.Fatalf("reconnect close=%x", data)
		}
	case <-time.After(time.Second):
		t.Fatal("retired lifetime did not close transport")
	}
	leases := upgrade.RelayLeases{Dir: dir}
	if st := leases.Status(); st.Count != 1 || st.Sessions != 0 {
		t.Fatalf("expired lease status=%+v", st)
	}
	if p.ActiveUpgradedConns() != 0 {
		t.Fatal("expired retired process still owns sessions")
	}
}

func TestReconnectCloseDoesNotCorruptPartialWebSocketFrame(t *testing.T) {
	p := New()
	client, peer := net.Pipe()
	defer peer.Close()
	tracked := p.conns.trackWithSession(client, ConnPrincipal{}, nil, nil).(*trackedConn)
	consumed := make(chan []byte, 1)
	go func() { data, _ := io.ReadAll(peer); consumed <- data }()
	if _, err := tracked.Write([]byte{0x82, 0x04, 1}); err != nil {
		t.Fatal(err)
	}
	if err := tracked.closeForReconnect("retired_lifetime_exceeded"); err != nil {
		t.Fatal(err)
	}
	if data := <-consumed; len(data) != 3 {
		t.Fatalf("close frame inserted into partial data frame: %x", data)
	}
}
