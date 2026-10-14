//go:build linux

package process

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// This helper intentionally exits without stopping its replica, just like a
// controller cgroup killed by systemd. The app lives in its own broker unit.
func TestSystemdJournalControllerHelper(t *testing.T) {
	path := os.Getenv("BROKER_RESTART_TEST_ENDPOINT")
	if path == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rt, err := NewSystemdRuntime(ctx, os.Getenv("NATIVE_BROKER_TEST_SOCKET"))
	if err != nil {
		t.Fatal(err)
	}
	base := "/var/lib/shinyhub-isolation-test"
	ep, err := rt.Start(ctx, StartParams{AppID: 1, Slug: "alpha", Dir: base + "/apps/alpha/versions/v1", AppDataPath: base + "/appdata/alpha", AppCachePath: base + "/appcache/alpha", Port: 18489, DeploymentID: 18, LaunchID: "restart-log-fixture", Command: []string{"/usr/bin/python3", "-u", "-c", `from http.server import HTTPServer,BaseHTTPRequestHandler
class H(BaseHTTPRequestHandler):
 def do_GET(self):
  print("survived controller exit",flush=True)
  self.send_response(200);self.end_headers();self.wfile.write(b"same worker")
HTTPServer(("127.0.0.1",18489),H).serve_forever()`}}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(ep)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestSystemdReplicaAndJournalSurviveControllerExit(t *testing.T) {
	socket := os.Getenv("NATIVE_BROKER_TEST_SOCKET")
	if socket == "" {
		t.Skip("disposable systemd fixture required")
	}
	path := filepath.Join(t.TempDir(), "endpoint.json")
	child := exec.Command(os.Args[0], "-test.run=^TestSystemdJournalControllerHelper$")
	child.Env = append(os.Environ(), "BROKER_RESTART_TEST_ENDPOINT="+path)
	if output, err := child.CombinedOutput(); err != nil {
		t.Fatalf("launch helper: %v %s", err, output)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var ep ReplicaEndpoint
	if err := json.Unmarshal(raw, &ep); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rt, err := NewSystemdRuntime(ctx, socket)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.RemoveHandle(ep.Handle)
	if ep.ExternalLogs == nil || ep.ExternalLogs.Provider != "systemd_journal" {
		t.Fatalf("journal capability missing: %+v", ep)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, err := http.Get(ep.URL)
		if err == nil {
			response.Body.Close()
			if response.StatusCode == 200 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("worker did not serve after controller exit")
		}
		time.Sleep(20 * time.Millisecond)
	}
	pid, err := rt.InspectPID(ep.WorkerID)
	if err != nil || pid != ep.Handle.PID {
		t.Fatalf("worker changed: %d -> %d, %v", ep.Handle.PID, pid, err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		page, err := rt.Read(ctx, *ep.ExternalLogs, "", 100)
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range page.Events {
			if strings.Contains(event.Message, "survived controller exit") {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("journal did not retain output after controller exit")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
