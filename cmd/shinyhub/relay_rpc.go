package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/proxy"
	"github.com/rvben/shinyhub/internal/upgrade"
	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"
)

// A retiring relay talks only to the current binary. It keeps no database
// connection, so subsequent migrations do not extend the old schema contract.
type relayRequest struct {
	Protocol   int                        `json:"protocol"`
	Operation  string                     `json:"operation"`
	Principals []upgrade.RelayPrincipalV1 `json:"principals,omitempty"`
	Start      *upgrade.RelayUsageStartV1 `json:"start,omitempty"`
	IDs        []string                   `json:"ids,omitempty"`
	ID         string                     `json:"id,omitempty"`
	Ended      time.Time                  `json:"ended,omitempty"`
}
type relayResponse struct {
	Stored      bool   `json:"stored"`
	Revocations []bool `json:"revocations,omitempty"`
}

func relayMAC(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte("shinyhub-relay-rpc-v1\x00"))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

type relayOwner struct {
	server    *http.Server
	path      string
	identity  os.FileInfo
	registry  string
	once      sync.Once
	previous  []byte
	committed bool
	stateMu   sync.Mutex
	closed    bool
}

// Prefer durable private directories; Unix socket addresses must also fit the
// shorter macOS limit. Long installations can use systemd's runtime directory.
func relayIPCDirectory(registry string) (string, error) {
	for _, base := range []string{registry, os.Getenv("RUNTIME_DIRECTORY")} {
		if !filepath.IsAbs(base) || len(filepath.Join(base, "shinyhub-relay-1234567890", "owner-0123456789abcdef.sock")) >= 104 {
			continue
		}
		info, err := os.Lstat(base)
		if err != nil || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
			continue
		}
		if stat, ok := info.Sys().(*syscall.Stat_t); !ok || stat.Uid != uint32(os.Geteuid()) {
			continue
		}
		return os.MkdirTemp(base, "shinyhub-relay-")
	}
	// Keep even long macOS TMPDIR installations usable. Operators must exclude
	// these live private directories from age-based temporary-file cleanup.
	return os.MkdirTemp("/tmp", "shinyhub-relay-")
}

func startRelayOwner(dir, secret string, store *db.Store, recheck proxy.Reauthorizer) (*relayOwner, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("relay directory must be private")
	}
	socketDir, err := relayIPCDirectory(dir)
	if err != nil {
		return nil, err
	}
	keepSocketDir := false
	defer func() {
		if !keepSocketDir {
			os.RemoveAll(socketDir)
		}
	}()
	var id [8]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, err
	}
	path := filepath.Join(socketDir, "owner-"+hex.EncodeToString(id[:])+".sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	// A predecessor closing its listener must not unlink the successor's socket.
	ln.(*net.UnixListener).SetUnlinkOnClose(false)
	if err := os.Chmod(path, 0o600); err != nil {
		ln.Close()
		os.Remove(path)
		return nil, err
	}
	identity, err := os.Lstat(path)
	if err != nil {
		ln.Close()
		return nil, err
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/relay" {
			http.NotFound(w, r)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 8<<20))
		if err != nil {
			http.Error(w, "invalid request", 400)
			return
		}
		provided, err := hex.DecodeString(r.Header.Get("X-ShinyHub-Relay-MAC"))
		expected, _ := hex.DecodeString(relayMAC(secret, body))
		if err != nil || !hmac.Equal(provided, expected) {
			http.Error(w, "unauthorized", 401)
			return
		}
		var request relayRequest
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&request) != nil || request.Protocol != 1 || decoder.Decode(new(any)) != io.EOF {
			http.Error(w, "unsupported relay protocol", 400)
			return
		}
		response := relayResponse{}
		switch request.Operation {
		case "authorize":
			if len(request.Principals) > 256 || recheck == nil {
				http.Error(w, "invalid principals", 400)
				return
			}
			for _, principal := range request.Principals {
				revoked, _, checkErr := recheck(proxy.ConnPrincipal(principal))
				if checkErr != nil {
					err = checkErr
					break
				}
				response.Revocations = append(response.Revocations, revoked)
			}
		case "start":
			if request.Start == nil {
				http.Error(w, "invalid session", 400)
				return
			}
			response.Stored, err = store.BeginUsageSessionWithPolicy(db.UsageSessionStart(*request.Start))
		case "heartbeat":
			if len(request.IDs) > 500 {
				http.Error(w, "too many sessions", 400)
				return
			}
			err = store.HeartbeatUsageSessions(request.IDs)
		case "end":
			if request.ID == "" || request.Ended.IsZero() {
				http.Error(w, "invalid session", 400)
				return
			}
			err = store.EndUsageSession(request.ID, request.Ended)
			if errors.Is(err, db.ErrNotFound) {
				err = nil
			}
		default:
			http.Error(w, "unsupported action", 400)
			return
		}
		if err != nil {
			http.Error(w, "owner could not verify request", 503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	})
	srv := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 5 * time.Second}
	owner := &relayOwner{server: srv, path: path, identity: identity, registry: dir}
	keepSocketDir = true
	go func() { _ = srv.Serve(ln) }()
	return owner, nil
}

// Activation is serialized across binaries. Publishing happens before Ready;
// a child failing readiness restores the prior pointer before closing its socket.
func (o *relayOwner) publicationLock() (func(), error) {
	file, err := os.OpenFile(filepath.Join(o.registry, "owner.publish.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX); err != nil {
		file.Close()
		return nil, err
	}
	return func() { file.Close() }, nil
}
func writeRelayPointer(dir string, data []byte) error {
	file, err := os.CreateTemp(dir, ".relay-owner-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	_, err = file.Write(data)
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(file.Name(), filepath.Join(dir, "owner.current"))
}
func (o *relayOwner) Activate() error {
	o.stateMu.Lock()
	defer o.stateMu.Unlock()
	if o.closed {
		return errors.New("relay owner is closed")
	}
	release, err := o.publicationLock()
	if err != nil {
		return err
	}
	defer release()
	previous, err := os.ReadFile(filepath.Join(o.registry, "owner.current"))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := writeRelayPointer(o.registry, []byte(o.path)); err != nil {
		return err
	}
	o.previous = previous
	return nil
}
func (o *relayOwner) Commit() { o.stateMu.Lock(); defer o.stateMu.Unlock(); o.committed = true }

func (o *relayOwner) Close() {
	o.once.Do(func() {
		o.stateMu.Lock()
		o.closed = true
		o.stateMu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = o.server.Shutdown(ctx)
		_ = o.server.Close()
		o.stateMu.Lock()
		defer o.stateMu.Unlock()
		// Successful owners leave the pointer in place. A failed startup restores
		// its predecessor under the same publication lock, never over a newer owner.
		if !o.committed {
			if release, err := o.publicationLock(); err == nil {
				dir := o.registry
				if current, err := os.ReadFile(filepath.Join(dir, "owner.current")); err == nil && string(current) == o.path {
					if len(o.previous) > 0 {
						_ = writeRelayPointer(dir, o.previous)
					} else {
						_ = os.Remove(filepath.Join(dir, "owner.current"))
					}
				}
				release()
			}
		}

		if info, err := os.Lstat(o.path); err == nil && os.SameFile(info, o.identity) {
			_ = os.Remove(o.path)
			_ = os.Remove(filepath.Dir(o.path))
		}
	})
}

type relayClient struct {
	client *http.Client
	secret string
}

func newRelayClient(dir, secret string) *relayClient {
	transport := &http.Transport{DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			name, err := os.ReadFile(filepath.Join(dir, "owner.current"))
			if err != nil {
				return nil, err
			}
			path := string(name)
			base := filepath.Base(path)
			if !filepath.IsAbs(path) || len(base) != len("owner-")+16+len(".sock") || !strings.HasPrefix(base, "owner-") || !strings.HasSuffix(base, ".sock") {
				return nil, errors.New("invalid relay owner endpoint")
			}
			if _, err := hex.DecodeString(base[len("owner-") : len(base)-len(".sock")]); err != nil {
				return nil, err
			}
			info, err := os.Lstat(filepath.Dir(path))
			if err != nil {
				return nil, err
			}
			stat, ok := info.Sys().(*syscall.Stat_t)
			if !info.IsDir() || info.Mode().Perm()&0077 != 0 || !ok || stat.Uid != uint32(os.Geteuid()) {
				return nil, errors.New("invalid relay socket directory")
			}
			return (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, "unix", path)
		}}
	return &relayClient{client: &http.Client{Transport: transport, Timeout: 5 * time.Second}, secret: secret}
}

func (c *relayClient) call(request relayRequest) (relayResponse, error) {
	return c.callContext(context.Background(), request)
}

func (c *relayClient) callContext(ctx context.Context, request relayRequest) (relayResponse, error) {
	request.Protocol = 1
	body, err := json.Marshal(request)
	if err != nil {
		return relayResponse{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://relay/relay", bytes.NewReader(body))
	if err != nil {
		return relayResponse{}, err
	}
	req.Header.Set("X-ShinyHub-Relay-MAC", relayMAC(c.secret, body))
	req.Header.Set("User-Agent", "shinyhub-session-relay")
	resp, err := c.client.Do(req)
	if err != nil {
		return relayResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return relayResponse{}, fmt.Errorf("relay owner status %d", resp.StatusCode)
	}
	var response relayResponse
	err = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&response)
	return response, err
}

// Relay authorization is batched, so an unavailable owner costs one bounded
// RPC per sweep rather than a timeout for each connected principal.
func (c *relayClient) authorize(principals []proxy.ConnPrincipal) (map[proxy.ConnPrincipal]bool, error) {
	return c.authorizeContext(context.Background(), principals)
}

func (c *relayClient) authorizeContext(ctx context.Context, principals []proxy.ConnPrincipal) (map[proxy.ConnPrincipal]bool, error) {
	verdicts := make(map[proxy.ConnPrincipal]bool, len(principals))
	for offset := 0; offset < len(principals); offset += 256 {
		chunk := principals[offset:min(offset+256, len(principals))]
		wire := make([]upgrade.RelayPrincipalV1, len(chunk))
		for i, principal := range chunk {
			wire[i] = upgrade.RelayPrincipalV1(principal)
		}
		response, err := c.callContext(ctx, relayRequest{Operation: "authorize", Principals: wire})
		if err != nil {
			return nil, err
		}
		if len(response.Revocations) != len(chunk) {
			return nil, errors.New("incomplete relay authorization")
		}
		for i, principal := range chunk {
			verdicts[principal] = response.Revocations[i]
		}
	}
	// Empty relays still verify that a current owner can renew the lease.
	if len(principals) == 0 {
		if _, err := c.callContext(ctx, relayRequest{Operation: "authorize"}); err != nil {
			return nil, err
		}
	}

	return verdicts, nil
}

// A separate expiry timer keeps the fail-closed deadline independent of a slow
// sweep. The sole RPC worker is joined before callers close relay dependencies.
func runRelayAuthorization(ctx context.Context, prx *proxy.Proxy, client *relayClient, every, grace time.Duration) {
	type result struct {
		verdicts map[proxy.ConnPrincipal]bool
		err      error
	}
	results := make(chan result, 1)
	var worker sync.WaitGroup
	defer worker.Wait()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	deadline := time.NewTimer(grace)
	defer deadline.Stop()
	ticker := time.NewTicker(max(time.Millisecond, every))
	defer ticker.Stop()
	busy := false
	start := func() {
		if busy {
			return
		}
		busy = true
		principals := prx.UpgradedPrincipals()
		worker.Add(1)
		go func() {
			defer worker.Done()
			verdicts, err := client.authorizeContext(ctx, principals)
			results <- result{verdicts, err}
		}()
	}
	start()
	for {
		select {
		case <-ctx.Done():
			return
		case <-deadline.C:
			prx.CloseUnverifiedUpgraded()
			return
		case <-ticker.C:
			start()
		case response := <-results:
			busy = false
			if response.err != nil {
				continue
			}
			prx.RecheckSessions(func(principal proxy.ConnPrincipal) (bool, string, error) {
				revoked, known := response.verdicts[principal]
				if !known {
					return true, "unverified relay principal", nil
				}
				return revoked, "access revoked", nil
			})
			if !deadline.Stop() {
				select {
				case <-deadline.C:
				default:
				}
			}
			deadline.Reset(grace)
		}
	}
}

func (c *relayClient) BeginUsageSession(start db.UsageSessionStart) error {
	_, err := c.BeginUsageSessionWithPolicy(start)
	return err
}
func (c *relayClient) BeginUsageSessionWithPolicy(start db.UsageSessionStart) (bool, error) {
	wire := upgrade.RelayUsageStartV1(start)
	response, err := c.call(relayRequest{Operation: "start", Start: &wire})
	return response.Stored, err
}
func (c *relayClient) HeartbeatUsageSessions(ids []string) error {
	_, err := c.call(relayRequest{Operation: "heartbeat", IDs: ids})
	return err
}
func (c *relayClient) EndUsageSession(id string, ended time.Time) error {
	_, err := c.call(relayRequest{Operation: "end", ID: id, Ended: ended})
	return err
}

// Capability probing must not load configuration or open the database.
func newRelayProtocolCmd() *cobra.Command {
	return &cobra.Command{Use: "relay-protocol", Hidden: true, Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error { _, err := fmt.Fprintln(cmd.OutOrStdout(), "1"); return err }}
}
