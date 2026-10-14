package main

import (
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/process"
	"github.com/rvben/shinyhub/internal/proxy"
	"github.com/rvben/shinyhub/internal/upgrade"
)

func reloadServingCheck(store *db.Store, mgr *process.Manager, prx *proxy.Proxy) upgrade.ReloadServingCheck {
	return upgrade.ReloadServingCheck{
		Snapshot: func() ([]upgrade.ReloadApp, error) {
			apps, err := store.ListAppsLean(0, 0)
			if err != nil {
				return nil, err
			}
			pools := prx.PoolSessionSnapshot()
			var serving []upgrade.ReloadApp
			for _, app := range apps {
				if pools[app.Slug].Replicas > 0 {
					ready, err := reloadReadyReplicas(app, store, mgr, prx, true)
					if err != nil {
						return nil, fmt.Errorf("%s: count ready replicas before reload: %w", app.Slug, err)
					}
					// A warm floor is a capacity target, not a prerequisite for
					// reloading a host already serving below that target.
					if ready > 0 {
						if ready < app.MinWarmReplicas {
							slog.Warn("reload preserves serving capacity below configured warm floor", "slug", app.Slug, "ready", ready, "min_warm_replicas", app.MinWarmReplicas)
						}
						serving = append(serving, upgrade.ReloadApp{ID: app.ID, Slug: app.Slug, MinimumReady: ready})
					}
				}
			}
			return serving, nil
		},
		Check: func(apps []upgrade.ReloadApp) string {
			var blockers []string
			for _, expected := range apps {
				app, err := store.GetAppByID(expected.ID)
				if err != nil || app.Slug != expected.Slug {
					blockers = append(blockers, expected.Slug+": serving app unavailable after reload")
					continue
				}
				ready, err := reloadReadyReplicas(app, store, mgr, prx, false)
				if err != nil {
					blockers = append(blockers, expected.Slug+": cannot verify replicas after reload")
					continue
				}
				if ready < max(1, expected.MinimumReady) {
					blockers = append(blockers, fmt.Sprintf("%s: %d ready replicas after reload, need %d (status %s)", app.Slug, ready, max(1, expected.MinimumReady), app.Status))
				}
			}
			return strings.Join(blockers, "; ")
		},
	}
}

// Use the same readiness evidence on both sides of the handoff: persisted and
// managed running state, a registered proxy target, and a reachable endpoint.
func reloadReadyReplicas(app *db.App, store *db.Store, mgr *process.Manager, prx *proxy.Proxy, canonicalOnly bool) (int, error) {
	rows, err := store.ListReplicas(app.ID)
	if err != nil {
		return 0, err
	}
	ready := 0
	for _, row := range rows {
		// A roll's surge slot is temporary and will disappear before handoff.
		// It must not raise the predecessor's durable serving obligation.
		if canonicalOnly && row.Index >= app.Replicas {
			continue
		}
		info, ok := mgr.GetReplica(app.Slug, row.Index)
		if row.Status != db.ReplicaStatusRunning || !ok || info.Status != process.StatusRunning ||
			row.EndpointURL == "" || prx.ReplicaTargetURL(app.Slug, row.Index) != row.EndpointURL || info.EndpointURL != row.EndpointURL {
			continue
		}
		endpoint, err := url.Parse(row.EndpointURL)
		if err != nil {
			continue
		}
		conn, err := net.DialTimeout("tcp", endpoint.Host, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			ready++
		}
	}
	return ready, nil
}
