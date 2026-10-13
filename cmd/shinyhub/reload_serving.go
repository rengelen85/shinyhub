package main

import (
	"fmt"
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
					serving = append(serving, upgrade.ReloadApp{ID: app.ID, Slug: app.Slug, MinimumReady: max(1, app.MinWarmReplicas)})
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
				rows, err := store.ListReplicas(app.ID)
				if err != nil {
					blockers = append(blockers, expected.Slug+": cannot verify replicas after reload")
					continue
				}
				ready := 0
				for _, row := range rows {
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
				if ready < max(1, expected.MinimumReady) {
					blockers = append(blockers, fmt.Sprintf("%s: %d ready replicas after reload, need %d (status %s)", app.Slug, ready, max(1, expected.MinimumReady), app.Status))
				}
			}
			return strings.Join(blockers, "; ")
		},
	}
}
