package cli

import (
	"net/http"
	"strings"
	"testing"

	"github.com/rvben/shinyhub/internal/fleet"
)

func TestScheduleReconciliationPreflightPromotesUnchangedPlan(t *testing.T) {
	for _, action := range []fleet.Action{fleet.ActionUnchanged, fleet.ActionUpdateConfig, fleet.ActionAdopt} {
		t.Run(string(action), func(t *testing.T) {
			calls := 0
			ciTestTransport(t, func(r *http.Request) (*http.Response, error) {
				calls++
				if r.Method != "POST" || r.URL.Path != "/api/apps/demo/deploy-preflight" {
					t.Fatalf("unexpected request: %s %s", r.Method, r.URL)
				}
				return ciResponse(200, `{"valid":true,"problems":[],"schedule_changes":[{"name":"old","action":"retired","schedule_id":7}]}`), nil
			})
			manifest := &fleet.Manifest{Apps: []fleet.AppEntry{{Slug: "demo"}}}
			diff := []fleet.AppDiff{{Slug: "demo", Action: action, LocalDigest: "same", ServerDigest: "same"}}
			problems, err := fleetServerPreflight(&cliConfig{Host: "https://hub.example"}, serverCaps{DeployPreflight: true, ScheduleReconciliation: true, FleetPreconditions: true}, manifest, diff, map[string]fleetBundleFacts{"demo": {Dir: t.TempDir()}})
			if err != nil || len(problems) != 0 || calls != 1 {
				t.Fatalf("calls=%d problems=%v err=%v", calls, problems, err)
			}
			want := fleet.ActionUpdateSource
			if action == fleet.ActionUpdateConfig {
				want = fleet.ActionUpdateSourceConfig
			}
			if action == fleet.ActionAdopt {
				want = action
			}
			if diff[0].Action != want || !diff[0].ScheduleDrift || len(diff[0].Warnings) != 1 || !strings.Contains(diff[0].Warnings[0], "history is retained") || ownershipOnlyAdopt(diff[0], true) {
				t.Fatalf("plan=%+v", diff[0])
			}
		})
	}
}

func TestScheduleReconciliationVerificationRespectsOwnership(t *testing.T) {
	for _, tc := range []struct {
		name, owner   string
		enabled, fail bool
	}{
		{"manual", "manual", true, false}, {"managed orphan", "bundle", true, true}, {"legacy server", "", true, true}, {"retired", "bundle", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := applyResult{}
			err := verifyScheduleDeclarations([]scheduleDTO{{Name: "extra", Enabled: tc.enabled, ManagedBy: tc.owner}}, map[string]bool{}, &res)
			if (err != nil) != tc.fail {
				t.Fatalf("error=%v wantFailure=%v", err, tc.fail)
			}
		})
	}
}
