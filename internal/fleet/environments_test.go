package fleet

import (
	"strings"
	"testing"
)

const environmentManifest = `fleet_id = "dashboards"
[environments.dev]
host = "https://dev.example.com/"
[environments.prd]
host = "https://prd.example.com"
[[app]]
slug = "dashboard"
source = "./dashboard"
[app.config]
replicas = 1
min_warm_replicas = 1
render_seconds = 2
env = { MODE = "base", KEEP = "yes" }
autoscale = { enabled = true, min_replicas = 1, max_replicas = 4, target = 0.6 }
[app.environments.prd.config]
replicas = 16
min_warm_replicas = 16
render_seconds = 0
env = { MODE = "production" }
autoscale = { enabled = true, min_replicas = 16, max_replicas = 16, target = 0.6 }
`

func TestEnvironmentSelectionAndSparseMerge(t *testing.T) {
	for _, tc := range []struct {
		host, explicit, want string
		fail                 bool
	}{
		{"https://dev.example.com:443", "", "dev", false},
		{"https://prd.example.com/", "", "prd", false},
		{"https://unknown.example.com", "", "", true},
		{"https://unknown.example.com", "prd", "prd", false},
		{"https://dev.example.com", "prd", "", true},
		{"https://dev.example.com", "typo", "", true},
	} {
		t.Run(tc.host+tc.explicit, func(t *testing.T) {
			m, p := ParseManifest([]byte(environmentManifest), "fleet.toml")
			if len(p) > 0 {
				t.Fatal(p)
			}
			err := m.ResolveEnvironment(tc.host, tc.explicit)
			if (err != nil) != tc.fail {
				t.Fatalf("ResolveEnvironment error=%v", err)
			}
			if tc.fail {
				return
			}
			if m.ResolvedEnvironment != tc.want {
				t.Fatal(m.ResolvedEnvironment)
			}
			c := EffectiveConfig(m.Apps[0])
			if c.Env["KEEP"] != "yes" {
				t.Fatal("sparse env merge lost baseline")
			}
			if tc.want == "prd" && (*c.MinWarmReplicas != 16 || *c.RenderSeconds != 0 || c.Autoscale.MaxReplicas != 16 || c.Env["MODE"] != "production") {
				t.Fatalf("overlay not applied: %+v", c)
			}
			if tc.want == "dev" && *c.Replicas != 1 {
				t.Fatal("production leaked into dev")
			}
		})
	}
}

func TestEnvironmentValidationAllOverlays(t *testing.T) {
	for _, tc := range []struct{ find, replace, want string }{
		{"min_replicas = 16, max_replicas = 16", "min_replicas = 17, max_replicas = 16", "min_replicas"},
		{"[app.environments.prd.config]", "[app.environments.typo.config]", "undeclared environment"},
		{"render_seconds = 0", "visibility = 'public'", "unknown key"},
		{"render_seconds = 0", "project = 'prod'", "unknown key"},
		{"render_seconds = 0", "render_second = 0", "unknown key"},
		{"https://prd.example.com", "https://dev.example.com:443", "same host"},
		{"https://prd.example.com", "https://user:password@prd.example.com", "without credentials"},
	} {
		t.Run(tc.want+tc.replace, func(t *testing.T) {
			_, problems := ParseManifest([]byte(strings.Replace(environmentManifest, tc.find, tc.replace, 1)), "fleet.toml")
			var messages []string
			for _, p := range problems {
				messages = append(messages, p.Msg)
			}
			if !strings.Contains(strings.Join(messages, "\n"), tc.want) {
				t.Fatalf("problems=%v", problems)
			}
		})
	}
}

func TestEnvironmentDriftProvenanceAndRedaction(t *testing.T) {
	m, p := ParseManifest([]byte(environmentManifest), "")
	if len(p) > 0 {
		t.Fatal(p)
	}
	if err := m.ResolveEnvironment("https://prd.example.com", ""); err != nil {
		t.Fatal(err)
	}
	drift := ConfigDrift(m.Apps[0], ObservedApp{})
	seen := false
	for _, d := range drift {
		if d.Key == "min_warm_replicas" {
			seen = true
			if d.Environment != "prd" || d.Desired != "16" {
				t.Fatal(d)
			}
		}
		if strings.HasPrefix(d.Key, "env.") && (d.Server != "(redacted)" || d.Desired != "(redacted)") {
			t.Fatal("env value leaked")
		}
	}
	if !seen {
		t.Fatal("missing overlay drift")
	}
}
