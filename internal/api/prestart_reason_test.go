package api

import (
	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/deploy"
	"strings"
	"testing"
)

func TestPrestartDowntimeReasonNamesRetainedSchedule(t *testing.T) {
	plan := &prestartSchedulePlan{producers: []*db.Schedule{{Name: "refresh-data"}}}
	reason := plan.downtimeReason(&deploy.Manifest{}, "dashboard")
	for _, want := range []string{"changes shared producer state", `schedule "refresh-data"`, "not in the manifest", "shinyhub schedule rm dashboard refresh-data"} {
		if !strings.Contains(reason, want) {
			t.Errorf("missing %q in %q", want, reason)
		}
	}
	if reason := plan.downtimeReason(nil, "dashboard"); strings.Contains(reason, "not in the manifest") {
		t.Fatal(reason)
	}
}
