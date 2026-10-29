package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/rvben/shinyhub/internal/deploy"
)

const failureScheduleManifestMismatch = "schedule_manifest_mismatch"

// A nil set means the bundle does not declare schedule policy. An empty,
// non-nil set means a full manifest is present with no schedules remaining.
// Use the upload's resolved bytes and ignore rules, including sparse manifests
// synthesized for environment-only settings on otherwise manifest-free apps.
func fleetDeclaredSchedules(spec bundleBuildSpec) (map[string]bool, error) {
	data := spec.Manifest
	if data == nil {
		var err error
		data, err = fleetSourceManifestData(spec.Dir)
		if err != nil {
			return nil, err
		}
	}
	if data == nil {
		return nil, nil
	}
	manifest, err := deploy.ParseManifest(data)
	if err != nil {
		return nil, err
	}
	if manifest.Sparse {
		return nil, nil
	}
	names := make(map[string]bool, len(manifest.Schedules))
	for _, schedule := range manifest.Schedules {
		names[schedule.Name] = true
	}
	return names, nil
}

func verifyScheduleDeclarations(schedules []scheduleDTO, declared map[string]bool, res *applyResult) error {
	if declared == nil {
		return nil
	}
	var extra []string
	for _, schedule := range schedules {
		if schedule.Enabled && !declared[schedule.Name] {
			extra = append(extra, fmt.Sprintf("%q", schedule.Name))
			res.freshnessGate = append(res.freshnessGate, scheduleGateOutcome{Schedule: schedule.Name, State: "undeclared"})
		}
	}
	if len(extra) == 0 {
		return nil
	}
	res.failureKind = failureScheduleManifestMismatch
	return fmt.Errorf("schedule verification gate unsatisfied: enabled server schedules absent from shinyhub.toml: %s; declare intentional schedules in the manifest, or use shinyhub schedule disable %s <name> or shinyhub schedule rm %s <name> for obsolete schedules; --prune only removes apps", strings.Join(extra, ", "), res.slug, res.slug)
}

// Check before optional warm/refresh actions so the CLI does not deliberately
// trigger a removed schedule while trying to verify this bundle.
func verifyScheduleDeclarationsContext(ctx context.Context, cfg *cliConfig, slug string, declared map[string]bool, res *applyResult) error {
	if declared == nil {
		return nil
	}
	schedules, err := listSchedulesContext(ctx, cfg, slug)
	if err != nil {
		res.failureKind = failureScheduleStateMissing
		return fmt.Errorf("verify schedule declarations: %w", err)
	}
	return verifyScheduleDeclarations(schedules, declared, res)
}
