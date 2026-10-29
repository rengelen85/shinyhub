package api

import (
	"errors"
	"fmt"

	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/deploy"
)

// projectScheduleRetirements is read-only: planning must not disable the serving
// generation's jobs. Only a successful publication commits these declarations.
// A sparse environment overlay explicitly preserves the existing schedule set.
func (s *Server) projectScheduleRetirements(appID int64, manifest *deploy.Manifest, existing []*db.Schedule) ([]*db.Schedule, []*db.Schedule, error) {
	declared := map[string]bool{}
	if manifest != nil {
		for _, spec := range manifest.Schedules {
			declared[spec.Name] = true
		}
	}
	legacy := map[string]bool{}
	// Upgrade legacy rows only when the currently serving bundle proves ownership.
	// Historical or orphaned names are deliberately not guessed to be bundle-owned.
	needsLegacy := false
	for _, schedule := range existing {
		if schedule.ManagedBy != db.ScheduleManagedByBundle {
			needsLegacy = true
			break
		}
	}
	if appID != 0 && needsLegacy {
		serving, err := s.store.GetServingDeployment(appID)
		if err != nil && !errors.Is(err, db.ErrNotFound) {
			return nil, nil, err
		}
		if serving != nil && serving.BundleDir != "" {
			previous, err := deploy.LoadManifest(serving.BundleDir)
			if err != nil {
				return nil, nil, fmt.Errorf("read serving schedule ownership: %w", err)
			}
			if previous != nil {
				for _, spec := range previous.Schedules {
					legacy[spec.Name] = true
				}
			}
		}
	}
	var projected, retirements []*db.Schedule
	for _, schedule := range existing {
		candidate := *schedule
		if legacy[candidate.Name] {
			candidate.ManagedBy = db.ScheduleManagedByBundle
		}
		if (manifest == nil || !manifest.Sparse) && candidate.ManagedBy == db.ScheduleManagedByBundle && !declared[candidate.Name] && !candidate.Retired {
			candidate.Enabled = false
			candidate.Retired = true
			retirements = append(retirements, &candidate)
		}
		projected = append(projected, &candidate)
	}
	return projected, retirements, nil
}
