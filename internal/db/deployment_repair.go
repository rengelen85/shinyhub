package db

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// deploymentRepairConditionSQL applies to a deployments row aliased as failed.
// Admission, startup recovery and API observations must use the same fence.
const deploymentRepairConditionSQL = `failed.status IN ('pending', 'failed')
	AND (failed.producer_barrier_entered = 1 OR failed.prior_schedule_snapshot_recorded = 0)
	AND failed.id > COALESCE((
		SELECT MAX(ok.id) FROM deployments ok
		WHERE ok.app_id = failed.app_id AND ok.status = 'succeeded'
	), 0)`

// DeploymentRepairRequiredForApps reports only deployment compatibility fences,
// not running or failed schedule writers. This is a page-sized API read; keeping
// it out of GetAppBySlug avoids adding work to watcher and access hot paths.
func (s *Store) DeploymentRepairRequiredForApps(appIDs []int64) (map[int64]bool, error) {
	result := make(map[int64]bool, len(appIDs))
	if len(appIDs) == 0 {
		return result, nil
	}
	args := make([]any, len(appIDs))
	for i, id := range appIDs {
		args[i] = id
		result[id] = false
	}
	rows, err := s.db.Query(`SELECT DISTINCT failed.app_id FROM deployments failed
		WHERE failed.app_id IN (`+strings.TrimSuffix(strings.Repeat("?,", len(appIDs)), ",")+`)
		AND `+deploymentRepairConditionSQL, args...)
	if err != nil {
		return nil, fmt.Errorf("load deployment repair state: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan deployment repair state: %w", err)
		}
		result[id] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read deployment repair state: %w", err)
	}
	return result, nil
}

// AppProducerCompatibility names independent physical-writer blockers. A
// completed deployment does not imply that these per-schedule facts are repaired.
type AppProducerCompatibility struct {
	RepairProducers  []string
	RunningProducers []string
}

func (s *Store) ProducerCompatibilityForApps(appIDs []int64) (map[int64]AppProducerCompatibility, error) {
	result := make(map[int64]AppProducerCompatibility, len(appIDs))
	if len(appIDs) == 0 {
		return result, nil
	}
	args := make([]any, len(appIDs))
	for i, id := range appIDs {
		args[i] = id
		result[id] = AppProducerCompatibility{RepairProducers: []string{}, RunningProducers: []string{}}
	}
	rows, err := s.db.Query(`SELECT sc.app_id, sc.name,
 CASE WHEN EXISTS (SELECT 1 FROM schedule_data_uncertainty u WHERE u.schedule_id=sc.id) THEN 1 ELSE 0 END,
 CASE WHEN EXISTS (SELECT 1 FROM schedule_runs r WHERE r.schedule_id=sc.id AND r.publishes_data=1 AND r.status='running') THEN 1 ELSE 0 END
 FROM app_schedules sc WHERE sc.app_id IN (`+strings.TrimSuffix(strings.Repeat("?,", len(appIDs)), ",")+`) ORDER BY sc.app_id, sc.name`, args...)
	if err != nil {
		return nil, fmt.Errorf("load producer compatibility: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var appID int64
		var name string
		var repair, running int
		if err := rows.Scan(&appID, &name, &repair, &running); err != nil {
			return nil, err
		}
		state := result[appID]
		if repair != 0 {
			state.RepairProducers = append(state.RepairProducers, name)
		}
		if running != 0 {
			state.RunningProducers = append(state.RunningProducers, name)
		}
		result[appID] = state
	}
	return result, rows.Err()
}

// AppDeploymentRepairBlocker returns the oldest unresolved deployment fence.
// Later refused attempts may inherit it, so the latest deployment need not be
// the cause. A successful deployment clears all older fences.
func (s *Store) AppDeploymentRepairBlocker(appID int64) (*DeploymentSummary, error) {
	var dep DeploymentSummary
	err := s.db.QueryRow(`SELECT failed.id, failed.version, failed.status,
 COALESCE(failed.failure_reason, ''), failed.created_at FROM deployments failed
 WHERE failed.app_id = ? AND `+deploymentRepairConditionSQL+`
 ORDER BY failed.id LIMIT 1`, appID).Scan(&dep.ID, &dep.Version, &dep.Status, &dep.FailureReason, &dep.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load deployment repair blocker: %w", err)
	}
	return &dep, nil
}
