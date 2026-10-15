package api

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rvben/shinyhub/internal/db"
)

var errScheduleActivationInFlight = errors.New("scheduled data activation owns serving runtime state")
var errCompatibilityQuarantined = errors.New("app is compatibility-quarantined")
var errGenerationDraining = errors.New("previous generation is still draining")
var errRetiringProxy = errors.New("previous ShinyHub process still proxies sessions; retry after its reload drain completes")

// guardActivationLifecycle is called while the app's deploy lock is held.
// The lock excludes an active Roll call; the durable query excludes mutations
// between repair attempts, when no goroutine owns the lock but the activation
// still owns runtime and routing state.
func (s *Server) guardActivationLifecycle(appID int64, operation string) error {
	return s.guardActivationLifecycleWithDrain(appID, operation, false)
}

// Only generation handoffs may proceed beside independently retiring generations.
// Destructive operations retain the ordinary drain fence.
func (s *Server) guardActivationLifecycleWithDrain(appID int64, operation string, allowDrain bool) error {
	if s.proxy != nil {
		if s.proxy.RetiringParentAlive() {
			return fmt.Errorf("%s: %w", operation, errRetiringProxy)
		}
		app, err := s.store.GetAppByID(appID)
		if err != nil {
			return fmt.Errorf("%s: inspect generation drain: %w", operation, err)
		}
		if !allowDrain && (s.proxy.HasDrainingGeneration(app.Slug) || s.proxy.HasRelaySessions(app.Slug, 0)) {
			return fmt.Errorf("%s: %w; retry after retirement completes", operation, errGenerationDraining)
		}
	}
	inFlight, err := s.store.ScheduleActivationInFlight(appID)
	if err != nil {
		return fmt.Errorf("%s: check scheduled data activation: %w", operation, err)
	}
	if inFlight {
		return fmt.Errorf("%s: %w; retry after activation completes or repairs", operation, errScheduleActivationInFlight)
	}
	return nil
}

func (s *Server) guardCompatibilityQuarantine(appID int64, operation string) error {
	quarantined, err := s.store.AppCompatibilityQuarantined(appID)
	if err != nil {
		return fmt.Errorf("%s: check compatibility quarantine: %w", operation, err)
	}
	if quarantined {
		blocker, err := s.store.AppDeploymentRepairBlocker(appID)
		if err != nil {
			return fmt.Errorf("%s: inspect deployment repair state: %w", operation, err)
		}
		if blocker != nil {
			return fmt.Errorf("%s: %w; %s", operation, errCompatibilityQuarantined, deploymentRepairGuidance(blocker))
		}
		producers, err := s.store.ProducerCompatibilityForApps([]int64{appID})
		if err != nil {
			return fmt.Errorf("%s: inspect producer repair state: %w", operation, err)
		}
		app, err := s.store.GetAppByID(appID)
		if err != nil {
			return err
		}
		var commands []string
		for _, name := range producers[appID].RepairProducers {
			commands = append(commands, fmt.Sprintf("shinyhub schedule run %s %s --follow", app.Slug, name))
		}
		if len(commands) > 0 {
			return fmt.Errorf("%s: %w; repair producers with %s before starting consumers", operation, errCompatibilityQuarantined, strings.Join(commands, "; "))
		}
		return fmt.Errorf("%s: %w; wait for running data producers to finish before starting consumers", operation, errCompatibilityQuarantined)
	}
	return nil
}

func (s *Server) acquireConsumerBootGate(appID int64) (func(), error) {
	if s.jobs == nil {
		quarantined, err := s.store.AppCompatibilityQuarantined(appID)
		if err != nil {
			return nil, err
		}
		if quarantined {
			return nil, errCompatibilityQuarantined
		}
		return func() {}, nil
	}
	return s.jobs.AcquireCompatibleConsumerBootGate(appID)
}

// acquireRawConsumerBootGate is reserved for candidate convergence, which
// must be able to enter the fence precisely in order to repair an existing
// quarantine before promotion.
func (s *Server) acquireRawConsumerBootGate(appID int64) (func(), error) {
	if s.jobs == nil {
		return func() {}, nil
	}
	return s.jobs.AcquireConsumerBootGate(appID)
}

func deploymentRepairGuidance(dep *db.DeploymentSummary) string {
	detail := fmt.Sprintf("unresolved deployment producer barrier from deployment %d (%s, %s) at %s", dep.ID, dep.Version, dep.Status, dep.CreatedAt.UTC().Format(time.RFC3339))
	if dep.FailureReason != "" {
		detail += ": " + dep.FailureReason
	}
	if dep.Status == db.DeploymentPending {
		return detail + "; wait for the deployment to finish, or investigate an interrupted deployment before retrying"
	}
	return detail + "; successful producer runs alone do not clear this fence. Redeploy the desired bundle successfully, including its required data producers; use --allow-downtime to permit a stop-first deploy when replacing a working version"
}
