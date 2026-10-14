package api

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rvben/shinyhub/internal/activation"
	"github.com/rvben/shinyhub/internal/admission"
	"github.com/rvben/shinyhub/internal/config"
	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/process"
)

// checkUpdateCPU accounts for live worker requests before checking measured
// pressure. Callers hold the launch gate through the candidate readiness boundary.
func (s *Server) checkUpdateCPU(app *db.App, replicas int) error {
	cores, source, err := s.effectiveUpdateCPUCores()
	var available float64
	if err == nil && s.availableCPUCores != nil {
		available, err = s.availableCPUCores()
	} else if err == nil {
		ctx := s.generationCtx
		if ctx == nil {
			ctx = context.Background()
		}
		available, err = admission.AvailableCPUCores(ctx, cores, source)
	}
	if cores <= 0 || math.IsNaN(cores) || math.IsInf(cores, 0) || err != nil || available < 0 || math.IsNaN(available) || math.IsInf(available, 0) {
		return &activation.CapacityError{Resource: "cpu", Units: "cores", Reason: "CPU headroom could not be measured; current sessions are preserved", RetryAfter: time.Minute}
	}
	perWorker := s.cfg.Server.UpdateCapacity.CPURequest()
	needed := perWorker * float64(max(1, replicas))
	floor := s.cfg.Server.UpdateCapacity.CPUReserve(cores)
	if perWorker <= 0 || math.IsNaN(needed) || math.IsInf(needed, 0) || floor < 0 || math.IsNaN(floor) || math.IsInf(floor, 0) {
		return &activation.CapacityError{Resource: "cpu", Units: "cores", Reason: "CPU budget cannot be represented; check server.update_capacity worker requests and reserve; current sessions are preserved", RetryAfter: time.Minute}
	}
	liveWorkers := s.localLiveCPUWorkers()
	reserved := float64(liveWorkers) * perWorker
	if !s.cfg.Server.UpdateCapacity.CPUFits(cores, liveWorkers, max(1, replicas)) {
		remaining, required := math.Max(0, cores-reserved-floor), needed
		return &activation.CapacityError{Resource: "cpu", Units: "cores", Available: &remaining, Required: &required, Reason: fmt.Sprintf("CPU budget: %.2f cores allocated to live workers; %.2f requested for candidates plus %.2f reserved on a %.2f-core node; current sessions are preserved", reserved, needed, floor, cores), RetryAfter: time.Minute}
	}
	if !config.FitsCPUBudget(needed+floor, available) {
		required := needed + floor
		return &activation.CapacityError{Resource: "cpu", Units: "cores", Available: &available, Required: &required, Reason: fmt.Sprintf("CPU: %.2f cores available; need %.2f for %d startup workers plus %.2f cores of node headroom; current sessions are preserved", available, needed, max(1, replicas), floor), RetryAfter: time.Minute}
	}
	return nil
}

func (s *Server) checkUpdateSurge(app *db.App, candidates int) error {
	ceiling := s.cfg.Server.UpdateCapacity.MaxSurgeReplicas
	if ceiling == 0 || s.manager == nil {
		return nil
	}
	live := func(w *process.ProcessInfo) bool {
		return w != nil && w.Status != process.StatusStopped && w.Status != process.StatusCrashed
	}
	active, total := 0, 0
	for _, w := range s.manager.AllForSlug(app.Slug) {
		if live(w) {
			active++
		}
	}
	for _, w := range s.manager.All() {
		if w.Slug == app.Slug && live(w) {
			total++
		}
	}
	extra := total - active + max(1, candidates)
	if extra > ceiling {
		available, required := float64(max(0, ceiling-(total-active))), float64(max(1, candidates))
		return &activation.CapacityError{Resource: "surge", Units: "replicas", Available: &available, Required: &required,
			Reason: fmt.Sprintf("server.update_capacity.max_surge_replicas=%d cannot fit %d candidates and %d existing non-active workers; current sessions are preserved", ceiling, max(1, candidates), total-active), RetryAfter: time.Minute}
	}
	return nil
}

func capacityFailureReason(block *activation.CapacityError) string {
	return "capacity-blocked/" + block.Resource + ": " + block.Reason
}

func capacityConflictMessage(block *activation.CapacityError) string {
	if block.Resource == "surge" {
		return "working version preserved: " + block.Reason + "; retry after previous generations retire or increase server.update_capacity.max_surge_replicas; use --allow-downtime for an explicit stop-first deployment"
	}
	return "working version preserved: " + block.Reason + "; retry after freeing capacity, add capacity, or use --allow-downtime for an explicit stop-first deployment"
}

func writeCapacityConflict(w http.ResponseWriter, block *activation.CapacityError) {
	retry := max(1, int(block.RetryAfter.Seconds()))
	w.Header().Set("Retry-After", strconv.Itoa(retry))
	writeJSON(w, http.StatusConflict, map[string]any{
		"error":        capacityConflictMessage(block),
		"update_state": "blocked_capacity", "resource": block.Resource,
		"available": block.Available, "required": block.Required, "units": block.Units,
		"retry_after_seconds": retry, "automatic_retry": false, "working_version_preserved": true,
	})
}

// A blocked upload is an unsuccessful attempt, not a queued execution. Only
// scheduled rolls retry automatically; expose that distinction explicitly.
func (s *Server) blockedUpdateStatus(slug string) map[string]any {
	history, err := s.store.ListDeploymentsBySlug(slug)
	if err != nil || len(history) == 0 || history[0].Status != db.DeploymentFailed {
		return nil
	}
	latest := history[0]
	suffix, ok := strings.CutPrefix(latest.FailureReason, "capacity-blocked/")
	if !ok {
		return nil
	}
	resource, reason, ok := strings.Cut(suffix, ": ")
	if !ok {
		return nil
	}
	nextAction := "free or add capacity and retry the deployment; use --allow-downtime only for an intentional stop-first update"
	if resource == "surge" {
		nextAction = "retry after previous generations retire or increase server.update_capacity.max_surge_replicas; use --allow-downtime only for an intentional stop-first update"
	}
	return map[string]any{"state": "blocked_capacity", "resource": resource, "reason": reason,
		"deployment_id": latest.ID, "requested_at": latest.CreatedAt, "automatic_retry": false,
		"next_action": nextAction}
}

// localLiveCPUWorkers counts the same fleet for admission and startup advice.
// Idle, suspended, staged and draining workers retain their accounting requests.
func (s *Server) localLiveCPUWorkers() int {
	liveWorkers := 0
	if s.manager != nil {
		for _, worker := range s.manager.All() {
			if worker.Status == process.StatusStopped || worker.Status == process.StatusCrashed {
				continue
			}
			if worker.Provider == "fargate" || (s.nodeForTier != nil && s.nodeForTier(worker.Tier) != "") {
				continue
			}
			liveWorkers++
		}
	}

	return liveWorkers
}

// UpdateCPUCapacityWarning is a pressure-free snapshot after worker recovery.
// It cannot reserve capacity; admission rechecks under the launch gate.
func (s *Server) UpdateCPUCapacityWarning() string {
	cores, source, err := s.effectiveUpdateCPUCores()
	if err != nil {
		return "live-update CPU capacity could not be detected; parallel updates fail closed until CPU accounting is available"
	}
	warning := s.cfg.Server.UpdateCapacity.CPUFleetWarning(cores, s.localLiveCPUWorkers())
	if warning != "" {
		return warning + fmt.Sprintf(" (startup snapshot after worker recovery; capacity source: %s)", source)
	}
	return ""
}

// effectiveUpdateCPUCores is shared by admission and its pressure-free startup
// advice. An override never enlarges the quota of this process's cgroup.
func (s *Server) effectiveUpdateCPUCores() (float64, string, error) {
	cores, source := admission.Detect(s.cfg.HostCapacityCores())
	if s.hostCapacity != nil {
		cores, source = s.hostCapacity.Cores, s.hostCapacity.CoresSource
	}
	var err error
	if s.cpuQuotaReader == nil {
		detected := cores
		cores, err = admission.EffectiveCPUCores(cores)
		if cores < detected {
			source = "cgroup-quota"
		}
	} else {
		quota, limited, quotaErr := s.cpuQuotaReader.CPUCapacity()
		err = quotaErr
		if limited && quota < cores {
			cores, source = quota, "cgroup-quota"
		}
	}
	if err != nil {
		return cores, source, err
	}
	if cores <= 0 || math.IsNaN(cores) || math.IsInf(cores, 0) {
		return cores, source, fmt.Errorf("invalid effective CPU capacity")
	}
	return cores, source, nil
}
