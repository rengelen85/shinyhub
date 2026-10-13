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
	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/deploy"
)

// checkUpdateCPU reserves startup CPU for parallel workers while retaining a
// ten-percent node margin for existing sessions and the control plane. It is
// checked under the same launch reservation as memory admission.
func (s *Server) checkUpdateCPU(app *db.App, replicas int) error {
	cores, source := admission.Detect(s.cfg.HostCapacityCores())
	if s.hostCapacity != nil {
		cores, source = s.hostCapacity.Cores, s.hostCapacity.CoresSource
	}
	var available float64
	var err error
	if s.availableCPUCores != nil {
		available, err = s.availableCPUCores()
	} else {
		ctx := s.generationCtx
		if ctx == nil {
			ctx = context.Background()
		}
		available, err = admission.AvailableCPUCores(ctx, cores, source)
	}
	if cores <= 0 || math.IsNaN(cores) || math.IsInf(cores, 0) || err != nil || available < 0 || math.IsNaN(available) || math.IsInf(available, 0) {
		return &activation.CapacityError{Resource: "cpu", Units: "cores", Reason: "CPU headroom could not be measured; current sessions are preserved", RetryAfter: time.Minute}
	}
	_, defaultCPU := s.cfg.Runtime.DefaultResourcesForApp(app)
	quota := deploy.ResolveCPUQuotaPercent(app.CPUQuotaPercent, defaultCPU)
	// An unenforced or fractional quota does not prove a cold import uses less
	// than one core. Higher configured ceilings require the larger budget.
	perWorker := math.Max(1, float64(quota)/100)
	needed := perWorker * float64(max(1, replicas))
	floor := cores * 0.1
	if available < needed+floor {
		required := needed + floor
		return &activation.CapacityError{Resource: "cpu", Units: "cores", Available: &available, Required: &required, Reason: fmt.Sprintf("CPU: %.2f cores available; need %.2f for %d startup workers plus %.2f cores of node headroom; current sessions are preserved", available, needed, max(1, replicas), floor), RetryAfter: time.Minute}
	}
	return nil
}

func capacityFailureReason(block *activation.CapacityError) string {
	return "capacity-blocked/" + block.Resource + ": " + block.Reason
}

func capacityConflictMessage(block *activation.CapacityError) string {
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
	return map[string]any{"state": "blocked_capacity", "resource": resource, "reason": reason,
		"deployment_id": latest.ID, "requested_at": latest.CreatedAt, "automatic_retry": false,
		"next_action": "free or add capacity and retry the deployment; use --allow-downtime only for an intentional stop-first update"}
}
