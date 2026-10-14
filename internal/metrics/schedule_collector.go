package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// ScheduleSample is the metrics-local view of one schedule's last-success
// state. main.go adapts db.ScheduleFreshness into this so the metrics package
// never imports db. OK is false for a schedule that has never succeeded, in
// which case the collector emits no sample (absence = never succeeded).
type ScheduleSample struct {
	Slug                  string
	Name                  string
	LastSuccessUnix       int64
	OK                    bool
	ActivationStatus      string
	ActivationCreatedUnix int64
	ActivationGeneration  int64
	CapacityDeferredUnix  int64
	CapacityDeferrals     int
}

var scheduleLastSuccessDesc = prometheus.NewDesc(
	"shinyhub_schedule_last_success_seconds",
	"Unix timestamp of the last successful run of a schedule.",
	[]string{"slug", "schedule"}, nil,
)

var scheduleActivationStatusDesc = prometheus.NewDesc(
	"shinyhub_schedule_activation_status",
	"Current durable serving-data activation state (1 for the labeled state).",
	[]string{"slug", "schedule", "status"}, nil,
)

var scheduleActivationAgeDesc = prometheus.NewDesc(
	"shinyhub_schedule_activation_age_seconds",
	"Age in seconds of the current nonterminal serving-data activation.",
	[]string{"slug", "schedule"}, nil,
)

var scheduleActivationGenerationDesc = prometheus.NewDesc(
	"shinyhub_schedule_activation_target_generation",
	"Target serving-data generation of the latest activation.",
	[]string{"slug", "schedule"}, nil,
)

var scheduleCapacityDeferralsDesc = prometheus.NewDesc(
	"shinyhub_schedule_activation_capacity_deferrals",
	"Capacity deferral count of the latest activation, retained across coalesced producer successes.",
	[]string{"slug", "schedule"}, nil,
)
var scheduleCapacityDeferredAgeDesc = prometheus.NewDesc(
	"shinyhub_schedule_activation_capacity_deferred_age_seconds",
	"Seconds since the first capacity deferral of an outstanding activation, retained across coalesced producer successes.",
	[]string{"slug", "schedule"}, nil,
)

// scheduleFreshnessCollector emits one gauge per schedule that has ever
// succeeded, querying the store live at scrape time so the value survives a
// process restart (DB-backed, like apps_crashed).
type scheduleFreshnessCollector struct {
	query func() ([]ScheduleSample, error)
}

// NewScheduleFreshnessCollector builds the DB-backed last-success collector.
func NewScheduleFreshnessCollector(query func() ([]ScheduleSample, error)) prometheus.Collector {
	return &scheduleFreshnessCollector{query: query}
}

func (c *scheduleFreshnessCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- scheduleLastSuccessDesc
	ch <- scheduleActivationStatusDesc
	ch <- scheduleActivationAgeDesc
	ch <- scheduleActivationGenerationDesc
	ch <- scheduleCapacityDeferralsDesc
	ch <- scheduleCapacityDeferredAgeDesc
}

func (c *scheduleFreshnessCollector) Collect(ch chan<- prometheus.Metric) {
	samples, err := c.query()
	if err != nil {
		return // a scrape-time DB error yields no samples rather than a broken scrape
	}
	now := time.Now().Unix()
	for _, s := range samples {
		if s.OK {
			ch <- prometheus.MustNewConstMetric(
				scheduleLastSuccessDesc, prometheus.GaugeValue,
				float64(s.LastSuccessUnix), s.Slug, s.Name,
			)
		}
		if s.ActivationStatus == "" {
			continue
		}
		ch <- prometheus.MustNewConstMetric(scheduleActivationStatusDesc, prometheus.GaugeValue,
			1, s.Slug, s.Name, s.ActivationStatus)
		ch <- prometheus.MustNewConstMetric(scheduleCapacityDeferralsDesc, prometheus.GaugeValue,
			float64(s.CapacityDeferrals), s.Slug, s.Name)
		if s.CapacityDeferredUnix > 0 && nonterminalActivationMetricStatus(s.ActivationStatus) {
			age := max(int64(0), now-s.CapacityDeferredUnix)
			ch <- prometheus.MustNewConstMetric(scheduleCapacityDeferredAgeDesc, prometheus.GaugeValue,
				float64(age), s.Slug, s.Name)
		}
		if s.ActivationGeneration > 0 {
			ch <- prometheus.MustNewConstMetric(scheduleActivationGenerationDesc, prometheus.GaugeValue,
				float64(s.ActivationGeneration), s.Slug, s.Name)
		}
		if s.ActivationCreatedUnix > 0 && nonterminalActivationMetricStatus(s.ActivationStatus) {
			age := now - s.ActivationCreatedUnix
			if age < 0 {
				age = 0
			}
			ch <- prometheus.MustNewConstMetric(scheduleActivationAgeDesc, prometheus.GaugeValue,
				float64(age), s.Slug, s.Name)
		}
	}
}

func nonterminalActivationMetricStatus(status string) bool {
	switch status {
	case "pending", "deferred_interval", "deferred_capacity", "repairing", "running":
		return true
	default:
		return false
	}
}
