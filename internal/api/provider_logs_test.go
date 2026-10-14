package api

import (
	"context"
	"github.com/rvben/shinyhub/internal/process"
	"testing"
)

type journalResourceReader struct{}

func (journalResourceReader) Read(_ context.Context, details process.ExternalLogs, _ string, _ int32) (process.ExternalLogPage, error) {
	return process.ExternalLogPage{Events: []process.ExternalLogEvent{{Message: details.Resource}}}, nil
}
func TestJournalLogReadsDoNotShareAcrossUnits(t *testing.T) {
	c := newProviderLogCoordinator(process.ExternalLogReaders{"systemd_journal": journalResourceReader{}})
	for _, unit := range []string{"first-unit", "second-unit", "first-unit"} {
		page, _, err := c.read(context.Background(), process.ExternalLogs{Provider: "systemd_journal", Resource: unit}, "", 10)
		if err != nil || len(page.Events) != 1 || page.Events[0].Message != unit {
			t.Fatalf("unit=%s page=%+v err=%v", unit, page, err)
		}
	}
	if !c.available(&process.ExternalLogs{Provider: "systemd_journal", Resource: "unit"}) {
		t.Fatal("configured journal unavailable")
	}
	if c.available(&process.ExternalLogs{Provider: "aws_ecs", LogGroup: "g", LogStream: "s"}) {
		t.Fatal("unconfigured AWS reader advertised")
	}
}
