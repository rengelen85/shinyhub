package appstatus

import "testing"

func TestProducerFailedStatusIsClassified(t *testing.T) {
	if got := Class("failed"); got != KindFailed {
		t.Fatalf("producer failure class=%v, want KindFailed", got)
	}
}
