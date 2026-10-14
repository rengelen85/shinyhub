package db_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/dbtest"
)

func TestRecordLocalTokenRequiresDurableAudit(t *testing.T) {
	s := dbtest.New(t)
	if err := s.RecordLocalToken(context.Background(), "unique-id", time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	events, err := s.ListAuditEvents("local_token_issued", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].UserID != nil || events[0].ResourceID != "unique-id" || !strings.Contains(events[0].CredentialName, "unique-id") {
		t.Fatalf("bad audit: %+v", events)
	}
	s.Close()
	if err := s.RecordLocalToken(context.Background(), "must-not-issue", time.Now()); err == nil {
		t.Fatal("issuance ignored audit failure")
	}
}

func TestRecordLocalTokenCancelledIssuanceHasNoAudit(t *testing.T) {
	s := dbtest.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.RecordLocalToken(ctx, "cancelled", time.Now()); err == nil {
		t.Fatal("ignored cancelled issuance")
	}
	events, err := s.ListAuditEvents("local_token_issued", 10, 0)
	if err != nil || len(events) != 0 {
		t.Fatalf("cancelled credential recorded: %v %v", events, err)
	}
}
