package db

import (
	"context"
	"time"

	"github.com/rvben/shinyhub/internal/auth"
)

// RecordLocalToken fails closed: unaudited host capabilities must not be issued.
func (s *Store) RecordLocalToken(ctx context.Context, id string, expires time.Time) error {
	return s.recordAuditEvent(ctx, AuditEventParams{
		Action: "local_token_issued", ResourceType: "credential", ResourceID: id, IPAddress: "local",
		Detail:         AuditDetail(map[string]any{"uid": 0, "scope": "announcements", "expires_at": expires}),
		CredentialType: "local_token", CredentialName: auth.LocalTokenName(id),
	})
}
