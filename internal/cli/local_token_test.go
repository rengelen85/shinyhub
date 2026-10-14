package cli

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/auth"
)

func TestAuthHeaderLocalCapabilityUsesTokenScheme(t *testing.T) {
	token, _, _, err := auth.IssueLocalToken("cli-host-capability-test-secret-32-bytes", "announcements", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got := authHeader(token); got != "Token "+token {
		t.Fatal("CLI chose wrong scheme for host capability")
	}
}

func TestRejectedLocalCredentialSuggestsFreshHostToken(t *testing.T) {
	err := httpError("shl_sensitive", "manage announcement", &http.Response{StatusCode: 401, Status: "401 Unauthorized"}, []byte("unauthorized"))
	if !strings.Contains(err.Error(), "shinyhub local-token --scope announcements") || strings.Contains(err.Error(), "shl_sensitive") {
		t.Fatalf("unsafe or unhelpful recovery: %v", err)
	}
}
