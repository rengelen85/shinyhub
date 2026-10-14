package auth

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const localTestSecret = "local-capability-test-secret-32-bytes"

func TestLocalTokenBoundary(t *testing.T) {
	token, id, end, err := IssueLocalToken(localTestSecret, LocalTokenScope, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if id == "" || time.Until(end) > 10*time.Minute {
		t.Fatal("invalid identity or expiry")
	}
	if _, err := ValidateJWT(strings.TrimPrefix(token, LocalTokenPrefix), localTestSecret, nil); err == nil {
		t.Fatal("accepted as normal session")
	}
	for _, tc := range []struct {
		method, path string
		want         int
	}{
		{"GET", "/api/announcements", 200}, {"POST", "/api/announcements", 200},
		{"GET", "/api/announcements/one", 200}, {"PATCH", "/api/announcements/one", 200},
		{"POST", "/api/tokens", 403}, {"GET", "/api/users", 403}, {"GET", "/api/apps", 403},
		{"POST", "/api/auth/logout", 403}, {"GET", "/api/auth/me", 403}, {"POST", "/api/auth/refresh", 403},
		{"DELETE", "/api/announcements/one", 403}, {"POST", "/api/announcements/one", 403},
		{"PATCH", "/api/announcements/one/extra", 403}, {"GET", "/api/announcements/../users", 403},
		{"POST", "/api/announcements/active", 403}, {"GET", "/app/sales/", 403},
	} {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			req.Header.Set("Authorization", "Token "+token)
			w := httptest.NewRecorder()
			BearerMiddleware(localTestSecret, nil, func(int64) (*ContextUser, error) { t.Fatal("local token looked up a user"); return nil, nil }, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				u := UserFromContext(r.Context())
				if !u.LocalAnnouncements || u.Role != "" || u.ID != 0 {
					t.Fatal("local token has user privilege")
				}
				if TokenInfoFromContext(r.Context()) != nil {
					t.Fatal("local token is renewable")
				}
				c := CredentialInfoFromContext(r.Context())
				if c.Type != "local_token" || !strings.Contains(c.Name, id) {
					t.Fatal("missing credential attribution")
				}
			})).ServeHTTP(w, req)
			if w.Code != tc.want {
				t.Fatalf("status %d, want %d: %s", w.Code, tc.want, w.Body)
			}
		})
	}
	req := httptest.NewRequest("GET", "/api/announcements", nil)
	req.AddCookie(&http.Cookie{Name: SessionCookieName, Value: strings.TrimPrefix(token, LocalTokenPrefix)})
	if _, _, err := AuthenticateBrowserSession(req, localTestSecret, nil, nil); err == nil {
		t.Fatal("accepted as browser cookie")
	}
	if _, err := authenticateLocalToken(token, "different-secret", nil); err == nil {
		t.Fatal("accepted wrong secret")
	}
	if _, err := authenticateLocalToken(token, localTestSecret, func(got string) (bool, error) {
		if got != id {
			t.Fatal("wrong revocation identity")
		}
		return true, nil
	}); !errors.Is(err, ErrTokenRevoked) {
		t.Fatalf("revocation: %v", err)
	}
}

func TestLocalTokenRejectsInvalidLifetimeAndScope(t *testing.T) {
	for _, tc := range []struct {
		scope string
		ttl   time.Duration
	}{
		{"admin", time.Minute}, {"apps:sales", time.Minute}, {"announcements", 0}, {"announcements", -time.Second}, {"announcements", time.Millisecond}, {"announcements", 16 * time.Minute},
	} {
		if _, _, _, err := IssueLocalToken(localTestSecret, tc.scope, tc.ttl); err == nil {
			t.Fatalf("accepted %+v", tc)
		}
	}
	token, _, _, _ := IssueLocalToken(localTestSecret, LocalTokenScope, time.Minute)
	var base Claims
	if _, _, err := jwt.NewParser().ParseUnverified(strings.TrimPrefix(token, LocalTokenPrefix), &base); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*Claims)
	}{
		{"session purpose", func(c *Claims) { c.Purpose = "" }},
		{"other purpose", func(c *Claims) { c.Purpose = "support" }},
		{"missing expiry", func(c *Claims) { c.ExpiresAt = nil }},
		{"expired", func(c *Claims) { c.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-time.Minute)) }},
		{"long lifetime", func(c *Claims) { c.ExpiresAt = jwt.NewNumericDate(time.Now().Add(time.Hour)) }},
		{"role", func(c *Claims) { c.Role = "admin" }},
		{"user", func(c *Claims) { c.UserID = 1 }},
		{"audience", func(c *Claims) { c.Audience = jwt.ClaimStrings{"another-instance"} }},
		{"no id", func(c *Claims) { c.ID = "" }},
		{"future issuance", func(c *Claims) { c.IssuedAt = jwt.NewNumericDate(time.Now().Add(time.Minute)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := base
			tc.mutate(&c)
			raw, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, c).SignedString([]byte(localTestSecret))
			if _, err := authenticateLocalToken(LocalTokenPrefix+raw, localTestSecret, nil); err == nil {
				t.Fatal("accepted malformed capability")
			}
		})
	}
}

func TestLocalTokenDoesNotInheritUpstreamAdmin(t *testing.T) {
	token, _, _, err := IssueLocalToken(localTestSecret, LocalTokenScope, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/api/announcements", "/api/users"} {
		req := httptest.NewRequest("GET", path, nil)
		req.Header.Set("Authorization", "Token "+token)
		req = req.WithContext(WithUser(req.Context(), &ContextUser{ID: 1, Role: "admin"}))
		w := httptest.NewRecorder()
		BearerMiddleware(localTestSecret, nil, nil, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !UserFromContext(r.Context()).LocalAnnouncements {
				t.Fatal("inherited admin identity")
			}
		})).ServeHTTP(w, req)
		want := 200
		if path == "/api/users" {
			want = 403
		}
		if w.Code != want {
			t.Fatalf("status %d: %s", w.Code, w.Body)
		}
	}
}

func TestLocalTokenPurposeCannotBeRepackagedAsSession(t *testing.T) {
	token, _, _, err := IssueLocalToken(localTestSecret, LocalTokenScope, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	raw := strings.TrimPrefix(token, LocalTokenPrefix)
	req := httptest.NewRequest("GET", "/api/announcements", nil)
	req.Header.Set("Authorization", "Bearer "+raw)
	if _, _, err := AuthenticateRequest(req, localTestSecret, nil, nil, nil); err == nil {
		t.Fatal("accepted local-purpose JWT under session scheme")
	}
	now := time.Now()
	session := Claims{UserID: 1, Role: "admin", RegisteredClaims: jwt.RegisteredClaims{ID: "session", Subject: "admin", IssuedAt: jwt.NewNumericDate(now), ExpiresAt: jwt.NewNumericDate(now.Add(time.Minute))}}
	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, session).SignedString([]byte(localTestSecret))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authenticateLocalToken(LocalTokenPrefix+signed, localTestSecret, nil); err == nil {
		t.Fatal("accepted session JWT wrapped as local capability")
	}
}
