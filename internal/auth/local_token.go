package auth

import (
	"crypto/rand"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const LocalTokenScope = "announcements"
const LocalTokenPrefix = "shl_"
const LocalTokenMaxTTL = 15 * time.Minute

// LocalNoticeMaxWindow bounds the durable effect of a leaked host capability.
const LocalNoticeMaxWindow = 24 * time.Hour
const localTokenPurpose = "host-local-announcements-v1"

var ErrLocalTokenScope = errors.New("local token is restricted to announcement management")

// IssueLocalToken creates a non-renewable capability, never a user session.
// The caller must authenticate the local peer and durably audit issuance
// before returning the token to it.
func IssueLocalToken(secret, scope string, ttl time.Duration) (string, string, time.Time, error) {
	if scope != LocalTokenScope || ttl < time.Second || ttl > LocalTokenMaxTTL {
		return "", "", time.Time{}, errors.New("scope must be announcements and ttl must be 1s..15m")
	}
	if len(secret) < 32 {
		return "", "", time.Time{}, errors.New("local token requires a configured auth secret")
	}
	now := time.Now().UTC().Truncate(time.Second)
	end := now.Add(ttl).Truncate(time.Second)
	if !end.After(now) {
		return "", "", time.Time{}, errors.New("ttl must be at least one second")
	}
	id := rand.Text()
	claims := Claims{Purpose: localTokenPurpose, RegisteredClaims: jwt.RegisteredClaims{
		ID: id, Subject: "local-token(uid=0, scope=announcements)", Issuer: "shinyhub", Audience: jwt.ClaimStrings{localTokenPurpose},
		IssuedAt: jwt.NewNumericDate(now), NotBefore: jwt.NewNumericDate(now), ExpiresAt: jwt.NewNumericDate(end),
	}}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(secret))
	if err != nil {
		return "", "", time.Time{}, err
	}
	return LocalTokenPrefix + token, id, end, nil
}

func authenticateLocalToken(token, secret string, revoked RevocationChecker) (*authResult, error) {
	if !strings.HasPrefix(token, LocalTokenPrefix) {
		return nil, errors.New("invalid local token prefix")
	}
	c, err := validateJWT(strings.TrimPrefix(token, LocalTokenPrefix), secret, revoked, localTokenPurpose, jwt.WithValidMethods([]string{"HS256"}), jwt.WithExpirationRequired(), jwt.WithIssuedAt())
	if err != nil {
		return nil, err
	}
	if c.ID == "" || c.UserID != 0 || c.Role != "" || c.Issuer != "shinyhub" || len(c.Audience) != 1 || c.Audience[0] != localTokenPurpose || c.ExpiresAt == nil || c.IssuedAt == nil || c.NotBefore == nil || c.ExpiresAt.Sub(c.IssuedAt.Time) > LocalTokenMaxTTL || !c.ExpiresAt.After(c.IssuedAt.Time) || c.IssuedAt.After(time.Now()) {
		return nil, errors.New("invalid local capability")
	}
	end := c.ExpiresAt.Time
	return &authResult{User: &ContextUser{Username: "local-token(uid=0, scope=announcements)", LocalAnnouncements: true},
		Credential: &CredentialInfo{Type: "local_token", Name: LocalTokenName(c.ID), ExpiresAt: &end}}, nil
}

// Exact methods and path shapes keep new API endpoints denied by default.
func localAnnouncementRequest(r *http.Request) bool {
	p := r.URL.Path
	if p == "/api/announcements" {
		return r.Method == http.MethodGet || r.Method == http.MethodPost
	}
	if !strings.HasPrefix(p, "/api/announcements/") {
		return false
	}
	id := strings.TrimPrefix(p, "/api/announcements/")
	return id != "" && id != "." && id != ".." && id != "active" && !strings.ContainsAny(id, "/%") && (r.Method == http.MethodGet || r.Method == http.MethodPatch)
}

// IsLocalTokenHeader recognizes the credential family without trusting its claims.
// Authentication must still validate the signature, expiry and capability.
func IsLocalTokenHeader(header string) bool {
	parts := strings.SplitN(header, " ", 2)
	return len(parts) == 2 && strings.HasPrefix(parts[1], LocalTokenPrefix)
}

// LocalTokenName provides non-secret attribution shared by issuance and use.
func LocalTokenName(id string) string {
	return "local-token(uid=0, scope=announcements, id=" + id + ")"
}
