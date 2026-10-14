package api_test

import (
	"strings"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/api"
	"github.com/rvben/shinyhub/internal/auth"
)

func TestLocalTokenAnnouncementLifecycle(t *testing.T) {
	s, store := newTestServer(t)
	s.Config().Auth.Secret = "local-api-capability-secret-32-bytes"
	s = api.New(s.Config(), store, nil, nil)
	token, id, _, err := auth.IssueLocalToken(s.Config().Auth.Secret, "announcements", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	body := map[string]any{"title": "Release", "message": "Brief interruptions", "publication": "published", "ends_at": time.Now().Add(time.Hour)}
	w := announcementRequest(t, s, "POST", "/api/announcements", token, body)
	if w.Code != 201 {
		t.Fatalf("publish %d: %s", w.Code, w.Body)
	}
	a := decodeAnnouncement(t, w)
	if a.CreatedBy != nil || a.UpdatedBy != nil {
		t.Fatal("invented human attribution")
	}
	for _, path := range []string{"/api/announcements", "/api/announcements/" + a.ID} {
		if w := announcementRequest(t, s, "GET", path, token, nil); w.Code != 200 {
			t.Fatalf("get %s: %d %s", path, w.Code, w.Body)
		}
	}
	w = announcementRequest(t, s, "PATCH", "/api/announcements/"+a.ID, token, map[string]any{"expected_revision": a.Revision, "ends_at": nil})
	if w.Code != 400 {
		t.Fatalf("removed expiry: %d %s", w.Code, w.Body)
	}
	w = announcementRequest(t, s, "PATCH", "/api/announcements/"+a.ID, token, map[string]any{"expected_revision": a.Revision + 1, "publication": "disabled"})
	if w.Code != 409 {
		t.Fatalf("stale revision %d", w.Code)
	}
	w = announcementRequest(t, s, "PATCH", "/api/announcements/"+a.ID, token, map[string]any{"expected_revision": a.Revision, "publication": "disabled"})
	if w.Code != 200 {
		t.Fatalf("disable %d: %s", w.Code, w.Body)
	}
	events, err := store.ListAuditEvents("", 20, 0)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, e := range events {
		if e.ResourceID == a.ID {
			count++
			if e.UserID != nil || e.CredentialType != "local_token" || !strings.Contains(e.CredentialName, id) {
				t.Fatalf("bad attribution: %+v", e)
			}
		}
	}
	if count != 2 {
		t.Fatalf("expected two mutation audits, got %d", count)
	}
	for _, tc := range []struct{ method, path string }{
		{"GET", "/api/users"}, {"POST", "/api/tokens"}, {"GET", "/api/apps"}, {"POST", "/api/auth/refresh"}, {"POST", "/api/auth/logout"},
	} {
		w := announcementRequest(t, s, tc.method, tc.path, token, nil)
		if w.Code >= 200 && w.Code < 300 {
			t.Fatalf("capability escaped: %s %s", tc.method, tc.path)
		}
	}
	delete(body, "ends_at")
	if w := announcementRequest(t, s, "POST", "/api/announcements", token, body); w.Code != 400 {
		t.Fatalf("unbounded notice %d %s", w.Code, w.Body)
	}
}

func TestLocalTokenBoundsPublishedNoticeWindow(t *testing.T) {
	s, store := newTestServer(t)
	s.Config().Auth.Secret = "bounded-notice-test-secret-32-bytes"
	s = api.New(s.Config(), store, nil, nil)
	token, _, _, err := auth.IssueLocalToken(s.Config().Auth.Secret, "announcements", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	body := map[string]any{"title": "Release", "message": "Brief interruptions", "publication": "published", "ends_at": time.Now().Add(time.Hour)}
	w := announcementRequest(t, s, "POST", "/api/announcements", token, body)
	if w.Code != 201 {
		t.Fatal(w.Body)
	}
	a := decodeAnnouncement(t, w)
	for _, end := range []time.Time{time.Now().Add(25 * time.Hour), time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)} {
		body["ends_at"] = end
		if w := announcementRequest(t, s, "POST", "/api/announcements", token, body); w.Code != 400 {
			t.Fatalf("unbounded POST %d %s", w.Code, w.Body)
		}
		if w := announcementRequest(t, s, "PATCH", "/api/announcements/"+a.ID, token, map[string]any{"expected_revision": a.Revision, "ends_at": end}); w.Code != 400 {
			t.Fatalf("unbounded PATCH %d %s", w.Code, w.Body)
		}
	}
	body["starts_at"] = time.Now().Add(30 * 24 * time.Hour)
	body["ends_at"] = time.Now().Add(31 * 24 * time.Hour)
	if w := announcementRequest(t, s, "POST", "/api/announcements", token, body); w.Code != 400 {
		t.Fatalf("unbounded schedule %d %s", w.Code, w.Body)
	}
	_, adminID := seedUserAndJWT(t, store, "long-notice-admin", "admin")
	admin, err := auth.IssueJWT(adminID, "long-notice-admin", "admin", s.Config().Auth.Secret)
	if err != nil {
		t.Fatal(err)
	}
	delete(body, "starts_at")
	body["ends_at"] = time.Now().Add(48 * time.Hour)
	w = announcementRequest(t, s, "POST", "/api/announcements", admin, body)
	if w.Code != 201 {
		t.Fatalf("changed admin behavior %d %s", w.Code, w.Body)
	}
	long := decodeAnnouncement(t, w)
	if w := announcementRequest(t, s, "PATCH", "/api/announcements/"+long.ID, token, map[string]any{"expected_revision": long.Revision, "title": "Changed title"}); w.Code != 400 {
		t.Fatalf("retained unbounded published window %d %s", w.Code, w.Body)
	}
	if w := announcementRequest(t, s, "PATCH", "/api/announcements/"+long.ID, token, map[string]any{"expected_revision": long.Revision, "publication": "disabled"}); w.Code != 200 {
		t.Fatalf("cannot withdraw long notice %d %s", w.Code, w.Body)
	}
	body["ends_at"] = time.Now().Add(23 * time.Hour)
	if w := announcementRequest(t, s, "POST", "/api/announcements", token, body); w.Code != 201 {
		t.Fatalf("valid window rejected %d %s", w.Code, w.Body)
	}
}
