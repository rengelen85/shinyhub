package api_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"image"
	"image/color"
	"image/png"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rvben/shinyhub/internal/auth"
	"github.com/rvben/shinyhub/internal/db"
)

func avatarFixture(t *testing.T, width int) string {
	t.Helper()
	img := image.NewNRGBA(image.Rect(0, 0, width, 2))
	img.Set(0, 0, color.NRGBA{R: 80, G: 140, B: 200, A: 255})
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatal(err)
	}
	return "data:image/png;base64," + base64.StdEncoding.EncodeToString(buf.Bytes())
}

func TestPatchMeAvatar(t *testing.T) {
	srv, store := newTestServer(t)
	token, id := seedUserAndJWT(t, store, "photo-user", "viewer")
	photo := avatarFixture(t, 2)
	rec := patchMe(t, srv, token, map[string]any{"avatar": photo})
	if rec.Code != 200 {
		t.Fatalf("upload: %d %s", rec.Code, rec.Body.String())
	}
	var response struct {
		User struct {
			AvatarURL string `json:"avatar_url"`
		} `json:"user"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(response.User.AvatarURL, "/api/auth/me/avatar?v=") {
		t.Fatalf("missing avatar URL: %+v", response)
	}
	if got := getMe(t, srv, token).User.AvatarURL; got != response.User.AvatarURL {
		t.Fatalf("reloaded avatar URL = %q, want %q", got, response.User.AvatarURL)
	}
	user, _ := store.GetUserByID(id)
	if user.AvatarVersion == "" {
		t.Fatal("avatar was not persisted")
	}
	get := func(token string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", response.User.AvatarURL, nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		rec := httptest.NewRecorder()
		srv.Router().ServeHTTP(rec, req)
		return rec
	}
	rec = get(token)
	if rec.Code != 200 || rec.Header().Get("Content-Type") != "image/png" || rec.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatalf("serve: %d %v", rec.Code, rec.Header())
	}
	if _, err := png.Decode(bytes.NewReader(rec.Body.Bytes())); err != nil {
		t.Fatal(err)
	}
	if get("").Code != http.StatusUnauthorized {
		t.Fatal("avatar must require authentication")
	}
	other, _ := seedUserAndJWT(t, store, "other-photo-user", "viewer")
	if get(other).Code != http.StatusNotFound {
		t.Fatal("avatar endpoint must serve only the caller's photo")
	}
	// Omitting the field must preserve a photo.
	if patchMe(t, srv, token, map[string]any{"display_name": "Photo Person"}).Code != 200 {
		t.Fatal("name update")
	}
	if get(token).Code != 200 {
		t.Fatal("name edit removed avatar")
	}
	if patchMe(t, srv, token, map[string]any{"avatar": ""}).Code != 200 {
		t.Fatal("remove failed")
	}
	if get(token).Code != http.StatusNotFound {
		t.Fatal("removed avatar still served")
	}
}

func TestPatchMeAvatarValidation(t *testing.T) {
	for name, value := range map[string]string{
		"url":        "https://example.com/photo.png",
		"svg":        "data:image/svg+xml;base64,PHN2Zz4=",
		"corrupt":    "data:image/png;base64,aGVsbG8=",
		"oversized":  "data:image/png;base64," + strings.Repeat("A", 700000),
		"dimensions": avatarFixture(t, 513),
	} {
		t.Run(name, func(t *testing.T) {
			srv, store := newTestServer(t)
			token, id := seedUserAndJWT(t, store, "invalid-photo", "viewer")
			rec := patchMe(t, srv, token, map[string]any{"avatar": value, "display_name": "Must not persist"})
			if rec.Code != 400 {
				t.Fatalf("want 400, got %d: %s", rec.Code, rec.Body.String())
			}
			user, _ := store.GetUserByID(id)
			if user.AvatarVersion != "" || user.DisplayName != "" {
				t.Fatal("invalid request mutated profile")
			}
		})
	}
}

func TestPatchMeAvatarAccountPermissions(t *testing.T) {
	for _, kind := range []string{"sso", "forward", "managed", "service"} {
		t.Run(kind, func(t *testing.T) {
			srv, store := newTestServer(t)
			hash := ""
			if kind == "forward" {
				hash = "!disabled"
			}
			if err := store.CreateUser(db.CreateUserParams{Username: "avatar-" + kind, PasswordHash: hash, Role: "viewer"}); err != nil {
				t.Fatal(err)
			}
			user, _ := store.GetUserByUsername("avatar-" + kind)
			if kind == "managed" {
				_, _ = store.DB().Exec(`UPDATE users SET managed_by = 'public-demo' WHERE id = ?`, user.ID)
			}
			if kind == "service" {
				_, _ = store.DB().Exec(`UPDATE users SET principal_type = 'service_account' WHERE id = ?`, user.ID)
			}
			token, err := auth.IssueJWT(user.ID, user.Username, user.Role, "test-secret")
			if err != nil {
				t.Fatal(err)
			}
			rec := patchMe(t, srv, token, map[string]any{"avatar": avatarFixture(t, 2), "avatar_color": "#123456"})
			if kind == "managed" || kind == "service" {
				if rec.Code != 403 && rec.Code != 401 {
					t.Fatalf("restricted account: %d %s", rec.Code, rec.Body.String())
				}
			} else if rec.Code != 200 {
				t.Fatalf("SSO photo: %d %s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestPatchMeAvatarRejectedPasswordDoesNotMutateProfile(t *testing.T) {
	srv, store := newTestServer(t)
	token, id := seedUserAndJWT(t, store, "photo-password", "viewer")
	rec := patchMe(t, srv, token, map[string]any{
		"avatar": avatarFixture(t, 2), "display_name": "Should not change",
		"current_password": "incorrect", "new_password": "a-long-new-password",
	})
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("got %d: %s", rec.Code, rec.Body.String())
	}
	user, _ := store.GetUserByID(id)
	if user.AvatarVersion != "" || user.DisplayName != "" {
		t.Fatal("rejected password request changed profile")
	}
}

func TestPatchMeAvatarColor(t *testing.T) {
	srv, store := newTestServer(t)
	token, id := seedUserAndJWT(t, store, "avatar-color", "viewer")
	rec := patchMe(t, srv, token, map[string]any{"avatar_color": "#AABBCC"})
	if rec.Code != 200 {
		t.Fatalf("set color: %d %s", rec.Code, rec.Body.String())
	}
	if got := getMe(t, srv, token).User.AvatarColor; got != "#aabbcc" {
		t.Fatalf("session color = %q", got)
	}
	user, _ := store.GetUserByID(id)
	if user.AvatarColor != "#aabbcc" {
		t.Fatal("color not persisted")
	}
	for _, body := range []map[string]any{
		{"avatar": avatarFixture(t, 2)}, {"display_name": "Color Person"}, {"avatar": ""},
	} {
		if rec := patchMe(t, srv, token, body); rec.Code != 200 {
			t.Fatalf("update: %d %s", rec.Code, rec.Body.String())
		}
		if got := getMe(t, srv, token).User.AvatarColor; got != "#aabbcc" {
			t.Fatalf("unrelated update changed color: %q", got)
		}
	}
	for _, color := range []any{"red", "#fff", "#ffffff00", "#gggggg", "#-12345", " #123456", "url(x)", 20} {
		srv, store := newTestServer(t)
		token, id := seedUserAndJWT(t, store, "invalid-avatar-color", "viewer")
		rec := patchMe(t, srv, token, map[string]any{"avatar_color": color, "display_name": "Do not change"})
		if rec.Code != 400 {
			t.Fatalf("invalid color %v: %d", color, rec.Code)
		}
		user, _ := store.GetUserByID(id)
		if user.AvatarColor != "" || user.DisplayName != "" {
			t.Fatal("invalid color mutated profile")
		}
	}
	if rec := patchMe(t, srv, token, map[string]any{"avatar_color": ""}); rec.Code != 200 {
		t.Fatalf("reset: %d", rec.Code)
	}
	if got := getMe(t, srv, token).User.AvatarColor; got != "" {
		t.Fatalf("reset color = %q", got)
	}
}
