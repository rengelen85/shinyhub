package appenv

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestBundleOverlayPreservesStoredEnvironment(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "shinyhub.toml"), []byte("[app.env]\nMODE='candidate'\nEMPTY=''\nTOKEN='explicit'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	env := []string{"MODE=stored", "KEEP=unchanged"}
	secrets := []string{"TOKEN=secret", "OTHER=secret"}
	got, secret, err := WithBundle(env, secrets, dir)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []string{"KEEP=unchanged", "EMPTY=", "MODE=candidate", "TOKEN=explicit"}) || !reflect.DeepEqual(secret, []string{"OTHER=secret"}) {
		t.Fatalf("overlay=%v secrets=%v", got, secret)
	}
	if !reflect.DeepEqual(env, []string{"MODE=stored", "KEEP=unchanged"}) || !reflect.DeepEqual(secrets, []string{"TOKEN=secret", "OTHER=secret"}) {
		t.Fatal("mutated stored environment")
	}
}

func TestBundleEnvValidationFailsClosed(t *testing.T) {
	for _, key := range []string{"PORT", "SHINYHUB_AUTH_SECRET", "bad-key"} {
		if err := ValidateBundle(map[string]string{key: "private-value"}); err == nil || strings.Contains(err.Error(), "private-value") {
			t.Fatalf("key %s: %v", key, err)
		}
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "shinyhub.toml"), []byte("[app.env]\nMODE=42\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := WithBundle([]string{"MODE=stored"}, nil, dir); err == nil {
		t.Fatal("invalid bundle silently fell back to stored environment")
	}
}
