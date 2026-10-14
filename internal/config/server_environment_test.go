package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestServerEnvironmentReloadDeletesKeysAndPreservesHandoff(t *testing.T) {
	dir := t.TempDir()
	envPath := filepath.Join(dir, "server.env")
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("server:\n  env_file: "+envPath+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	write := func(value string) {
		t.Helper()
		if err := os.WriteFile(envPath, []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("HTTPS_PROXY='http://old'\nUV_DEFAULT_INDEX=https://index\nSHINYHUB_DEPLOY_TOKEN=private\n")
	first, err := ServerEnvironment(path, []string{"NOTIFY_SOCKET=/notify", "TABLEFLIP_HAS_PARENT_7DIU3=yes", "PATH=/bin"})
	if err != nil {
		t.Fatal(err)
	}
	write("HTTPS_PROXY=http://new\n")
	second, err := ServerEnvironment(path, first)
	if err != nil {
		t.Fatal(err)
	}
	got := environmentValues(second)
	if got["HTTPS_PROXY"] != "http://new" || got["UV_DEFAULT_INDEX"] != "" || got["SHINYHUB_DEPLOY_TOKEN"] != "" || got["NOTIFY_SOCKET"] != "/notify" || got["TABLEFLIP_HAS_PARENT_7DIU3"] != "yes" {
		t.Fatal("environment replacement did not preserve only current settings and handoff variables")
	}
	write("NOTIFY_SOCKET=private\n")
	if _, err := ServerEnvironment(path, first); err == nil || strings.Contains(err.Error(), "private") {
		t.Fatal("reserved key accepted or value disclosed")
	}
	write("HTTPS_PROXY=private\nHTTPS_PROXY=other\n")
	if _, err := ServerEnvironment(path, first); err == nil || strings.Contains(err.Error(), "private") {
		t.Fatal("duplicate accepted or value disclosed")
	}
	if err := os.Chmod(envPath, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := ServerEnvironment(path, first); err == nil {
		t.Fatal("public environment file accepted")
	}
}

func TestLoadServerEnvironmentWithoutChangingCaller(t *testing.T) {
	dir := t.TempDir()
	envPath := filepath.Join(dir, "server.env")
	path := filepath.Join(dir, "config.yaml")
	t.Setenv("SHINYHUB_AUTH_SECRET", "")
	if err := os.WriteFile(envPath, []byte("SHINYHUB_AUTH_SECRET=abcdefghijklmnopqrstuvwxyz012345\nSHINYHUB_SERVER_PORT=9123\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("server:\n  env_file: "+envPath+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Port != 9123 || cfg.Auth.Secret == "" || os.Getenv("SHINYHUB_AUTH_SECRET") != "" {
		t.Fatal("file was not applied independently of caller environment")
	}
}
