package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestLocalTokenCommandRequiresExplicitScope(t *testing.T) {
	cmd := newLocalTokenCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{})
	if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "required flag(s) \"scope\" not set") {
		t.Fatal("local-token accepted an implicit scope")
	}
}
