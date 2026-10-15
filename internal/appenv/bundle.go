package appenv

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/BurntSushi/toml"
)

var bundleEnvKey = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)

// ValidateBundle checks the plaintext, versioned app.env declarations. Platform
// identity and launch variables are reserved; values never appear in errors.
func ValidateBundle(values map[string]string) error {
	if len(values) > 100 {
		return fmt.Errorf("env exceeds 100 keys")
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if !bundleEnvKey.MatchString(key) || strings.HasPrefix(key, "SHINYHUB_") || key == "PORT" {
			return fmt.Errorf("invalid or reserved env key %q", key)
		}
		if len(values[key]) > 64*1024 {
			return fmt.Errorf("env value for %q exceeds 64 KiB", key)
		}
		if strings.ContainsRune(values[key], 0) {
			return fmt.Errorf("env value for %q contains NUL", key)
		}
	}
	return nil
}

// LoadBundle reads only app.env, so process startup does not depend on the
// deploy engine. Missing manifests remain optional; unreadable or invalid env
// fails closed on every activation, including wake, rollback and scheduled runs.
func LoadBundle(dir string) (map[string]string, error) {
	if dir == "" {
		return nil, nil
	}
	data, err := os.ReadFile(filepath.Join(dir, "shinyhub.toml"))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read bundle env: %w", err)
	}
	var manifest struct {
		App struct {
			Env map[string]string `toml:"env"`
		} `toml:"app"`
	}
	meta, err := toml.Decode(string(data), &manifest)
	if err != nil {
		return nil, fmt.Errorf("parse bundle env: %w", err)
	}
	if meta.IsDefined("app", "env") && meta.Type("app", "env") != "Hash" {
		return nil, fmt.Errorf("app.env must be a table of strings")
	}
	if err := ValidateBundle(manifest.App.Env); err != nil {
		return nil, err
	}
	return manifest.App.Env, nil
}

// WithBundle overlays plaintext bundle values without changing or aliasing the
// stored environment. Shadowed secrets are removed from the secret channel so
// runtimes cannot accidentally give them precedence over the explicit bundle.
func WithBundle(env, secretEnv []string, dir string) ([]string, []string, error) {
	values, err := LoadBundle(dir)
	if err != nil {
		return nil, nil, err
	}
	if len(values) == 0 {
		return env, secretEnv, nil
	}
	filter := func(in []string) []string {
		out := make([]string, 0, len(in))
		for _, entry := range in {
			key, _, _ := strings.Cut(entry, "=")
			if _, overridden := values[key]; !overridden {
				out = append(out, entry)
			}
		}
		return out
	}
	env, secretEnv = filter(env), filter(secretEnv)
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		env = append(env, key+"="+values[key])
	}
	return env, secretEnv, nil
}
