package config

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

const managedEnvironmentKeys = "_SHINYHUB_ENV_FILE_KEYS"

func environmentValues(environment []string) map[string]string {
	values := make(map[string]string, len(environment))
	for _, entry := range environment {
		if key, value, ok := strings.Cut(entry, "="); ok {
			values[key] = value
		}
	}
	return values
}

// ServerEnvironment builds a fresh successor environment without changing the
// caller. Keys from the previous file are removed first, including deletions.
// Internal handoff and systemd variables always come from the service manager.
func ServerEnvironment(configPath string, inherited []string) ([]string, error) {
	values := environmentValues(inherited)
	var document struct {
		Server struct {
			EnvFile string `yaml:"env_file"`
		} `yaml:"server"`
	}
	if configPath != "" {
		data, err := os.ReadFile(configPath)
		if err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("read server environment configuration: %w", err)
		}
		if err == nil {
			if err := yaml.Unmarshal(data, &document); err != nil {
				return nil, fmt.Errorf("parse server environment configuration: %w", err)
			}
		}
	}
	for _, key := range strings.Split(values[managedEnvironmentKeys], ",") {
		if allowedServerEnvironmentKey(key) {
			delete(values, key)
		}
	}
	delete(values, managedEnvironmentKeys)
	var keys []string
	if path := document.Server.EnvFile; path != "" {
		file, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("server.env_file: %w", err)
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil {
			return nil, fmt.Errorf("server.env_file: %w", err)
		}
		if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
			return nil, fmt.Errorf("server.env_file must be a regular owner-only file (0600)")
		}
		scanner := bufio.NewScanner(file)
		scanner.Buffer(make([]byte, 4096), 256*1024)
		for line := 1; scanner.Scan(); line++ {
			entry := strings.TrimSpace(scanner.Text())
			if entry == "" || strings.HasPrefix(entry, "#") {
				continue
			}
			key, value, ok := strings.Cut(entry, "=")
			key, value = strings.TrimSpace(key), strings.TrimSpace(value)
			if !ok || !allowedServerEnvironmentKey(key) {
				return nil, fmt.Errorf("server.env_file line %d: expected a supported KEY=VALUE", line)
			}
			if value != "" && (value[0] == '\'' || value[0] == '"') {
				if value[0] == '\'' {
					if len(value) < 2 || value[len(value)-1] != '\'' {
						return nil, fmt.Errorf("server.env_file line %d: unterminated quote", line)
					}
					value = value[1 : len(value)-1]
				} else {
					decoded, err := strconv.Unquote(value)
					if err != nil {
						return nil, fmt.Errorf("server.env_file line %d: invalid quoted value", line)
					}
					value = decoded
				}
			}
			if strings.ContainsRune(value, 0) {
				return nil, fmt.Errorf("server.env_file line %d: NUL is not allowed", line)
			}
			for _, previous := range keys {
				if previous == key {
					return nil, fmt.Errorf("server.env_file line %d: duplicate key %s", line, key)
				}
			}
			values[key] = value
			keys = append(keys, key)
		}
		if err := scanner.Err(); err != nil {
			return nil, fmt.Errorf("server.env_file: cannot read file")
		}
	}
	sort.Strings(keys)
	if len(keys) > 0 {
		values[managedEnvironmentKeys] = strings.Join(keys, ",")
	}
	result := make([]string, 0, len(values))
	for key, value := range values {
		result = append(result, key+"="+value)
	}
	sort.Strings(result)
	return result, nil
}

func allowedServerEnvironmentKey(key string) bool {
	if key == "SHINYHUB_CONFIG" || key == "SHINYHUB_PID_FILE" {
		return false
	}
	if strings.HasPrefix(key, "SHINYHUB_") {
		return knownEnvSet[key]
	}
	switch key {
	case "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "all_proxy", "no_proxy":
		return true
	}
	if strings.HasPrefix(key, "UV_") {
		for _, c := range key {
			if c != '_' && (c < 'A' || c > 'Z') && (c < '0' || c > '9') {
				return false
			}
		}
		return true
	}
	return false
}

// ApplyServerEnvironment applies only the file-owned keys after complete
// validation. Service-manager and unrelated process variables are untouched.
func ApplyServerEnvironment(environment []string) {
	values := environmentValues(environment)
	oldKeys := strings.Split(os.Getenv(managedEnvironmentKeys), ",")
	newKeys := strings.Split(values[managedEnvironmentKeys], ",")
	for _, key := range append(oldKeys, newKeys...) {
		if !allowedServerEnvironmentKey(key) {
			continue
		}
		if value, ok := values[key]; ok {
			_ = os.Setenv(key, value)
		} else {
			_ = os.Unsetenv(key)
		}
	}
	if keys := values[managedEnvironmentKeys]; keys != "" {
		_ = os.Setenv(managedEnvironmentKeys, keys)
	} else {
		_ = os.Unsetenv(managedEnvironmentKeys)
	}
}
