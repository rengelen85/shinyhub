package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/rvben/shinyhub/internal/fleet"
)

// Only declared plaintext values are reconciled. Secrets stay outside fleet.toml;
// drift output redacts both sides, including values accidentally stored as secrets.
func observeFleetEnv(cfg *cliConfig, slug string, declared fleet.Config, observed fleet.ObservedApp) (fleet.ObservedApp, error) {
	if len(declared.Env) == 0 {
		return observed, nil
	}
	observed.Env = map[string]string{}
	for offset := 0; ; offset += 100 {
		req, err := http.NewRequest(http.MethodGet, cfg.Host+"/api/apps/"+url.PathEscape(slug)+fmt.Sprintf("/env?limit=100&offset=%d", offset), nil)
		if err != nil {
			return observed, err
		}
		req.Header.Set("Authorization", authHeader(cfg.Token))
		resp, err := httpClient.Do(req)
		if err != nil {
			return observed, err
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return observed, err
		}
		if resp.StatusCode >= 300 {
			return observed, httpError(cfg.Token, "read fleet env", resp, body)
		}
		var result struct {
			Total int `json:"total"`
			Items []struct {
				Key    string `json:"key"`
				Value  string `json:"value"`
				Secret bool   `json:"secret"`
			} `json:"items"`
		}
		if err := json.Unmarshal(body, &result); err != nil {
			return observed, err
		}
		if result.Items == nil {
			return observed, &protocolError{op: "decode fleet env", err: fmt.Errorf("response has no items array")}
		}
		for _, item := range result.Items {
			if !item.Secret {
				observed.Env[item.Key] = item.Value
			}
		}
		if len(result.Items) < 100 || offset+100 >= result.Total {
			break
		}
	}
	return observed, nil
}
