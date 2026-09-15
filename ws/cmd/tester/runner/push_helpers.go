package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// pushSubscribeRequest is the JSON body for POST /api/v1/push/subscribe.
type pushSubscribeRequest struct {
	Platform   string   `json:"platform"`
	Token      string   `json:"token,omitempty"`
	Endpoint   string   `json:"endpoint,omitempty"`
	P256dhKey  string   `json:"p256dh_key,omitempty"`
	AuthSecret string   `json:"auth_secret,omitempty"`
	Channels   []string `json:"channels"`
}

// pushUnsubscribeRequest is the JSON body for DELETE /api/v1/push/subscribe.
type pushUnsubscribeRequest struct {
	DeviceID int64 `json:"device_id"`
}

// pushSubscribe registers a push device via the gateway.
// Returns (deviceID, httpStatusCode, error).
func pushSubscribe(ctx context.Context, gatewayURL, token string, req pushSubscribeRequest) (int64, error) {
	body, err := json.Marshal(req) //nolint:gosec // G117: AuthSecret is a Web Push shared key field name, not a real secret
	if err != nil {
		return 0, fmt.Errorf("push subscribe: marshal: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, gatewayURL+"/api/v1/push/subscribe", bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("push subscribe: create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return 0, fmt.Errorf("push subscribe: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	if resp.StatusCode != http.StatusCreated {
		return 0, fmt.Errorf("push subscribe: HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		DeviceID int64 `json:"device_id"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return 0, fmt.Errorf("push subscribe: unmarshal response: %w", err)
	}

	return result.DeviceID, nil
}

// pushUnsubscribe unregisters a push device via the gateway.
// Returns (httpStatusCode, error).
func pushUnsubscribe(ctx context.Context, gatewayURL, token string, deviceID int64) error {
	body, _ := json.Marshal(pushUnsubscribeRequest{DeviceID: deviceID}) // json.Marshal on simple struct cannot fail

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, gatewayURL+"/api/v1/push/subscribe", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("push unsubscribe: create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("push unsubscribe: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return fmt.Errorf("push unsubscribe: HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// pushGetVAPIDKey retrieves the VAPID public key via the gateway.
// Returns (statusCode, error). The public key value is not used by callers
// (they only need the status code for gate/availability checks).
func pushGetVAPIDKey(ctx context.Context, gatewayURL, token string) (statusCode int, err error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, gatewayURL+"/api/v1/push/vapid-key", http.NoBody)
	if err != nil {
		return 0, fmt.Errorf("push vapid key: create request: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return 0, fmt.Errorf("push vapid key: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, fmt.Errorf("push vapid key: HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	return resp.StatusCode, nil
}

// pushServiceAvailable reports whether a push surface exists behind the
// gateway — a quick VAPID-key probe. 200 (or an application status like the
// edition 403) means the surface answered; 404 means the gateway runs with
// GATEWAY_PUSH_ENABLED=false (routes not registered); 502/503/504 or a
// transport error means the deploy-optional push service is not running.
// Used to discriminate "not deployed/disabled" skips from real failures.
func pushServiceAvailable(ctx context.Context, gatewayURL, token string) bool {
	statusCode, err := pushGetVAPIDKey(ctx, gatewayURL, token)
	if err == nil {
		return true
	}
	switch statusCode {
	case 0, http.StatusNotFound, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return false
	default:
		// The push surface answered (edition 403 etc.) — it exists.
		return true
	}
}
