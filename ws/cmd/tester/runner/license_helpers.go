package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/auth"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
	"github.com/sukko-dev/sukko/cmd/tester/restpublish"
	testersse "github.com/sukko-dev/sukko/cmd/tester/sse"
	testerws "github.com/sukko-dev/sukko/cmd/tester/ws"
	"github.com/sukko-dev/sukko/internal/shared/license"
)

// licenseHTTPTimeout is the HTTP client timeout for license API calls.
const licenseHTTPTimeout = 10 * time.Second

// propagationPollInterval is the polling interval for edition propagation checks.
const propagationPollInterval = 500 * time.Millisecond

// propagationTimeout is the maximum wait for edition to propagate.
const propagationTimeout = 5 * time.Second

// messageDeliveryTimeout is the maximum wait for a message to be delivered via WebSocket.
const messageDeliveryTimeout = 3 * time.Second

// --- HTTP helpers ---

// postLicense sends a license key to provisioning via POST /api/v1/license.
// Admin JWT auth required — the signer adds the Authorization header.
func postLicense(ctx context.Context, provURL, key string, signer auth.Provider) (int, error) {
	body, _ := json.Marshal(map[string]string{"key": key}) // json.Marshal on literal map of strings cannot fail
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, provURL+"/api/v1/license", bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("post license: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	signer.SignRequest(req)

	client := &http.Client{Timeout: licenseHTTPTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("post license: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // drain body

	return resp.StatusCode, nil
}

// fetchCurrentEdition queries GET /edition and returns the edition string.
func fetchCurrentEdition(ctx context.Context, baseURL string) (license.Edition, error) {
	client := &http.Client{Timeout: licenseHTTPTimeout}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/edition", http.NoBody)
	if err != nil {
		return "", fmt.Errorf("fetch edition: create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetch edition: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("fetch edition: HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("fetch edition: read body: %w", err)
	}

	var result struct {
		Edition license.Edition `json:"edition"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("fetch edition: unmarshal: %w", err)
	}
	return result.Edition, nil
}

// pollEdition polls GET /edition every 500ms for up to 5 seconds until the edition matches expected.
func pollEdition(ctx context.Context, baseURL string, expected license.Edition) (bool, error) {
	deadline := time.Now().Add(propagationTimeout)
	for time.Now().Before(deadline) {
		edition, err := fetchCurrentEdition(ctx, baseURL)
		if err == nil && edition == expected {
			return true, nil
		}
		select {
		case <-ctx.Done():
			return false, fmt.Errorf("poll edition: %w", ctx.Err())
		case <-time.After(propagationPollInterval):
		}
	}
	return false, nil
}

// propagationCheck creates a pass/fail CheckResult for an edition propagation step.
func propagationCheck(transition string, ok bool) metrics.CheckResult {
	if ok {
		return metrics.CheckResult{Name: transition + " propagation", Status: "pass"}
	}
	return metrics.CheckResult{Name: transition + " propagation", Status: "fail", Error: "edition not reflected within 5s"}
}

// --- Gate check helpers ---

// checkSSEGate verifies SSE endpoint accessibility based on expected edition behavior.
func checkSSEGate(ctx context.Context, gwURL, token string, expectSuccess bool, logger zerolog.Logger) []metrics.CheckResult {
	client, statusCode, err := testersse.Connect(ctx, testersse.ConnectConfig{
		GatewayURL: gwURL,
		Channels:   []string{"test.license-gate"},
		Token:      token,
		Logger:     logger,
	})

	if expectSuccess {
		if err != nil {
			return []metrics.CheckResult{{Name: "sse gate (expect 200)", Status: "fail", Error: fmt.Sprintf("expected 200: %v", err)}}
		}
		_ = client.Close()
		return []metrics.CheckResult{{Name: "sse gate (expect 200)", Status: "pass"}}
	}

	// Expect 403
	if statusCode == http.StatusForbidden {
		return []metrics.CheckResult{{Name: "sse gate (expect 403)", Status: "pass"}}
	}
	if client != nil {
		_ = client.Close()
		return []metrics.CheckResult{{Name: "sse gate (expect 403)", Status: "fail", Error: "expected 403, got 200 (connection succeeded)"}}
	}
	errMsg := fmt.Sprintf("expected 403, got %d", statusCode)
	if err != nil {
		errMsg = fmt.Sprintf("expected 403: %v", err)
	}
	return []metrics.CheckResult{{Name: "sse gate (expect 403)", Status: "fail", Error: errMsg}}
}

// checkRESTGate verifies REST publish endpoint accessibility based on expected edition behavior.
func checkRESTGate(ctx context.Context, gwURL, token string, expectSuccess bool) []metrics.CheckResult {
	client := restpublish.NewClient(gwURL)
	body := []byte(`{"channel":"test.license-gate","data":{}}`)
	statusCode, _, err := client.PublishRaw(ctx, body, restpublish.AuthConfig{Token: token}, "application/json")

	if err != nil {
		return []metrics.CheckResult{{Name: "rest publish gate", Status: "fail", Error: fmt.Sprintf("request failed: %v", err)}}
	}

	if expectSuccess {
		if statusCode == http.StatusOK {
			return []metrics.CheckResult{{Name: "rest publish gate (expect 200)", Status: "pass"}}
		}
		return []metrics.CheckResult{{Name: "rest publish gate (expect 200)", Status: "fail", Error: fmt.Sprintf("expected 200, got %d", statusCode)}}
	}

	// Expect 403
	if statusCode == http.StatusForbidden {
		return []metrics.CheckResult{{Name: "rest publish gate (expect 403)", Status: "pass"}}
	}
	return []metrics.CheckResult{{Name: "rest publish gate (expect 403)", Status: "fail", Error: fmt.Sprintf("expected 403, got %d", statusCode)}}
}

// checkPushGate verifies push endpoint accessibility. Returns "skip" on 503 (push not deployed).
func checkPushGate(ctx context.Context, gwURL, token string, expectSuccess bool) []metrics.CheckResult {
	statusCode, err := pushGetVAPIDKey(ctx, gwURL, token)

	if err != nil && statusCode == http.StatusServiceUnavailable {
		return []metrics.CheckResult{{Name: "push gate", Status: "skip", Error: "push service not deployed (503)"}}
	}

	if expectSuccess {
		if err == nil {
			return []metrics.CheckResult{{Name: "push gate (expect 200)", Status: "pass"}}
		}
		return []metrics.CheckResult{{Name: "push gate (expect 200)", Status: "fail", Error: fmt.Sprintf("expected 200: %v", err)}}
	}

	// Expect 403
	if statusCode == http.StatusForbidden {
		return []metrics.CheckResult{{Name: "push gate (expect 403)", Status: "pass"}}
	}
	if err != nil {
		return []metrics.CheckResult{{Name: "push gate (expect 403)", Status: "fail", Error: fmt.Sprintf("expected 403: %v", err)}}
	}
	return []metrics.CheckResult{{Name: "push gate (expect 403)", Status: "fail", Error: fmt.Sprintf("expected 403, got %d", statusCode)}}
}

// --- WS survival + message delivery ---

// checkWSSurvival verifies the WebSocket connection is still alive by attempting a subscribe.
func checkWSSurvival(client *testerws.Client, tenantID, transition string) metrics.CheckResult {
	if err := client.Subscribe([]string{tenantChannel(tenantID, "test.survival-"+transition)}); err != nil {
		return metrics.CheckResult{Name: transition + " ws survival", Status: "fail", Error: fmt.Sprintf("subscribe failed: %v", err)}
	}
	return metrics.CheckResult{Name: transition + " ws survival", Status: "pass"}
}

// checkMessageDelivery publishes 1 message via REST and verifies it arrives on the WebSocket within 3 seconds.
// The msgCh channel receives messages from the WS client's ReadLoop (set up at connect time via OnMessage callback).
func checkMessageDelivery(ctx context.Context, gwURL, token, tenantID string, msgCh <-chan string, transition string) []metrics.CheckResult {
	channel := tenantChannel(tenantID, "test.license-delivery")

	// Publish via REST
	restClient := restpublish.NewClient(gwURL)
	msgBody, _ := json.Marshal(map[string]string{"test": "license-reload-" + transition}) // json.Marshal on literal map cannot fail
	_, pubErr := restClient.Publish(ctx, restpublish.Request{
		Channel: channel,
		Data:    msgBody,
	}, restpublish.AuthConfig{Token: token})
	if pubErr != nil {
		return []metrics.CheckResult{{Name: transition + " message delivery", Status: "fail", Error: fmt.Sprintf("publish failed: %v", pubErr)}}
	}

	// Wait for delivery on the channel
	deliveryCtx, cancel := context.WithTimeout(ctx, messageDeliveryTimeout)
	defer cancel()

	select {
	case <-msgCh:
		return []metrics.CheckResult{{Name: transition + " message delivery", Status: "pass"}}
	case <-deliveryCtx.Done():
		return []metrics.CheckResult{{Name: transition + " message delivery", Status: "fail", Error: "message not received within 3s"}}
	}
}
