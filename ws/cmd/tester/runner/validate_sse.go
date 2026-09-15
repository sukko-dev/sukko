package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/cmd/tester/metrics"
	"github.com/sukko-dev/sukko/cmd/tester/restpublish"
	"github.com/sukko-dev/sukko/cmd/tester/sse"
)

// validateSSEChannel is the channel SUFFIX used by the SSE and rest-publish
// validation suites — qualified with the run's tenant via tenantChannel().
const validateSSEChannel = "general.validate"

func validateSSE(ctx context.Context, run *TestRun, logger zerolog.Logger) ([]metrics.CheckResult, error) {
	provClient := run.authResult.ProvClient
	tenantID := run.authResult.TenantID
	sseChannel := tenantChannel(tenantID, validateSSEChannel)

	// Step 0: Set routing rules and channel rules
	_ = provClient.SetRoutingRules(ctx, tenantID, testRoutingRules)
	_ = provClient.SetChannelRules(ctx, tenantID, testChannelRules)

	var checks []metrics.CheckResult

	// Step 1: Connect WS client + SSE client, wait for propagation
	token := run.authResult.TokenFunc(0)

	wsClient, err := connectWithRetry(ctx, run.Config.GatewayURL, token, logger)
	if err != nil {
		return []metrics.CheckResult{{
			Name: "ws connect", Status: "fail", Error: err.Error(),
		}}, nil
	}
	defer func() { _ = wsClient.Close() }()

	sseClient, statusCode, err := sse.Connect(ctx, sse.ConnectConfig{
		GatewayURL: httpURL(run.Config.GatewayURL),
		Channels:   []string{sseChannel},
		Token:      token,
		Logger:     logger,
	})
	if err != nil {
		return []metrics.CheckResult{{
			Name: "sse connect", Status: "fail",
			Error: fmt.Sprintf("HTTP %d: %v", statusCode, err),
		}}, nil
	}
	defer func() { _ = sseClient.Close() }()

	checks = append(checks, metrics.CheckResult{
		Name: "sse connect", Status: "pass",
	})

	time.Sleep(500 * time.Millisecond) // subscription propagation

	// Step 2: Publish via WS → verify SSE receives with correct id/event/data
	msgID := uuid.NewString()
	payload, _ := json.Marshal(map[string]any{ // json.Marshal on literal map of primitives cannot fail
		"msg_id": msgID,
		"ts":     time.Now().UnixMilli(),
	})

	start := time.Now()
	if err := wsClient.Publish(sseChannel, payload); err != nil {
		checks = append(checks, metrics.CheckResult{
			Name: "ws publish → sse receive", Status: "fail",
			Error: fmt.Sprintf("ws publish: %v", err),
		})
	} else {
		// Read until the event carrying this publish's msg_id arrives (matched at .data.msg_id,
		// the envelope's inner payload). Filtering by msg_id keeps the check non-vacuous — an
		// unrelated event (keepalive-adjacent traffic, a prior message) cannot satisfy it.
		readCtx, cancel := context.WithTimeout(ctx, defaultDeliveryTimeout)
		event, readErr := readSSEUntilMsgID(readCtx, sseClient, msgID)
		cancel()

		if readErr != nil {
			checks = append(checks, metrics.CheckResult{
				Name: "ws publish → sse receive", Status: "fail",
				Error: readErr.Error(),
			})
		} else {
			run.Collector.SSEMessagesReceived.Add(1)

			// Verify event format
			var errs []string
			if event.ID == "" {
				errs = append(errs, "missing event id")
			}
			if event.Type != "message" {
				errs = append(errs, fmt.Sprintf("event type = %q, want %q", event.Type, "message"))
			}
			if event.Data == "" {
				errs = append(errs, "empty event data")
			}

			if len(errs) == 0 {
				checks = append(checks, metrics.CheckResult{
					Name: "ws publish → sse receive", Status: "pass",
					Latency: time.Since(start).Round(time.Millisecond).String(),
				})
			} else {
				checks = append(checks, metrics.CheckResult{
					Name: "ws publish → sse receive", Status: "fail",
					Error: strings.Join(errs, "; "),
				})
			}
		}
	}

	// Step 3: Publish via REST → verify SSE receives
	// Need a fresh SSE connection since step 2 may have consumed the previous one's context
	sseClient2, _, err := sse.Connect(ctx, sse.ConnectConfig{
		GatewayURL: httpURL(run.Config.GatewayURL),
		Channels:   []string{sseChannel},
		Token:      token,
		Logger:     logger,
	})
	if err != nil {
		checks = append(checks, metrics.CheckResult{
			Name: "rest publish → sse receive", Status: "fail",
			Error: fmt.Sprintf("sse reconnect: %v", err),
		})
	} else {
		defer func() { _ = sseClient2.Close() }()

		time.Sleep(500 * time.Millisecond) // subscription propagation

		restClient := restpublish.NewClient(httpURL(run.Config.GatewayURL))
		msgID2 := uuid.NewString()
		payload2, _ := json.Marshal(map[string]any{
			"msg_id": msgID2,
			"ts":     time.Now().UnixMilli(),
		})

		start2 := time.Now()
		_, pubErr := restClient.Publish(ctx, restpublish.Request{
			Channel: sseChannel,
			Data:    payload2,
		}, restpublish.AuthConfig{Token: token})
		if pubErr != nil {
			checks = append(checks, metrics.CheckResult{
				Name: "rest publish → sse receive", Status: "fail",
				Error: fmt.Sprintf("rest publish: %v", pubErr),
			})
		} else {
			run.Collector.RESTPublishSuccess.Add(1)

			// Match this REST publish's msg_id at .data.msg_id (non-vacuous — see Step 2).
			readCtx2, cancel2 := context.WithTimeout(ctx, defaultDeliveryTimeout)
			event2, readErr2 := readSSEUntilMsgID(readCtx2, sseClient2, msgID2)
			cancel2()

			if readErr2 != nil {
				checks = append(checks, metrics.CheckResult{
					Name: "rest publish → sse receive", Status: "fail",
					Error: readErr2.Error(),
				})
			} else {
				run.Collector.SSEMessagesReceived.Add(1)
				checks = append(checks, metrics.CheckResult{
					Name: "rest publish → sse receive", Status: "pass",
					Latency: time.Since(start2).Round(time.Millisecond).String(),
				})
				_ = event2 // used for receipt confirmation
			}
		}
	}

	// Step 4: No credentials → 401
	checks = append(checks, checkSSEReject(ctx, run.Config.GatewayURL, sse.ConnectConfig{
		GatewayURL: httpURL(run.Config.GatewayURL),
		Channels:   []string{sseChannel},
		Logger:     logger,
	}, "no credentials → 401", 401))

	// Step 5: Expired JWT → 401
	expiredToken, err := run.authResult.Minter.MintExpired(1)
	if err != nil {
		checks = append(checks, metrics.CheckResult{
			Name: "expired JWT → 401", Status: "fail",
			Error: fmt.Sprintf("mint expired: %v", err),
		})
	} else {
		checks = append(checks, checkSSEReject(ctx, run.Config.GatewayURL, sse.ConnectConfig{
			GatewayURL: httpURL(run.Config.GatewayURL),
			Channels:   []string{sseChannel},
			Token:      expiredToken,
			Logger:     logger,
		}, "expired JWT → 401", 401))
	}

	// Steps 6-7: Edge cases → 400
	checks = append(checks,
		checkSSEReject(ctx, run.Config.GatewayURL, sse.ConnectConfig{
			GatewayURL: httpURL(run.Config.GatewayURL),
			Channels:   nil,
			Token:      token,
			Logger:     logger,
		}, "no channels → 400", 400),
		checkSSEReject(ctx, run.Config.GatewayURL, sse.ConnectConfig{
			GatewayURL: httpURL(run.Config.GatewayURL),
			Channels:   []string{",,,"},
			Token:      token,
			Logger:     logger,
		}, "empty channels → 400", 400),
	)

	return checks, nil
}

// checkSSEReject connects to SSE and expects a specific HTTP error status code.
func checkSSEReject(ctx context.Context, _ string, cfg sse.ConnectConfig, name string, wantStatus int) metrics.CheckResult {
	_, status, err := sse.Connect(ctx, cfg)
	switch {
	case err != nil && status == wantStatus:
		return metrics.CheckResult{Name: name, Status: "pass"}
	case err == nil:
		return metrics.CheckResult{
			Name: name, Status: "fail",
			Error: fmt.Sprintf("expected %d, connection was accepted", wantStatus),
		}
	default:
		return metrics.CheckResult{
			Name: name, Status: "fail",
			Error: fmt.Sprintf("expected %d, got HTTP %d: %v", wantStatus, status, err),
		}
	}
}
