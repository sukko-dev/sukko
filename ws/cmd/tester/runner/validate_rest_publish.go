package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/cmd/tester/metrics"
	"github.com/sukko-dev/sukko/cmd/tester/restpublish"
	"github.com/sukko-dev/sukko/cmd/tester/sse"
	testerws "github.com/sukko-dev/sukko/cmd/tester/ws"
	"github.com/sukko-dev/sukko/internal/shared/license"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// oversizedBodySize exceeds the default GATEWAY_MAX_PUBLISH_SIZE (65536 bytes).
const oversizedBodySize = 65537

func validateRestPublish(ctx context.Context, run *TestRun, logger zerolog.Logger) ([]metrics.CheckResult, error) {
	provClient := run.authResult.ProvClient
	tenantID := run.authResult.TenantID
	sseChannel := tenantChannel(tenantID, validateSSEChannel)
	token := run.authResult.TokenFunc(0)

	// Step 0: Set routing rules and channel rules (edition-gate tolerant — on a
	// Community stack routing rules may be gated and are not available).
	if err := setupSuiteRoutingRules(ctx, provClient, tenantID, logger); err != nil {
		return []metrics.CheckResult{{Name: "setup routing rules", Status: "fail", Error: err.Error()}}, nil
	}
	_ = provClient.SetChannelRules(ctx, tenantID, testChannelRules)

	restClient := restpublish.NewClient(httpURL(run.Config.GatewayURL))
	auth := restpublish.AuthConfig{Token: token}

	// Routing rules reach ws-server's producer via an async gRPC snapshot stream. Post-#179
	// a publish before that snapshot arrives is a retryable 409 (no rule yet) or 503 (not
	// synced) — probe until routable so the delivery checks below aren't flaky.
	if err := waitForRoutable(ctx, restClient, sseChannel, auth); err != nil {
		return []metrics.CheckResult{{Name: "routing rules propagate", Status: "fail", Error: err.Error()}}, nil
	}

	var checks []metrics.CheckResult

	// Step 1: Connect WS subscriber + subscribe. Track received msg_ids in a presence set,
	// accepting delivered ({"type":"message"}) as well as peer-forwarded ({"type":"publish"})
	// envelopes (acceptsDeliveryEnvelope) — a "publish"-only filter drops delivered broadcasts.
	tracker := newMsgIDTracker()

	wsClient, err := connectWithRetry(ctx, run.Config.GatewayURL, token, logger, func(msg testerws.Message) {
		if !acceptsDeliveryEnvelope(msg.Type) {
			return
		}
		var payload struct {
			MsgID string `json:"msg_id"`
		}
		if err := json.Unmarshal(msg.Data, &payload); err != nil || payload.MsgID == "" {
			return
		}
		tracker.Add(payload.MsgID, msg.Mid)
	})
	if err != nil {
		return []metrics.CheckResult{{
			Name: "ws subscriber connect", Status: "fail", Error: err.Error(),
		}}, nil
	}
	defer func() { _ = wsClient.Close() }()

	// Start ReadLoop so OnMessage fires
	go func() {
		defer logging.RecoverPanic(logger, "rest-publish-ws-readloop", nil)
		_, _ = wsClient.ReadLoop(ctx)
	}()

	if err := wsClient.Subscribe([]string{sseChannel}); err != nil {
		return []metrics.CheckResult{{
			Name: "ws subscribe", Status: "fail", Error: err.Error(),
		}}, nil
	}

	// Delivery-liveness warmup: prove the publish→(produce→consume→)broadcast loop is live before the
	// measured publishes. Reuses the pubsub engine's probeUntilLive (republishing a probe until it is
	// received) to absorb the Kafka consumer-join AtEnd cold-start window. The window is per-topic, so
	// one warmup on sseChannel covers BOTH the WS and SSE legs; on the direct backend it returns within
	// one interval. Returns an error (never a silent skip) if the loop is not live within the bound.
	warmupID := "warmup-" + uuid.NewString()
	warmupPayload, _ := json.Marshal(map[string]any{"msg_id": warmupID}) // json.Marshal on literal map of primitives cannot fail
	warmupErr := probeUntilLive(ctx, deliveryWarmupTimeout, deliveryWarmupInterval,
		func() error {
			if _, err := restClient.Publish(ctx, restpublish.Request{Channel: sseChannel, Data: warmupPayload}, auth); err != nil {
				return fmt.Errorf("publish warmup probe on %s: %w", sseChannel, err)
			}
			return nil
		},
		func() bool { return tracker.Has(warmupID) },
		tracker.Clear,
		logger,
	)

	// Step 2: Publish via REST → verify WS receives. If the warmup proved the loop is NOT live, emit
	// the delivery check present-and-fail (so REQUIRE_PASS/the verdict red the cell) without wasting a timeout.
	if warmupErr != nil {
		checks = append(checks, metrics.CheckResult{
			Name: "rest publish → ws receive", Status: "fail",
			Error: fmt.Sprintf("delivery loop not live: %v", warmupErr),
		})
	} else {
		msgID := uuid.NewString()
		payload, _ := json.Marshal(map[string]any{ // json.Marshal on literal map of primitives cannot fail
			"msg_id": msgID,
			"ts":     time.Now().UnixMilli(),
		})

		start := time.Now()
		pubResp, pubErr := restClient.Publish(ctx, restpublish.Request{
			Channel: sseChannel,
			Data:    payload,
		}, auth)
		if pubErr != nil {
			run.Collector.RESTPublishErrors.Add(1)
			checks = append(checks, metrics.CheckResult{
				Name: "rest publish → ws receive", Status: "fail",
				Error: fmt.Sprintf("rest publish: %v", pubErr),
			})
		} else {
			run.Collector.RESTPublishSuccess.Add(1)

			// Wait for WS to receive the measured message (loop already proven live by the warmup).
			if tracker.WaitFor(ctx, msgID, defaultDeliveryTimeout) {
				checks = append(checks, metrics.CheckResult{
					Name: "rest publish → ws receive", Status: "pass",
					Latency: time.Since(start).Round(time.Millisecond).String(),
				})
				// Ack↔delivery identity (ADR-0008): the REST response's mid must be
				// present (single-topic routing on every cell → the backend always
				// assigns one) and byte-equal to the delivered envelope's mid.
				switch deliveredMid := tracker.MidOf(msgID); {
				case pubResp == nil || pubResp.Mid == "":
					checks = append(checks, metrics.CheckResult{
						Name: "rest publish mid equality", Status: "fail",
						Error: "REST response carried no mid (expected: single-topic publish always assigns one)",
					})
				case deliveredMid != pubResp.Mid:
					checks = append(checks, metrics.CheckResult{
						Name: "rest publish mid equality", Status: "fail",
						Error: fmt.Sprintf("ack mid %q != delivered envelope mid %q", pubResp.Mid, deliveredMid),
					})
				default:
					checks = append(checks, metrics.CheckResult{
						Name: "rest publish mid equality", Status: "pass",
					})
				}
			} else {
				checks = append(checks, metrics.CheckResult{
					Name: "rest publish → ws receive", Status: "fail",
					Error: "ws subscriber did not receive message within timeout",
				})
			}
		}
	}

	// Step 3: Connect SSE subscriber, publish via REST → verify SSE receives. The SSE subscriber MUST
	// use the tenant-prefixed channel (a bare channel is stripped by the gateway's channel filter → 400).
	// SSE transport stays Pro-gated (REST publish itself is Community, ADR-0009) — on Community the
	// SSE leg must hit the 403 gate: connect-rejected → explicit skip; connect-succeeded → gate
	// regression (fail). Discriminates on the cause, not the outcome.
	edition, editionErr := fetchCurrentEdition(ctx, httpURL(run.Config.GatewayURL))
	sseGated := editionErr == nil && !license.EditionHasFeature(edition, license.SSETransport)
	sseClient, _, sseErr := sse.Connect(ctx, sse.ConnectConfig{
		GatewayURL: httpURL(run.Config.GatewayURL),
		Channels:   []string{sseChannel},
		Token:      token,
		Logger:     logger,
	})
	switch {
	case sseGated && sseErr != nil && strings.Contains(sseErr.Error(), "403"):
		checks = append(checks, metrics.CheckResult{
			Name: "rest publish → sse receive", Status: "skip",
			Error: "sse transport requires Pro edition or higher (403)",
		})
		if sseClient != nil {
			_ = sseClient.Close()
		}
		return checks, nil
	case sseGated && sseErr == nil:
		checks = append(checks, metrics.CheckResult{
			Name: "rest publish → sse receive", Status: "fail",
			Error: "community: SSE connect succeeded but SSETransport is gated (gate regressed)",
		})
		_ = sseClient.Close()
		return checks, nil
	case warmupErr != nil:
		// Loop not live (proven by the warmup) — emit present-and-fail rather than waiting out the SSE read.
		checks = append(checks, metrics.CheckResult{
			Name: "rest publish → sse receive", Status: "fail",
			Error: fmt.Sprintf("delivery loop not live: %v", warmupErr),
		})
		if sseClient != nil {
			_ = sseClient.Close()
		}
	case sseErr != nil:
		checks = append(checks, metrics.CheckResult{
			Name: "rest publish → sse receive", Status: "fail",
			Error: fmt.Sprintf("sse connect: %v", sseErr),
		})
	default:
		defer func() { _ = sseClient.Close() }()

		time.Sleep(500 * time.Millisecond) // subscription propagation

		msgID2 := uuid.NewString()
		payload2, _ := json.Marshal(map[string]any{ // json.Marshal on literal map of primitives cannot fail
			"msg_id": msgID2,
			"ts":     time.Now().UnixMilli(),
		})

		start2 := time.Now()
		_, pubErr2 := restClient.Publish(ctx, restpublish.Request{
			Channel: sseChannel,
			Data:    payload2,
		}, auth)
		if pubErr2 != nil {
			run.Collector.RESTPublishErrors.Add(1)
			checks = append(checks, metrics.CheckResult{
				Name: "rest publish → sse receive", Status: "fail",
				Error: fmt.Sprintf("rest publish: %v", pubErr2),
			})
		} else {
			run.Collector.RESTPublishSuccess.Add(1)

			// Read SSE events until the one carrying msgID2 arrives (or timeout). The SSE
			// transport delivers the full broadcast envelope, so the msg_id is matched at
			// .data.msg_id (readSSEUntilMsgID) — a top-level parse never matches and the read
			// always times out. Filtering by the specific msg_id keeps the check non-vacuous
			// so unrelated traffic (e.g. the routing probe) cannot satisfy it.
			readCtx, cancel := context.WithTimeout(ctx, defaultDeliveryTimeout)
			_, readErr := readSSEUntilMsgID(readCtx, sseClient, msgID2)
			cancel()

			if readErr == nil {
				run.Collector.SSEMessagesReceived.Add(1)
				checks = append(checks, metrics.CheckResult{
					Name: "rest publish → sse receive", Status: "pass",
					Latency: time.Since(start2).Round(time.Millisecond).String(),
				})
			} else {
				checks = append(checks, metrics.CheckResult{
					Name: "rest publish → sse receive", Status: "fail",
					Error: fmt.Sprintf("sse subscriber did not receive msg_id=%s within timeout: %v", msgID2, readErr),
				})
			}
		}
	}

	// Step 4: Verify response format
	resp, err := restClient.Publish(ctx, restpublish.Request{
		Channel: sseChannel,
		Data:    json.RawMessage(`{"check":"format"}`),
	}, auth)
	if err != nil {
		checks = append(checks, metrics.CheckResult{
			Name: "response format", Status: "fail", Error: err.Error(),
		})
	} else {
		if resp.Status == "accepted" && resp.Channel == sseChannel {
			run.Collector.RESTPublishSuccess.Add(1)
			checks = append(checks, metrics.CheckResult{
				Name: "response format", Status: "pass",
			})
		} else {
			checks = append(checks, metrics.CheckResult{
				Name: "response format", Status: "fail",
				Error: fmt.Sprintf("status=%q channel=%q, want accepted/%s", resp.Status, resp.Channel, sseChannel),
			})
		}
	}

	// Steps 5-10: Error cases using PublishRaw
	errorCases := []struct {
		name        string
		body        []byte
		contentType string
		authCfg     restpublish.AuthConfig
		wantStatus  int
	}{
		{
			name:        "missing channel → 400",
			body:        []byte(`{"data":{"x":1}}`),
			contentType: "application/json",
			authCfg:     auth,
			wantStatus:  400,
		},
		{
			name:        "missing data → 400",
			body:        []byte(`{"channel":"general.test"}`),
			contentType: "application/json",
			authCfg:     auth,
			wantStatus:  400,
		},
		{
			name:        "invalid JSON → 400",
			body:        []byte(`not valid json`),
			contentType: "application/json",
			authCfg:     auth,
			wantStatus:  400,
		},
		{
			name:        "oversized body → 413",
			body:        []byte(`{"channel":"general.test","data":"` + strings.Repeat("x", oversizedBodySize) + `"}`),
			contentType: "application/json",
			authCfg:     auth,
			wantStatus:  413,
		},
		{
			name:        "no auth → 401",
			body:        []byte(`{"channel":"general.test","data":{"x":1}}`),
			contentType: "application/json",
			authCfg:     restpublish.AuthConfig{},
			wantStatus:  401,
		},
		{
			name:        "wrong content-type → 400",
			body:        []byte(`{"channel":"general.test","data":{"x":1}}`),
			contentType: "text/plain",
			authCfg:     auth,
			wantStatus:  400,
		},
	}

	for _, tc := range errorCases {
		status, _, rawErr := restClient.PublishRaw(ctx, tc.body, tc.authCfg, tc.contentType)
		if rawErr != nil {
			checks = append(checks, metrics.CheckResult{
				Name: tc.name, Status: "fail",
				Error: fmt.Sprintf("transport error: %v", rawErr),
			})
			continue
		}

		if status == tc.wantStatus {
			checks = append(checks, metrics.CheckResult{
				Name: tc.name, Status: "pass",
			})
		} else {
			run.Collector.RESTPublishErrors.Add(1)
			checks = append(checks, metrics.CheckResult{
				Name: tc.name, Status: "fail",
				Error: fmt.Sprintf("got HTTP %d, want %d", status, tc.wantStatus),
			})
		}
	}

	return checks, nil
}

// waitForRoutable publishes a probe message until the gateway accepts it, absorbing the
// routing-rules propagation window. Post-#179 an early publish is a retryable 409 (no
// applicable rule synced yet) or 503 (snapshot not received); any other status is a real
// failure and returns immediately. Bounded to ~2s.
func waitForRoutable(ctx context.Context, c *restpublish.Client, channel string, auth restpublish.AuthConfig) error {
	body, _ := json.Marshal(map[string]any{
		"channel": channel,
		"data":    map[string]any{"probe": true},
	})
	deadline := time.After(6 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	var lastStatus int
	for {
		status, _, err := c.PublishRaw(ctx, body, auth, "application/json")
		if err == nil && status < 300 {
			return nil
		}
		lastStatus = status
		// Retryable propagation states: 409 (kafka routing not yet applied), 503
		// (not synced), 403 (channel-rule propagation — post-remap the publish
		// endpoint has no edition gate, so a 403 here is the async rule stream
		// catching up, e.g. after a neighboring suite replaced the shared
		// tenant's rules), 429 (per-tenant publish limiter recovering after the
		// ratelimit suite). Anything else is a real failure; all of these still
		// fail loudly at the deadline with the last status.
		if status != http.StatusConflict && status != http.StatusServiceUnavailable &&
			status != http.StatusForbidden && status != http.StatusTooManyRequests {
			if err != nil {
				return fmt.Errorf("routing probe transport error (status=%d): %w", status, err)
			}
			return fmt.Errorf("routing probe rejected with status %d", status)
		}
		select {
		case <-deadline:
			return fmt.Errorf("routing rules did not propagate within timeout (last status=%d)", lastStatus)
		case <-ticker.C:
		}
	}
}

// msgIDTracker is a presence set of received msg_ids, guarded for concurrent writes from the WS
// read-loop goroutine. A set (not a single last-write value) so any published attempt AND duplicate
// deliveries both count, and a warmup probe cannot flip the tracked value out from under a poll.
type msgIDTracker struct {
	mu  sync.Mutex
	ids map[string]string // msg_id → delivered envelope mid ("" when absent)
}

func newMsgIDTracker() *msgIDTracker { return &msgIDTracker{ids: make(map[string]string)} }

func (t *msgIDTracker) Add(id, mid string) {
	t.mu.Lock()
	t.ids[id] = mid
	t.mu.Unlock()
}

func (t *msgIDTracker) Has(id string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.ids[id]
	return ok
}

// MidOf returns the delivered envelope's mid for id ("" if absent/unknown).
func (t *msgIDTracker) MidOf(id string) string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.ids[id]
}

func (t *msgIDTracker) Clear() {
	t.mu.Lock()
	t.ids = make(map[string]string)
	t.mu.Unlock()
}

// WaitFor polls until id is present, or timeout / ctx cancellation elapses.
func (t *msgIDTracker) WaitFor(ctx context.Context, id string, timeout time.Duration) bool {
	deadline := time.After(timeout)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if t.Has(id) {
			return true
		}
		select {
		case <-deadline:
			return false
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}
