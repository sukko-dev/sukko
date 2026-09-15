package runner

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/cmd/tester/metrics"
	"github.com/sukko-dev/sukko/cmd/tester/restpublish"
)

// pushTestChannel is the channel pattern used by push validation (with tenant prefix).
const pushTestChannel = "general.push-test"

// Step-0 availability probe bounds. The probe is the run's FIRST gateway interaction and races
// control-plane propagation of the just-created throwaway tenant: the gateway's tenant key
// registry is fed by an async WatchKeys stream and its push-service gRPC channel connects
// lazily, so the first call(s) can transiently fail (401 while the key delta is in flight,
// 500 while the push conn establishes). Terminal statuses (200 pass, 403 edition skip,
// 503 not-deployed skip) return immediately; everything else is retried until the deadline and
// only then classified as a real failure. Mirrors waitForRoutable's bounded propagation retry.
const (
	pushAvailableTimeout  = 10 * time.Second
	pushAvailableInterval = 250 * time.Millisecond
)

// pushProbeTerminal reports whether a VAPID-probe HTTP status is a terminal outcome for the
// "push available" check (mapped by the caller: 200 pass, 403 skip, 503 skip). Anything else
// (0 transport error, 401 propagation, other 5xx) is treated as transient and retried.
func pushProbeTerminal(status int) bool {
	return status == http.StatusOK || status == http.StatusForbidden || status == http.StatusServiceUnavailable
}

// waitForPushAvailable polls probe until it returns a terminal status (per pushProbeTerminal)
// or timeout elapses, returning the last observed status and error for classification.
func waitForPushAvailable(ctx context.Context, timeout, interval time.Duration, probe func(context.Context) (int, error)) (int, error) {
	status, err := probe(ctx)
	if pushProbeTerminal(status) {
		return status, err
	}

	deadline := time.After(timeout)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return status, err
		case <-deadline:
			return status, err
		case <-ticker.C:
			status, err = probe(ctx)
			if pushProbeTerminal(status) {
				return status, err
			}
		}
	}
}

func validatePush(ctx context.Context, run *TestRun, logger zerolog.Logger) ([]metrics.CheckResult, error) {
	provClient := run.authResult.ProvClient
	tenantID := run.authResult.TenantID
	token := run.authResult.TokenFunc(0)
	// GatewayURL is a ws:// URL (for WS client connects); the push suite only calls HTTP
	// endpoints (VAPID key, subscribe, REST publish), so convert ws://→http:// once here —
	// http.Client rejects a ws:// scheme. Mirrors validate_rest_publish.go's httpURL() wrap.
	gwURL := httpURL(run.Config.GatewayURL)

	var checks []metrics.CheckResult
	var deviceIDs []int64 // track for cleanup

	// Cleanup: reverse creation order — subscriptions, channels, credentials.
	defer func() { //nolint:contextcheck // intentional: use background context so cleanup survives parent cancellation
		cleanupCtx := context.Background()
		for _, id := range deviceIDs {
			if err := pushUnsubscribe(cleanupCtx, gwURL, token, id); err != nil {
				logger.Debug().Err(err).Int64("device_id", id).Msg("push cleanup: unsubscribe failed")
			}
		}
		if err := provClient.DeletePushChannels(cleanupCtx, tenantID); err != nil {
			logger.Debug().Err(err).Msg("push cleanup: delete channels failed")
		}
		if err := provClient.DeletePushCredentials(cleanupCtx, tenantID, "vapid"); err != nil {
			logger.Debug().Err(err).Msg("push cleanup: delete credentials failed")
		}
	}()

	// Step 0: Health probe — verify push infrastructure is available. Retried (bounded) because
	// this is the first gateway call against a tenant created milliseconds ago — see the
	// pushAvailableTimeout comment for the propagation races this absorbs.
	vapidStatus, vapidErr := waitForPushAvailable(ctx, pushAvailableTimeout, pushAvailableInterval,
		func(ctx context.Context) (int, error) { return pushGetVAPIDKey(ctx, gwURL, token) })
	if vapidErr != nil {
		switch vapidStatus {
		case 404:
			return []metrics.CheckResult{{
				Name: "push available", Status: "skip",
				Error: "push disabled at the gateway (GATEWAY_PUSH_ENABLED=false, 404)",
			}}, nil
		case 503:
			return []metrics.CheckResult{{
				Name: "push available", Status: "skip",
				Error: "push service not deployed (503)",
			}}, nil
		case 403:
			return []metrics.CheckResult{{
				Name: "push available", Status: "skip",
				Error: "push requires Pro edition or higher (403)",
			}}, nil
		default:
			return []metrics.CheckResult{{
				Name: "push available", Status: "fail",
				Error: fmt.Sprintf("VAPID key probe failed: HTTP %d: %v", vapidStatus, vapidErr),
			}}, nil
		}
	}
	checks = append(checks, metrics.CheckResult{
		Name: "push available", Status: "pass",
	})

	// Step 0.4: Provision routing + channel rules (#179 P1 reconciliation). In kafka mode a REST
	// publish with no applicable routing rule is now REJECTED (409 PUBLISH_NOT_ROUTABLE), and the
	// gateway denies publishes to a tenant with no channel rules. Set a catch-all routing rule +
	// the shared channel rules (publish authorization for general.*) so the push publishes below
	// resolve a valid topic, then wait for the routing snapshot to reach ws-server (retry 409/503).
	// Harmless in direct mode (no routing → waitForRoutable's first probe returns 2xx immediately).
	if err := provClient.SetRoutingRules(ctx, tenantID, testRoutingRules); err != nil {
		return []metrics.CheckResult{{Name: "push routing rules", Status: "fail", Error: err.Error()}}, nil
	}
	_ = provClient.SetChannelRules(ctx, tenantID, testChannelRules)
	if err := waitForRoutable(ctx, restpublish.NewClient(gwURL), tenantID+"."+pushTestChannel, restpublish.AuthConfig{Token: token}); err != nil {
		return []metrics.CheckResult{{Name: "push routing propagate", Status: "fail", Error: err.Error()}}, nil
	}

	// Step 0.5: Delivery verification — runs BEFORE the fake-credential
	// overwrite below: real WebPush delivery needs the auto-generated (real)
	// VAPID keys from step 0 and real client crypto material. This asserts a
	// notification actually ARRIVES at an endpoint, not merely that the
	// publish was accepted.
	checks = append(checks, verifyPushDelivery(ctx, run, gwURL, token, tenantID, logger)...)

	// Step 1: Setup — set VAPID credentials + channel config
	// Note: GetVAPIDKey in step 0 auto-generates credentials. SetPushCredentials overwrites via upsert.
	vapidCreds := `{"public_key":"BGxGBnqX_test_key","private_key":"OISg2_test_private"}` //nolint:gosec // G101: fake test credentials for push validation — not real secrets
	if err := provClient.SetPushCredentials(ctx, tenantID, "vapid", vapidCreds); err != nil {
		checks = append(checks, metrics.CheckResult{
			Name: "credential create", Status: "fail", Error: err.Error(),
		})
	} else {
		checks = append(checks, metrics.CheckResult{
			Name: "credential create", Status: "pass",
		})
	}

	pushChannel := tenantID + "." + pushTestChannel
	if err := provClient.SetPushChannels(ctx, tenantID, []string{pushChannel}, 2419200, "normal"); err != nil {
		checks = append(checks, metrics.CheckResult{
			Name: "channel config create", Status: "fail", Error: err.Error(),
		})
	} else {
		checks = append(checks, metrics.CheckResult{
			Name: "channel config create", Status: "pass",
		})
	}

	// Step 2: Register web subscription
	subID := uuid.NewString()[:8]
	deviceID, subErr := pushSubscribe(ctx, gwURL, token, pushSubscribeRequest{ //nolint:gosec // G101: fake test tokens for push validation
		Platform:   "web",
		Endpoint:   "https://push.example.com/test-" + subID,
		P256dhKey:  "BNcRdreALRFXTkOOUHK1EtK2wtaz5Ry4YfYCA_0QTpQtUbVlUls0VJXg7A8u-Ts1XbjhazAkj7I99e8p8aE48",
		AuthSecret: "tBHItJI5svbpC7CIKw9w2A",
		Channels:   []string{pushChannel},
	})
	if subErr != nil {
		checks = append(checks, metrics.CheckResult{
			Name: "push subscribe", Status: "fail", Error: subErr.Error(),
		})
	} else {
		deviceIDs = append(deviceIDs, deviceID)
		checks = append(checks, metrics.CheckResult{
			Name: "push subscribe", Status: "pass",
		})
	}

	// Step 3: Publish message — verify 200 accepted
	restClient := restpublish.NewClient(gwURL)
	msgPayload, _ := json.Marshal(map[string]any{ // json.Marshal on literal map of primitives cannot fail
		"msg_id": "push-test-" + uuid.NewString()[:8],
		"title":  "Test Push",
		"body":   "Push validation test message",
	})
	_, pubErr := restClient.Publish(ctx, restpublish.Request{
		Channel: pushChannel,
		Data:    msgPayload,
	}, restpublish.AuthConfig{Token: token})
	if pubErr != nil {
		checks = append(checks, metrics.CheckResult{
			Name: "push publish accepted", Status: "fail", Error: pubErr.Error(),
		})
	} else {
		checks = append(checks, metrics.CheckResult{
			Name: "push publish accepted", Status: "pass",
		})
	}

	// Step 4: Unregister subscription
	if deviceID != 0 {
		unsubErr := pushUnsubscribe(ctx, gwURL, token, deviceID)
		if unsubErr != nil {
			checks = append(checks, metrics.CheckResult{
				Name: "push unsubscribe", Status: "fail", Error: unsubErr.Error(),
			})
		} else {
			// Remove from cleanup list since we already unsubscribed
			deviceIDs = deviceIDs[:0]
			checks = append(checks, metrics.CheckResult{
				Name: "push unsubscribe", Status: "pass",
			})
		}
	}

	// Step 5: Channel config CRUD — create, get, delete, get (404)
	testChannel2 := tenantID + ".push-crud-test.*"
	if err := provClient.SetPushChannels(ctx, tenantID, []string{testChannel2}, 86400, "high"); err != nil {
		checks = append(checks, metrics.CheckResult{
			Name: "channel config crud create", Status: "fail", Error: err.Error(),
		})
	} else {
		checks = append(checks, metrics.CheckResult{
			Name: "channel config crud create", Status: "pass",
		})

		// Get — verify patterns match
		body, err := provClient.GetPushChannels(ctx, tenantID)
		if err != nil {
			checks = append(checks, metrics.CheckResult{
				Name: "channel config get", Status: "fail", Error: err.Error(),
			})
		} else {
			var cfg struct {
				Patterns []string `json:"patterns"`
			}
			if jsonErr := json.Unmarshal(body, &cfg); jsonErr != nil || len(cfg.Patterns) == 0 {
				checks = append(checks, metrics.CheckResult{
					Name: "channel config get", Status: "fail",
					Error: "unexpected response: " + string(body),
				})
			} else {
				checks = append(checks, metrics.CheckResult{
					Name: "channel config get", Status: "pass",
				})
			}
		}

		// Delete
		if err := provClient.DeletePushChannels(ctx, tenantID); err != nil {
			checks = append(checks, metrics.CheckResult{
				Name: "channel config delete", Status: "fail", Error: err.Error(),
			})
		} else {
			checks = append(checks, metrics.CheckResult{
				Name: "channel config delete", Status: "pass",
			})

			// Get after delete — expect error (404)
			_, err := provClient.GetPushChannels(ctx, tenantID)
			if err != nil {
				checks = append(checks, metrics.CheckResult{
					Name: "channel config delete verify", Status: "pass",
				})
			} else {
				checks = append(checks, metrics.CheckResult{
					Name: "channel config delete verify", Status: "fail",
					Error: "expected 404 after delete, got 200",
				})
			}
		}
	}

	// Step 6: Credential CRUD — create, delete
	testCreds := `{"public_key":"test_crud_pub","private_key":"test_crud_priv"}` //nolint:gosec // G101: fake test credentials for CRUD validation
	if err := provClient.SetPushCredentials(ctx, tenantID, "vapid", testCreds); err != nil {
		checks = append(checks, metrics.CheckResult{
			Name: "credential crud create", Status: "fail", Error: err.Error(),
		})
	} else {
		checks = append(checks, metrics.CheckResult{
			Name: "credential crud create", Status: "pass",
		})

		if err := provClient.DeletePushCredentials(ctx, tenantID, "vapid"); err != nil {
			checks = append(checks, metrics.CheckResult{
				Name: "credential delete", Status: "fail", Error: err.Error(),
			})
		} else {
			checks = append(checks, metrics.CheckResult{
				Name: "credential delete", Status: "pass",
			})
		}
	}

	// Step 7 (P2): Multi-platform — register web, android, ios
	multiPlatforms := []struct {
		name string
		req  pushSubscribeRequest
	}{
		{
			name: "multi-platform web",
			req: pushSubscribeRequest{ //nolint:gosec // G101: fake test tokens for multi-platform validation
				Platform:   "web",
				Endpoint:   "https://push.example.com/multi-" + uuid.NewString()[:8],
				P256dhKey:  "BNcRdreALRFXTkOOUHK1EtK2wtaz5Ry4YfYCA_0QTpQtUbVlUls0VJXg7A8u-Ts1XbjhazAkj7I99e8p8aE48",
				AuthSecret: "tBHItJI5svbpC7CIKw9w2A",
				Channels:   []string{pushChannel},
			},
		},
		{
			name: "multi-platform android",
			req: pushSubscribeRequest{
				Platform: "android",
				Token:    "fake-fcm-token-" + uuid.NewString()[:8],
				Channels: []string{pushChannel},
			},
		},
		{
			name: "multi-platform ios",
			req: pushSubscribeRequest{
				Platform: "ios",
				Token:    "fake-apns-token-" + uuid.NewString()[:8],
				Channels: []string{pushChannel},
			},
		},
	}

	// Re-create channel config for multi-platform test (was deleted in step 5)
	_ = provClient.SetPushChannels(ctx, tenantID, []string{pushChannel}, 2419200, "normal")
	_ = provClient.SetPushCredentials(ctx, tenantID, "vapid", vapidCreds)

	for _, mp := range multiPlatforms {
		id, err := pushSubscribe(ctx, gwURL, token, mp.req)
		switch {
		case err == nil:
			deviceIDs = append(deviceIDs, id)
			checks = append(checks, metrics.CheckResult{
				Name: mp.name, Status: "pass",
			})
		// FCM/APNs registration is Enterprise (MobilePush, ADR-0009) — on Pro
		// the per-platform gate returns 403 naming Enterprise. Discriminate on
		// the cause (edition gate), not on failure alone.
		case (mp.req.Platform == "android" || mp.req.Platform == "ios") &&
			strings.Contains(err.Error(), "HTTP 403") && strings.Contains(err.Error(), "Enterprise"):
			checks = append(checks, metrics.CheckResult{
				Name: mp.name, Status: "skip", Error: "mobile push requires Enterprise edition (403)",
			})
		default:
			checks = append(checks, metrics.CheckResult{
				Name: mp.name, Status: "fail", Error: err.Error(),
			})
		}
	}

	return checks, nil
}

// pushDeliveryTimeout bounds the wait for the push service to deliver a notification to the mock
// receiver. Generous (60s) because in kafka mode the ws-server consumer joins the tenant topic
// AtEnd only after a ~30s refreshTopicsLoop + rebalance, and delivery is then queued through the
// push worker — so the loop below re-publishes on a throttle until arrival (#144).
const pushDeliveryTimeout = 60 * time.Second

// verifyPushDelivery registers a device whose endpoint is the tester's own
// mock WebPush receiver, publishes to a push-routed channel, and asserts the
// push service delivers an encrypted notification to it (RFC 8030). Skips
// with an explicit reason when TESTER_PUSH_RECEIVER_HOST is unset (managed
// deployments where the tester is not reachable from the push service).
func verifyPushDelivery(ctx context.Context, run *TestRun, gwURL, token, tenantID string, logger zerolog.Logger) []metrics.CheckResult {
	if run.pushReceiverHost == "" {
		return []metrics.CheckResult{{
			Name: "push delivery", Status: "skip",
			Error: "receiver host not configured — set TESTER_PUSH_RECEIVER_HOST for delivery verification",
		}}
	}

	receiver, err := startPushReceiver(ctx, run.pushReceiverPort, logger)
	if err != nil {
		return []metrics.CheckResult{{Name: "push delivery", Status: "fail", Error: err.Error()}}
	}
	//nolint:contextcheck // Close uses its own bounded shutdown context deliberately: cleanup must run even when the check's ctx is already canceled (§IV).
	defer func() { _ = receiver.Close() }() // best-effort test cleanup

	// Real client crypto material: WebPush encryption (aes128gcm) needs a
	// genuine P-256 subscriber keypair and a 16-byte auth secret — fake
	// strings would make the push service's encryption step fail.
	clientKey, err := ecdh.P256().GenerateKey(rand.Reader)
	if err != nil {
		return []metrics.CheckResult{{Name: "push delivery", Status: "fail", Error: fmt.Sprintf("generate client key: %v", err)}}
	}
	authSecret := make([]byte, 16)
	if _, err := rand.Read(authSecret); err != nil {
		return []metrics.CheckResult{{Name: "push delivery", Status: "fail", Error: fmt.Sprintf("generate auth secret: %v", err)}}
	}

	deliveryChannel := tenantID + "." + pushTestChannel
	provClient := run.authResult.ProvClient
	if err := provClient.SetPushChannels(ctx, tenantID, []string{deliveryChannel}, 2419200, "normal"); err != nil {
		return []metrics.CheckResult{{Name: "push delivery", Status: "fail", Error: fmt.Sprintf("set push channels: %v", err)}}
	}

	// Unique endpoint path so the assertion matches THIS run's delivery only.
	pathToken := uuid.NewString()[:8]
	endpoint := fmt.Sprintf("http://%s:%d/push/%s", run.pushReceiverHost, receiver.Port(), pathToken)

	deviceID, subErr := pushSubscribe(ctx, gwURL, token, pushSubscribeRequest{
		Platform:   "web",
		Endpoint:   endpoint,
		P256dhKey:  base64.RawURLEncoding.EncodeToString(clientKey.PublicKey().Bytes()),
		AuthSecret: base64.RawURLEncoding.EncodeToString(authSecret),
		Channels:   []string{deliveryChannel},
	})
	if subErr != nil {
		return []metrics.CheckResult{{Name: "push delivery", Status: "fail", Error: fmt.Sprintf("subscribe: %v", subErr)}}
	}
	defer func() { _ = pushUnsubscribe(ctx, gwURL, token, deviceID) }() // best-effort test cleanup

	payload, _ := json.Marshal(map[string]any{ // json.Marshal on literal map of primitives cannot fail
		"msg_id": "push-delivery-" + pathToken,
		"title":  "Delivery Test",
	})
	restClient := restpublish.NewClient(gwURL)
	publishOnce := func() error {
		_, err := restClient.Publish(ctx, restpublish.Request{Channel: deliveryChannel, Data: payload}, restpublish.AuthConfig{Token: token})
		if err != nil {
			return fmt.Errorf("publish: %w", err)
		}
		return nil
	}
	// Publish immediately, then re-publish on a throttle until arrival or timeout. The ws-server
	// consumer joins the tenant topic AtEnd only after the topic-refresh + rebalance window, so a
	// single pre-assignment publish is lost; re-publishing tolerates that (#144). The first publish
	// must succeed (routing was warmed by waitForRoutable in setup) — later ones are best-effort.
	if err := publishOnce(); err != nil {
		return []metrics.CheckResult{{Name: "push delivery", Status: "fail", Error: err.Error()}}
	}

	deadline := time.After(pushDeliveryTimeout)
	republish := time.NewTicker(2 * time.Second)
	defer republish.Stop()
	check := time.NewTicker(200 * time.Millisecond)
	defer check.Stop()
	for {
		select {
		case <-ctx.Done():
			return []metrics.CheckResult{{Name: "push delivery", Status: "fail", Error: "context canceled while waiting for delivery"}}
		case <-deadline:
			return []metrics.CheckResult{{
				Name: "push delivery", Status: "fail",
				Error: fmt.Sprintf("no delivery to %s within %s (received %d unrelated requests)", endpoint, pushDeliveryTimeout, len(receiver.Requests())),
			}}
		case <-republish.C:
			_ = publishOnce() // best-effort re-publish; a transient failure is retried next tick
		case <-check.C:
			for _, req := range receiver.Requests() {
				if strings.Contains(req.Path, pathToken) && req.BodyLen > 0 {
					return []metrics.CheckResult{{Name: "push delivery", Status: "pass"}}
				}
			}
		}
	}
}
