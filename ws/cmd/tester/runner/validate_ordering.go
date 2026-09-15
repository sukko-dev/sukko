package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/auth"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
)

const orderingMessageCount = 20

// orderingMessageIDPrefix tags the measured sequence's message IDs so the checks can be isolated
// from delivery-warmup probe traffic (see measuredIDs / waitForDeliveryLive).
const orderingMessageIDPrefix = "order-"

// measuredIDs filters an arrival-order list to the measured sequence (order-* IDs), dropping any
// warmup-probe straggler that arrives after the tracker reset so it cannot inflate the received
// count or corrupt the FIFO-order check.
func measuredIDs(all []string) []string {
	out := make([]string, 0, len(all))
	for _, id := range all {
		if strings.HasPrefix(id, orderingMessageIDPrefix) {
			out = append(out, id)
		}
	}
	return out
}

func validateOrdering(ctx context.Context, run *TestRun, logger zerolog.Logger) ([]metrics.CheckResult, error) {
	provClient := run.authResult.ProvClient
	tenantID := run.authResult.TenantID

	// Setup: routing rules so publishes are routed to the bus.
	// Error ignored: if this fails, delivery checks below will fail with clear errors.
	_ = provClient.SetRoutingRules(ctx, tenantID, testRoutingRules)

	engine := NewPubSubEngine(PubSubEngineConfig{
		GatewayURL: run.Config.GatewayURL,
		Logger:     logger,
	})

	user, err := engine.CreateUser(ctx, run.authResult.Minter, auth.MintOptions{
		Subject: "ordering-test",
	})
	if err != nil {
		return []metrics.CheckResult{{Name: "create user", Status: "fail", Error: err.Error()}}, nil
	}
	defer func() { _ = user.Client.Close() }()

	channel := tenantChannel(tenantID, "general.test")
	// Confirmed subscribe (#242 class): a silently-filtered subscribe on a fresh tenant
	// would leave the warmup below republishing into a channel nobody is subscribed to —
	// the warmup retries publishes, not subscribes, so only SubscribeConfirmed unwedges it.
	if err := user.SubscribeConfirmed(ctx, []string{channel}, deliveryWarmupTimeout, deliveryWarmupInterval); err != nil {
		return []metrics.CheckResult{{Name: "subscribe", Status: "fail", Error: err.Error()}}, nil
	}

	// Warm up the delivery loop before the measured sequence. A fixed sleep is a direct-mode
	// assumption: in Kafka mode the ws-server consumer joins this freshly-provisioned tenant's topic
	// at AtEnd only after a cold-start propagation window, so early messages would be dropped. The
	// warmup republishes a probe until it round-trips (loop live), then clears the tracker.
	if err := waitForDeliveryLive(ctx, user, channel, logger); err != nil {
		return []metrics.CheckResult{{Name: "delivery warmup", Status: "fail", Error: err.Error()}}, nil
	}

	// Publish N messages in strict sequence
	for i := range orderingMessageCount {
		payload, _ := json.Marshal(map[string]any{ // json.Marshal on literal map of primitives cannot fail
			"msg_id": fmt.Sprintf(orderingMessageIDPrefix+"%04d", i),
			"ts":     time.Now().UnixMilli(),
		})
		if err := user.Client.Publish(channel, payload); err != nil {
			return []metrics.CheckResult{{
				Name: "publish sequence", Status: "fail",
				Error: fmt.Sprintf("publish msg %d: %v", i, err),
			}}, nil
		}
	}

	// Wait for all measured messages to arrive. Count only the measured IDs (order-*): a late
	// warmup-probe straggler delivered after the tracker reset MUST NOT inflate the count.
	deadline := time.After(10 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			goto check
		case <-ctx.Done():
			goto check
		case <-ticker.C:
			if len(measuredIDs(user.ReceivedOrder())) >= orderingMessageCount {
				time.Sleep(200 * time.Millisecond) // grace period for stragglers
				goto check
			}
		}
	}

check:
	received := measuredIDs(user.ReceivedOrder())
	checks := make([]metrics.CheckResult, 0, 2)

	if len(received) < orderingMessageCount {
		checks = append(checks, metrics.CheckResult{
			Name: "all messages received", Status: "fail",
			Error: fmt.Sprintf("received %d/%d", len(received), orderingMessageCount),
		})
	} else {
		checks = append(checks, metrics.CheckResult{
			Name: "all messages received", Status: "pass",
			Latency: fmt.Sprintf("%d/%d", len(received), orderingMessageCount),
		})
	}

	// Verify FIFO order: each message ID should be lexicographically >= the previous.
	// Works because format is "order-%04d" — lexicographic order == numeric order.
	inOrder := true
	firstOutOfOrder := ""
	for i := 1; i < len(received); i++ {
		if received[i] < received[i-1] {
			inOrder = false
			firstOutOfOrder = fmt.Sprintf("%s arrived after %s", received[i], received[i-1])
			break
		}
	}

	switch {
	case len(received) == 0:
		checks = append(checks, metrics.CheckResult{
			Name: "message ordering", Status: "fail", Error: "no messages received",
		})
	case inOrder:
		checks = append(checks, metrics.CheckResult{
			Name: "message ordering", Status: "pass",
			Latency: fmt.Sprintf("%d messages in FIFO order", len(received)),
		})
	default:
		checks = append(checks, metrics.CheckResult{
			Name: "message ordering", Status: "fail",
			Error: "out-of-order: " + firstOutOfOrder,
		})
	}

	return checks, nil
}
