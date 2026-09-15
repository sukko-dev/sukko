package gateway

import (
	"testing"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/time/rate"
)

// newTrackingTestProxy creates a Proxy configured for subscription tracking tests.
func newTrackingTestProxy() *Proxy {
	return &Proxy{
		logger:             zerolog.Nop(),
		subscribedChannels: make(map[string]struct{}),
		authLimiter:        rate.NewLimiter(rate.Every(30*time.Second), 1),
		publishLimiter:     rate.NewLimiter(10, 100),
		maxPublishSize:     64 * 1024,
	}
}

func TestTrackSubscriptionResponse_SubscriptionAck(t *testing.T) {
	t.Parallel()
	proxy := newTrackingTestProxy()

	payload := []byte(`{"type":"subscription_ack","subscribed":["sukko.BTC.trade","sukko.ETH.trade"],"count":2}`)
	proxy.trackSubscriptionResponse(payload)

	if len(proxy.subscribedChannels) != 2 {
		t.Fatalf("Expected 2 subscribed channels, got %d", len(proxy.subscribedChannels))
	}
	if _, ok := proxy.subscribedChannels["sukko.BTC.trade"]; !ok {
		t.Error("Expected sukko.BTC.trade in subscribedChannels")
	}
	if _, ok := proxy.subscribedChannels["sukko.ETH.trade"]; !ok {
		t.Error("Expected sukko.ETH.trade in subscribedChannels")
	}
}

func TestTrackSubscriptionResponse_UnsubscriptionAck(t *testing.T) {
	t.Parallel()
	proxy := newTrackingTestProxy()

	// Pre-populate
	proxy.subscribedChannels["sukko.BTC.trade"] = struct{}{}
	proxy.subscribedChannels["sukko.ETH.trade"] = struct{}{}

	payload := []byte(`{"type":"unsubscription_ack","unsubscribed":["sukko.BTC.trade"],"count":1}`)
	proxy.trackSubscriptionResponse(payload)

	if len(proxy.subscribedChannels) != 1 {
		t.Fatalf("Expected 1 subscribed channel, got %d", len(proxy.subscribedChannels))
	}
	if _, ok := proxy.subscribedChannels["sukko.BTC.trade"]; ok {
		t.Error("sukko.BTC.trade should have been removed")
	}
	if _, ok := proxy.subscribedChannels["sukko.ETH.trade"]; !ok {
		t.Error("sukko.ETH.trade should still be present")
	}
}

func TestTrackSubscriptionResponse_IgnoresBroadcast(t *testing.T) {
	t.Parallel()
	proxy := newTrackingTestProxy()

	// Broadcast message — no "_ack" in first 80 bytes, should be skipped
	payload := []byte(`{"type":"message","seq":1,"channel":"sukko.BTC.trade","data":{"price":50000}}`)
	proxy.trackSubscriptionResponse(payload)

	if len(proxy.subscribedChannels) != 0 {
		t.Errorf("Expected 0 subscribed channels after broadcast, got %d", len(proxy.subscribedChannels))
	}
}

func TestTrackSubscriptionResponse_EmptySubscription(t *testing.T) {
	t.Parallel()
	proxy := newTrackingTestProxy()

	// subscription_ack with empty channels list
	payload := []byte(`{"type":"subscription_ack","subscribed":[],"count":0}`)
	proxy.trackSubscriptionResponse(payload)

	if len(proxy.subscribedChannels) != 0 {
		t.Errorf("Expected 0 subscribed channels, got %d", len(proxy.subscribedChannels))
	}
}

func TestTrackSubscriptionResponse_DuplicateAdd(t *testing.T) {
	t.Parallel()
	proxy := newTrackingTestProxy()

	// First subscription
	payload := []byte(`{"type":"subscription_ack","subscribed":["sukko.BTC.trade"],"count":1}`)
	proxy.trackSubscriptionResponse(payload)

	// Duplicate subscription
	proxy.trackSubscriptionResponse(payload)

	if len(proxy.subscribedChannels) != 1 {
		t.Errorf("Expected 1 subscribed channel after duplicate add, got %d", len(proxy.subscribedChannels))
	}
}

func TestTrackSubscriptionResponse_RemoveNonexistent(t *testing.T) {
	t.Parallel()
	proxy := newTrackingTestProxy()

	// Unsubscribe from channel that was never subscribed
	payload := []byte(`{"type":"unsubscription_ack","unsubscribed":["sukko.BTC.trade"],"count":0}`)
	proxy.trackSubscriptionResponse(payload)

	if len(proxy.subscribedChannels) != 0 {
		t.Errorf("Expected 0 subscribed channels, got %d", len(proxy.subscribedChannels))
	}
}

func TestTrackSubscriptionResponse_IgnoresOtherAckTypes(t *testing.T) {
	t.Parallel()
	proxy := newTrackingTestProxy()

	// publish_ack contains "_ack" but should not affect tracking
	payload := []byte(`{"type":"publish_ack","channel":"sukko.BTC.trade","status":"accepted"}`)
	proxy.trackSubscriptionResponse(payload)

	if len(proxy.subscribedChannels) != 0 {
		t.Errorf("Expected 0 subscribed channels after publish_ack, got %d", len(proxy.subscribedChannels))
	}
}
