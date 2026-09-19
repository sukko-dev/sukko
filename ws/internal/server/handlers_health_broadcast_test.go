package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sukko-dev/sukko/internal/server/broadcast"
	"github.com/sukko-dev/sukko/internal/shared/alerting"
)

// healthTestBus implements broadcast.Bus with a configurable metrics snapshot,
// for handleHealth tests.
type healthTestBus struct {
	metrics broadcast.Metrics
}

func (b *healthTestBus) Subscribe(_ string) (<-chan *broadcast.Message, error) {
	return make(chan *broadcast.Message), nil
}
func (b *healthTestBus) SubscribeAll() (<-chan *broadcast.Message, error) {
	return make(chan *broadcast.Message), nil
}
func (b *healthTestBus) Unsubscribe(_ string, _ <-chan *broadcast.Message) error { return nil }
func (b *healthTestBus) UnsubscribeAll(_ <-chan *broadcast.Message) error        { return nil }
func (b *healthTestBus) Publish(_ *broadcast.Message) error                      { return nil }
func (b *healthTestBus) Run()                                                    {}
func (b *healthTestBus) Shutdown()                                               {}
func (b *healthTestBus) ShutdownWithContext(_ context.Context)                   {}
func (b *healthTestBus) IsHealthy() bool                                         { return b.metrics.Healthy }
func (b *healthTestBus) GetMetrics() broadcast.Metrics                           { return b.metrics }

// healthResponse is the subset of the /health body these tests assert on.
type healthResponse struct {
	Status  string `json:"status"`
	Healthy bool   `json:"healthy"`
	Checks  struct {
		Broadcast struct {
			Status                   string `json:"status"`
			Healthy                  bool   `json:"healthy"`
			PublishHealthy           bool   `json:"publish_healthy"`
			SubscriptionsConverged   bool   `json:"subscriptions_converged"`
			SubscriptionsDesired     int    `json:"subscriptions_desired"`
			SubscriptionsEstablished int    `json:"subscriptions_established"`
		} `json:"broadcast"`
	} `json:"checks"`
	Warnings []string `json:"warnings"`
	Errors   []string `json:"errors"`
}

func newHealthTestServer(t *testing.T, bus broadcast.Bus) *Server {
	t.Helper()
	params := newTestParams()
	params.BroadcastBus = bus
	srv, err := NewServer(params, &alerting.NoopAlerter{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	t.Cleanup(func() { _ = srv.Shutdown() })
	return srv
}

// TestHandleHealth_BroadcastNonConvergence_DegradedNotUnhealthy pins the
// ADR-0016 probe contract: subscription non-convergence is reported in the
// health body (degraded) but MUST NOT flip the health status to unhealthy —
// /health is the liveness probe, and failing it during a bus blip would
// restart pods fleet-wide. The readiness probe (/ready) never consults the
// bus at all and must stay 200.
func TestHandleHealth_BroadcastNonConvergence_DegradedNotUnhealthy(t *testing.T) {
	t.Parallel()
	bus := &healthTestBus{metrics: broadcast.Metrics{
		Type:                     "valkey",
		Healthy:                  false, // publish OK but not converged → IsHealthy false
		PublishHealthy:           true,
		SubscriptionsConverged:   false,
		SubscriptionsDesired:     5,
		SubscriptionsEstablished: 3,
	}}
	srv := newHealthTestServer(t, bus)

	rec := httptest.NewRecorder()
	srv.handleHealth(rec, httptest.NewRequest(http.MethodGet, "/health", http.NoBody))

	if rec.Code != http.StatusOK {
		t.Fatalf("/health status = %d, want %d (non-convergence is DEGRADED, must not fail the liveness probe)", rec.Code, http.StatusOK)
	}
	var body healthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Status != "degraded" {
		t.Errorf("status = %q, want %q", body.Status, "degraded")
	}
	if !body.Healthy {
		t.Error("healthy = false, want true (degraded is still healthy for the probe)")
	}
	bc := body.Checks.Broadcast
	if bc.Status != "degraded" {
		t.Errorf("checks.broadcast.status = %q, want %q", bc.Status, "degraded")
	}
	if bc.SubscriptionsConverged {
		t.Error("checks.broadcast.subscriptions_converged = true, want false")
	}
	if !bc.PublishHealthy {
		t.Error("checks.broadcast.publish_healthy = false, want true")
	}
	if bc.SubscriptionsDesired != 5 || bc.SubscriptionsEstablished != 3 {
		t.Errorf("checks.broadcast desired/established = %d/%d, want 5/3", bc.SubscriptionsDesired, bc.SubscriptionsEstablished)
	}
	if len(body.Warnings) == 0 {
		t.Error("warnings empty, want a broadcast non-convergence warning")
	}
	if len(body.Errors) != 0 {
		t.Errorf("errors = %v, want none (bus state must never be a liveness error)", body.Errors)
	}

	// Readiness must be untouched by bus state.
	recReady := httptest.NewRecorder()
	srv.handleReady(recReady, httptest.NewRequest(http.MethodGet, "/ready", http.NoBody))
	if recReady.Code != http.StatusOK {
		t.Fatalf("/ready status = %d, want %d (readiness must never consult the bus)", recReady.Code, http.StatusOK)
	}
}

// TestHandleHealth_BroadcastPublishUnhealthy_DegradedNotUnhealthy pins that a
// publish-side bus outage is likewise reported as degraded only: restarting or
// de-routing a pod cannot fix a Valkey outage, so the liveness probe must stay
// green while the body tells the truth.
func TestHandleHealth_BroadcastPublishUnhealthy_DegradedNotUnhealthy(t *testing.T) {
	t.Parallel()
	bus := &healthTestBus{metrics: broadcast.Metrics{
		Type:                   "valkey",
		Healthy:                false,
		PublishHealthy:         false,
		SubscriptionsConverged: true,
	}}
	srv := newHealthTestServer(t, bus)

	rec := httptest.NewRecorder()
	srv.handleHealth(rec, httptest.NewRequest(http.MethodGet, "/health", http.NoBody))

	if rec.Code != http.StatusOK {
		t.Fatalf("/health status = %d, want %d", rec.Code, http.StatusOK)
	}
	var body healthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Status != "degraded" {
		t.Errorf("status = %q, want %q", body.Status, "degraded")
	}
	if body.Checks.Broadcast.PublishHealthy {
		t.Error("checks.broadcast.publish_healthy = true, want false")
	}
	if len(body.Errors) != 0 {
		t.Errorf("errors = %v, want none", body.Errors)
	}
}

// TestHandleHealth_BroadcastHealthy_NoWarnings verifies a converged, healthy
// bus reports a healthy broadcast check and adds no warnings.
func TestHandleHealth_BroadcastHealthy_NoWarnings(t *testing.T) {
	t.Parallel()
	bus := &healthTestBus{metrics: broadcast.Metrics{
		Type:                     "valkey",
		Healthy:                  true,
		PublishHealthy:           true,
		SubscriptionsConverged:   true,
		SubscriptionsDesired:     4,
		SubscriptionsEstablished: 4,
	}}
	srv := newHealthTestServer(t, bus)

	rec := httptest.NewRecorder()
	srv.handleHealth(rec, httptest.NewRequest(http.MethodGet, "/health", http.NoBody))

	if rec.Code != http.StatusOK {
		t.Fatalf("/health status = %d, want %d", rec.Code, http.StatusOK)
	}
	var body healthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Status != "healthy" {
		t.Errorf("status = %q, want %q (warnings: %v)", body.Status, "healthy", body.Warnings)
	}
	if body.Checks.Broadcast.Status != "healthy" || !body.Checks.Broadcast.Healthy {
		t.Errorf("checks.broadcast = %+v, want healthy", body.Checks.Broadcast)
	}
	if len(body.Warnings) != 0 {
		t.Errorf("warnings = %v, want none", body.Warnings)
	}
}

// TestHandleHealth_NilBus_BroadcastNotConfigured verifies the broadcast check
// mirrors the backend check's not_configured convention (§XVIII) when no bus
// is wired (some test and tooling setups).
func TestHandleHealth_NilBus_BroadcastNotConfigured(t *testing.T) {
	t.Parallel()
	srv := newHealthTestServer(t, nil)

	rec := httptest.NewRecorder()
	srv.handleHealth(rec, httptest.NewRequest(http.MethodGet, "/health", http.NoBody))

	if rec.Code != http.StatusOK {
		t.Fatalf("/health status = %d, want %d", rec.Code, http.StatusOK)
	}
	var body healthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Checks.Broadcast.Status != "not_configured" {
		t.Errorf("checks.broadcast.status = %q, want %q", body.Checks.Broadcast.Status, "not_configured")
	}
	if body.Status != "healthy" {
		t.Errorf("status = %q, want %q", body.Status, "healthy")
	}
}
