package worker

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/sukko-dev/sukko/internal/provisioning"
	broadcast "github.com/sukko-dev/sukko/internal/server/broadcast"
)

// ProvisioningClient is the subset of the provisioning gRPC service the worker needs.
// Defined as an interface so tests can inject mocks without a real gRPC connection.
type ProvisioningClient interface {
	// ListWebhookTenants returns all tenant IDs with at least one active webhook.
	// Called at startup for initial cache hydration.
	ListWebhookTenants(ctx context.Context) ([]string, error)

	// ListWebhooksForTenant returns all webhook registrations for a tenant.
	ListWebhooksForTenant(ctx context.Context, tenantID string) ([]*provisioning.WebhookRecord, error)

	// UpdateWebhookStatus transitions a webhook's status and resets retry_count.
	UpdateWebhookStatus(ctx context.Context, id, tenantID, status string, retryCount int) error

	// RecordDelivery writes a delivery attempt to the provisioning store.
	RecordDelivery(ctx context.Context, d *provisioning.WebhookDelivery) error
}

// HTTPDoer performs outbound HTTP requests. Wraps *http.Client for testability.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Subscriber receives broadcast messages for all tenants.
// Wraps the channel returned by broadcast.Bus.SubscribeAll().
type Subscriber interface {
	Messages() <-chan *broadcast.Message
	Close() error
}

// Resolver resolves hostnames to IP addresses. Wraps net.DefaultResolver for testability.
// net.DefaultResolver (*net.Resolver) satisfies this interface directly via its LookupHost method.
type Resolver interface {
	LookupHost(ctx context.Context, host string) ([]string, error)
}

// Clock abstracts time.Now for deterministic testing of retry/degraded schedules.
type Clock func() time.Time

// busSubscriber wraps a broadcast.Bus SubscribeAll channel to satisfy Subscriber.
// Usage: ch, err := bus.SubscribeAll(); sub := worker.NewBusSubscriber(bus, ch)
type busSubscriber struct {
	bus broadcast.Bus
	ch  <-chan *broadcast.Message
}

func (s *busSubscriber) Messages() <-chan *broadcast.Message { return s.ch }
func (s *busSubscriber) Close() error {
	if err := s.bus.UnsubscribeAll(s.ch); err != nil {
		return fmt.Errorf("unsubscribe broadcast bus: %w", err)
	}
	return nil
}

// NewBusSubscriber wraps the channel returned by bus.SubscribeAll() into a Subscriber.
func NewBusSubscriber(bus broadcast.Bus, ch <-chan *broadcast.Message) Subscriber {
	return &busSubscriber{bus: bus, ch: ch}
}
