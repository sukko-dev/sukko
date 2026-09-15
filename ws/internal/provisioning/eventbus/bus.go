// Package eventbus provides an in-process publish/subscribe event bus for
// provisioning service events. It delivers events to subscribers using
// non-blocking channel sends per Constitution VII fan-out rules.
package eventbus

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
)

// EventType identifies the kind of provisioning change that occurred.
type EventType string

const (
	// KeysChanged indicates that API keys were created, revoked, or modified.
	KeysChanged EventType = "keys_changed"

	// TenantConfigChanged indicates that tenant channel rules or routing rules changed.
	TenantConfigChanged EventType = "tenant_config_changed"

	// TopicsChanged indicates that topics or tenant categories changed.
	TopicsChanged EventType = "topics_changed"

	// APIKeysChanged indicates that API keys were created or revoked.
	APIKeysChanged EventType = "api_keys_changed"

	// PushConfigChanged indicates that push credentials or channel configs changed.
	PushConfigChanged EventType = "push_config_changed"

	// AdminKeysChanged indicates that admin keys were registered or revoked.
	AdminKeysChanged EventType = "admin_keys_changed"

	// LicenseChanged indicates that the license key was reloaded.
	LicenseChanged EventType = "license_changed"

	// TokenRevocationsChanged indicates that a token was revoked or a revocation expired.
	TokenRevocationsChanged EventType = "token_revocations_changed"

	// subscriberBufferSize is the capacity of each subscriber's event channel.
	// Buffered to absorb short bursts without blocking the publisher.
	subscriberBufferSize = 16
)

// Event represents a provisioning change notification.
type Event struct {
	Type EventType
}

// Prometheus metrics for the event bus.
var (
	eventsPublished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "provisioning_event_bus_events_total",
		Help: "Total events published by the event bus",
	}, []string{"type"})

	eventsDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "provisioning_event_bus_dropped_total",
		Help: "Total events dropped due to slow subscribers",
	}, []string{"type"})
)

// Bus is an in-process event bus that broadcasts provisioning change
// events to registered subscribers. It uses non-blocking sends to
// skip slow subscribers, preventing a single slow consumer from
// blocking delivery to all others.
type Bus struct {
	mu          sync.RWMutex
	subscribers map[int]chan Event
	nextID      int
	logger      zerolog.Logger
}

// New creates a new event bus.
func New(logger zerolog.Logger) *Bus {
	return &Bus{
		subscribers: make(map[int]chan Event),
		logger:      logger.With().Str("component", "event_bus").Logger(),
	}
}

// Subscribe registers a new subscriber and returns its ID and event channel.
// The caller must call Unsubscribe when done to prevent resource leaks.
func (b *Bus) Subscribe() (id int, ch <-chan Event) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id = b.nextID
	b.nextID++

	bidiCh := make(chan Event, subscriberBufferSize)
	b.subscribers[id] = bidiCh
	ch = bidiCh

	b.logger.Debug().Int("subscriber_id", id).Msg("subscriber registered")

	return id, ch
}

// Unsubscribe removes a subscriber and closes its channel.
func (b *Bus) Unsubscribe(id int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	ch, ok := b.subscribers[id]
	if !ok {
		return
	}

	delete(b.subscribers, id)
	close(ch)

	b.logger.Debug().Int("subscriber_id", id).Msg("subscriber unregistered")
}

// Publish sends an event to all subscribers using non-blocking sends.
// Slow subscribers that have full buffers will have the event dropped
// and a metric incremented. This ensures a single slow subscriber
// cannot block delivery to all others.
func (b *Bus) Publish(event Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	eventsPublished.WithLabelValues(string(event.Type)).Inc()

	for id, ch := range b.subscribers {
		select {
		case ch <- event:
		default:
			// Subscriber buffer full — drop event per Constitution VII fan-out rules.
			eventsDropped.WithLabelValues(string(event.Type)).Inc()
			b.logger.Warn().
				Int("subscriber_id", id).
				Str("event_type", string(event.Type)).
				Msg("event dropped: subscriber buffer full")
		}
	}
}

// SubscriberCount returns the current number of subscribers.
func (b *Bus) SubscriberCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subscribers)
}
