package metrics

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/sukko-dev/sukko/cmd/tester/stats"
)

// Collector tracks test execution metrics with atomic operations.
type Collector struct {
	ConnectionsActive  atomic.Int64
	ConnectionsFailed  atomic.Int64
	ConnectionsTotal   atomic.Int64
	MessagesSent       atomic.Int64
	MessagesReceived   atomic.Int64
	MessagesDropped    atomic.Int64
	ErrorsTotal        atomic.Int64
	AuthRefreshTotal   atomic.Int64
	AuthRefreshFailed  atomic.Int64
	AuthErrors         atomic.Int64
	MessagesLost       atomic.Int64
	MessagesDuplicated atomic.Int64
	// Channel-mode metrics (load test with --channels)
	PublicSent          atomic.Int64
	PublicReceived      atomic.Int64
	UserScopedSent      atomic.Int64
	UserScopedReceived  atomic.Int64
	GroupScopedSent     atomic.Int64
	GroupScopedReceived atomic.Int64
	Misrouted           atomic.Int64
	// SSE + REST publish metrics (validation suites)
	SSEMessagesReceived atomic.Int64
	RESTPublishSuccess  atomic.Int64
	RESTPublishErrors   atomic.Int64
	// Auth mode metrics
	// Keyed off the actual credential used per-connection, not the global auth mode.
	ConnectionsAPIKey  atomic.Int64 // connections established using API key as initial credential
	ConnectionsJWT     atomic.Int64 // connections established using JWT as initial credential
	ConnectionsUpgrade atomic.Int64 // connections that completed the auth upgrade flow (auth_ack received)
	AuthUpgradeTotal   atomic.Int64 // total auth upgrade attempts (RefreshToken calls)
	AuthUpgradeFailed  atomic.Int64 // auth upgrade attempts that failed (auth_error or timeout)
	Latency            *stats.Histogram
	mu                 sync.RWMutex
	startTime          time.Time
}

// NewCollector creates a Collector with zeroed counters and a fresh start time.
func NewCollector() *Collector {
	return &Collector{
		Latency:   stats.NewHistogram(),
		startTime: time.Now(),
	}
}

// Snapshot is a serializable snapshot of current metrics.
type Snapshot struct {
	Timestamp           time.Time `json:"timestamp"`
	Elapsed             string    `json:"elapsed"`
	ConnectionsActive   int64     `json:"connections_active"`
	ConnectionsFailed   int64     `json:"connections_failed"`
	ConnectionsTotal    int64     `json:"connections_total"`
	MessagesSent        int64     `json:"messages_sent"`
	MessagesReceived    int64     `json:"messages_received"`
	MessagesDropped     int64     `json:"messages_dropped"`
	ErrorsTotal         int64     `json:"errors_total"`
	AuthRefreshTotal    int64     `json:"auth_refresh_total"`
	AuthRefreshFailed   int64     `json:"auth_refresh_failed"`
	AuthErrors          int64     `json:"auth_errors"`
	MessagesLost        int64     `json:"messages_lost,omitzero"`
	MessagesDuplicated  int64     `json:"messages_duplicated,omitzero"`
	PublicSent          int64     `json:"public_sent,omitzero"`
	PublicReceived      int64     `json:"public_received,omitzero"`
	UserScopedSent      int64     `json:"user_scoped_sent,omitzero"`
	UserScopedReceived  int64     `json:"user_scoped_received,omitzero"`
	GroupScopedSent     int64     `json:"group_scoped_sent,omitzero"`
	GroupScopedReceived int64     `json:"group_scoped_received,omitzero"`
	Misrouted           int64     `json:"misrouted,omitzero"`
	SSEMessagesReceived int64     `json:"sse_messages_received,omitzero"`
	RESTPublishSuccess  int64     `json:"rest_publish_success,omitzero"`
	RESTPublishErrors   int64     `json:"rest_publish_errors,omitzero"`
	// Auth mode metrics
	ConnectionsAPIKey  int64          `json:"connections_api_key,omitzero"`
	ConnectionsJWT     int64          `json:"connections_jwt,omitzero"`
	ConnectionsUpgrade int64          `json:"connections_upgrade,omitzero"`
	AuthUpgradeTotal   int64          `json:"auth_upgrade_total,omitzero"`
	AuthUpgradeFailed  int64          `json:"auth_upgrade_failed,omitzero"`
	Latency            stats.Snapshot `json:"latency"`
}

// Snapshot returns a point-in-time copy of all collected metrics.
func (c *Collector) Snapshot() Snapshot {
	c.mu.RLock()
	elapsed := time.Since(c.startTime).Round(time.Second).String()
	c.mu.RUnlock()

	return Snapshot{
		Timestamp:           time.Now(),
		Elapsed:             elapsed,
		ConnectionsActive:   c.ConnectionsActive.Load(),
		ConnectionsFailed:   c.ConnectionsFailed.Load(),
		ConnectionsTotal:    c.ConnectionsTotal.Load(),
		MessagesSent:        c.MessagesSent.Load(),
		MessagesReceived:    c.MessagesReceived.Load(),
		MessagesDropped:     c.MessagesDropped.Load(),
		ErrorsTotal:         c.ErrorsTotal.Load(),
		AuthRefreshTotal:    c.AuthRefreshTotal.Load(),
		AuthRefreshFailed:   c.AuthRefreshFailed.Load(),
		AuthErrors:          c.AuthErrors.Load(),
		MessagesLost:        c.MessagesLost.Load(),
		MessagesDuplicated:  c.MessagesDuplicated.Load(),
		PublicSent:          c.PublicSent.Load(),
		PublicReceived:      c.PublicReceived.Load(),
		UserScopedSent:      c.UserScopedSent.Load(),
		UserScopedReceived:  c.UserScopedReceived.Load(),
		GroupScopedSent:     c.GroupScopedSent.Load(),
		GroupScopedReceived: c.GroupScopedReceived.Load(),
		Misrouted:           c.Misrouted.Load(),
		SSEMessagesReceived: c.SSEMessagesReceived.Load(),
		RESTPublishSuccess:  c.RESTPublishSuccess.Load(),
		RESTPublishErrors:   c.RESTPublishErrors.Load(),
		ConnectionsAPIKey:   c.ConnectionsAPIKey.Load(),
		ConnectionsJWT:      c.ConnectionsJWT.Load(),
		ConnectionsUpgrade:  c.ConnectionsUpgrade.Load(),
		AuthUpgradeTotal:    c.AuthUpgradeTotal.Load(),
		AuthUpgradeFailed:   c.AuthUpgradeFailed.Load(),
		Latency:             c.Latency.Snapshot(),
	}
}

// Reset zeroes all counters and restarts the elapsed timer.
func (c *Collector) Reset() {
	c.ConnectionsActive.Store(0)
	c.ConnectionsFailed.Store(0)
	c.ConnectionsTotal.Store(0)
	c.MessagesSent.Store(0)
	c.MessagesReceived.Store(0)
	c.MessagesDropped.Store(0)
	c.ErrorsTotal.Store(0)
	c.AuthRefreshTotal.Store(0)
	c.AuthRefreshFailed.Store(0)
	c.AuthErrors.Store(0)
	c.MessagesLost.Store(0)
	c.MessagesDuplicated.Store(0)
	c.PublicSent.Store(0)
	c.PublicReceived.Store(0)
	c.UserScopedSent.Store(0)
	c.UserScopedReceived.Store(0)
	c.GroupScopedSent.Store(0)
	c.GroupScopedReceived.Store(0)
	c.Misrouted.Store(0)
	c.SSEMessagesReceived.Store(0)
	c.RESTPublishSuccess.Store(0)
	c.RESTPublishErrors.Store(0)
	c.ConnectionsAPIKey.Store(0)
	c.ConnectionsJWT.Store(0)
	c.ConnectionsUpgrade.Store(0)
	c.AuthUpgradeTotal.Store(0)
	c.AuthUpgradeFailed.Store(0)
	c.Latency.Reset()
	c.mu.Lock()
	c.startTime = time.Now()
	c.mu.Unlock()
}
