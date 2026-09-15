package provisioning

import (
	"context"
	"sync"
)

// NoopKafkaAdmin is a no-op implementation of KafkaAdmin for Phase 1.
// It records operations in memory but doesn't actually create Kafka resources.
// Thread-safe for concurrent use from HTTP handlers.
// Replace with real implementation in Phase 2.
type NoopKafkaAdmin struct {
	mu     sync.RWMutex
	topics map[string]bool
	acls   []ACLBinding
}

// NewNoopKafkaAdmin creates a new NoopKafkaAdmin.
func NewNoopKafkaAdmin() *NoopKafkaAdmin {
	return &NoopKafkaAdmin{
		topics: make(map[string]bool),
		acls:   []ACLBinding{},
	}
}

// CreateTopic records a topic creation (no-op).
func (n *NoopKafkaAdmin) CreateTopic(_ context.Context, name string, _ int, _ int16, _ map[string]string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.topics[name] = true
	return nil
}

// DeleteTopic records a topic deletion (no-op).
func (n *NoopKafkaAdmin) DeleteTopic(_ context.Context, name string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.topics, name)
	return nil
}

// TopicExists checks if a topic was recorded (no-op).
func (n *NoopKafkaAdmin) TopicExists(_ context.Context, name string) (bool, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	_, ok := n.topics[name]
	return ok, nil
}

// SetTopicConfig is a no-op.
func (n *NoopKafkaAdmin) SetTopicConfig(_ context.Context, _ string, _ map[string]string) error {
	return nil
}

// CreateACL records an ACL creation (no-op).
func (n *NoopKafkaAdmin) CreateACL(_ context.Context, acl ACLBinding) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.acls = append(n.acls, acl)
	return nil
}

// DeleteACL is a no-op.
func (n *NoopKafkaAdmin) DeleteACL(_ context.Context, _ ACLBinding) error {
	return nil
}

// SetQuota is a no-op.
func (n *NoopKafkaAdmin) SetQuota(_ context.Context, _ string, _ QuotaConfig) error {
	return nil
}

// CreateTopicACLs is a no-op.
func (n *NoopKafkaAdmin) CreateTopicACLs(_ context.Context, _, _ string) error {
	return nil
}

// DeleteTopicACLs is a no-op.
func (n *NoopKafkaAdmin) DeleteTopicACLs(_ context.Context, _, _ string) error {
	return nil
}

// DeleteQuota is a no-op.
func (n *NoopKafkaAdmin) DeleteQuota(_ context.Context, _, _ string) error {
	return nil
}

// Ensure NoopKafkaAdmin implements KafkaAdmin.
var _ KafkaAdmin = (*NoopKafkaAdmin)(nil)
