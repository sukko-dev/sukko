package provisioning

import (
	"context"
	"sync"
)

// NoopKafkaAdmin is a no-op implementation of KafkaAdmin. Physical topic
// creation is handled by ws-server's KafkaBackend, and topic-existence
// validation is now backed by the durable TopicStore (ADR-0006 Phase 2), so the
// admin's topic operations are genuine no-ops. Thread-safe for concurrent use.
type NoopKafkaAdmin struct {
	mu   sync.RWMutex
	acls []ACLBinding
}

// NewNoopKafkaAdmin creates a new NoopKafkaAdmin.
func NewNoopKafkaAdmin() *NoopKafkaAdmin {
	return &NoopKafkaAdmin{
		acls: []ACLBinding{},
	}
}

// CreateTopic is a no-op — ws-server's KafkaBackend creates physical topics.
func (n *NoopKafkaAdmin) CreateTopic(_ context.Context, _ string, _ int, _ int16, _ map[string]string) error {
	return nil
}

// DeleteTopic is a no-op.
func (n *NoopKafkaAdmin) DeleteTopic(_ context.Context, _ string) error {
	return nil
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
