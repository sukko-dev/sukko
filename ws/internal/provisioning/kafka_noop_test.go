package provisioning

import (
	"context"
	"sync"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/logging"
)

func TestNoopKafkaAdmin_ImplementsInterface(t *testing.T) {
	t.Parallel()
	var _ KafkaAdmin = (*NoopKafkaAdmin)(nil)
}

func TestNoopKafkaAdmin_TopicOpsAreNoops(t *testing.T) {
	t.Parallel()
	admin := NewNoopKafkaAdmin()
	ctx := context.Background()

	// Physical topic creation is ws-server's job and existence is validated by
	// the durable TopicStore (ADR-0006 Phase 2), so the admin's topic ops are
	// genuine no-ops that only need to succeed.
	if err := admin.CreateTopic(ctx, "test-topic", 3, 1, nil); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if err := admin.DeleteTopic(ctx, "test-topic"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
}

func TestNoopKafkaAdmin_NoopMethods(t *testing.T) {
	t.Parallel()
	admin := NewNoopKafkaAdmin()
	ctx := context.Background()

	if err := admin.SetTopicConfig(ctx, "topic", map[string]string{"k": "v"}); err != nil {
		t.Errorf("SetTopicConfig: %v", err)
	}
	if err := admin.DeleteACL(ctx, ACLBinding{}); err != nil {
		t.Errorf("DeleteACL: %v", err)
	}
	if err := admin.SetQuota(ctx, "tenant", QuotaConfig{}); err != nil {
		t.Errorf("SetQuota: %v", err)
	}
	if err := admin.CreateACL(ctx, ACLBinding{Principal: "test"}); err != nil {
		t.Errorf("CreateACL: %v", err)
	}
	if err := admin.CreateTopicACLs(ctx, "acme", "test"); err != nil {
		t.Errorf("CreateTopicACLs: %v", err)
	}
	if err := admin.DeleteTopicACLs(ctx, "acme", "test"); err != nil {
		t.Errorf("DeleteTopicACLs: %v", err)
	}
	if err := admin.DeleteQuota(ctx, "acme", "test"); err != nil {
		t.Errorf("DeleteQuota: %v", err)
	}
}

func TestNoopKafkaAdmin_Concurrent(t *testing.T) {
	t.Parallel()
	admin := NewNoopKafkaAdmin()
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := range 10 {
		wg.Go(func() {
			defer logging.RecoverPanic(zerolog.Nop(), "test_noop_kafka_concurrent", nil)
			topic := "topic-" + string(rune('a'+i))
			_ = admin.CreateTopic(ctx, topic, 1, 1, nil)
			_ = admin.DeleteTopic(ctx, topic)
			_ = admin.CreateACL(ctx, ACLBinding{Principal: topic})
		})
	}
	wg.Wait()
}
