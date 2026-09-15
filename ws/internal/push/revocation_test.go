package push

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/internal/push/repository"
	"github.com/sukko-dev/sukko/internal/shared/provapi"
)

// mockRevocationRepo implements repository.SubscriptionRepository for revocation testing.
type mockRevocationRepo struct {
	deletedByJTI map[string]int
	deletedBySub map[string]int
	subs         []repository.PushSubscription
}

func newMockRevocationRepo() *mockRevocationRepo {
	return &mockRevocationRepo{
		deletedByJTI: make(map[string]int),
		deletedBySub: make(map[string]int),
	}
}

func (m *mockRevocationRepo) Create(_ context.Context, sub *repository.PushSubscription) (int64, error) {
	m.subs = append(m.subs, *sub)
	return int64(len(m.subs)), nil
}

func (m *mockRevocationRepo) Delete(_ context.Context, _ int64, _ string) error  { return nil }
func (m *mockRevocationRepo) DeleteByToken(_ context.Context, _, _ string) error { return nil }
func (m *mockRevocationRepo) FindByTenant(_ context.Context, _ string) ([]repository.PushSubscription, error) {
	return m.subs, nil
}
func (m *mockRevocationRepo) UpdateLastSuccess(_ context.Context, _ int64) error { return nil }

func (m *mockRevocationRepo) DeleteByJTI(_ context.Context, _, jti string) (int, error) {
	m.deletedByJTI[jti]++
	return 1, nil
}

func (m *mockRevocationRepo) DeleteBySub(_ context.Context, tenantID, principal string, _ int64) (int, error) {
	key := tenantID + ":" + principal
	m.deletedBySub[key]++
	return 1, nil
}

func newTestServiceWithRepo(repo *mockRevocationRepo) *Service {
	return &Service{
		repo:   repo,
		logger: zerolog.Nop(),
	}
}

func TestHandleRevocation_ByJTI(t *testing.T) {
	t.Parallel()
	repo := newMockRevocationRepo()
	svc := newTestServiceWithRepo(repo)

	svc.HandleRevocation(provapi.RevocationEntry{
		TenantID: "acme",
		Type:     "token",
		JTI:      "tok-123",
	})

	if repo.deletedByJTI["tok-123"] != 1 {
		t.Errorf("expected 1 deletion for tok-123, got %d", repo.deletedByJTI["tok-123"])
	}
}

func TestHandleRevocation_BySub(t *testing.T) {
	t.Parallel()
	repo := newMockRevocationRepo()
	svc := newTestServiceWithRepo(repo)

	svc.HandleRevocation(provapi.RevocationEntry{
		TenantID:  "acme",
		Type:      "user",
		Sub:       "alice",
		RevokedAt: time.Now().Unix(),
	})

	if repo.deletedBySub["acme:alice"] != 1 {
		t.Errorf("expected 1 deletion for acme:alice, got %d", repo.deletedBySub["acme:alice"])
	}
}

func TestHandleRevocation_UnknownType(t *testing.T) {
	t.Parallel()
	repo := newMockRevocationRepo()
	svc := newTestServiceWithRepo(repo)

	// Should not panic or delete anything
	svc.HandleRevocation(provapi.RevocationEntry{
		TenantID: "acme",
		Type:     "unknown",
	})

	if len(repo.deletedByJTI) != 0 || len(repo.deletedBySub) != 0 {
		t.Error("unknown type should not trigger any deletions")
	}
}

func TestHandleRevocation_Idempotent(t *testing.T) {
	t.Parallel()
	repo := newMockRevocationRepo()
	svc := newTestServiceWithRepo(repo)

	entry := provapi.RevocationEntry{
		TenantID: "acme",
		Type:     "token",
		JTI:      "tok-dup",
	}

	svc.HandleRevocation(entry)
	svc.HandleRevocation(entry)

	if repo.deletedByJTI["tok-dup"] != 2 {
		t.Errorf("expected 2 deletions (idempotent), got %d", repo.deletedByJTI["tok-dup"])
	}
}
