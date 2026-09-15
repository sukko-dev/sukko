package repository_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/provisioning/repository"
	"github.com/sukko-dev/sukko/internal/shared/crypto"
	"github.com/sukko-dev/sukko/internal/shared/testutil"
	"github.com/sukko-dev/sukko/internal/shared/types"
)

// webhookTestEnv holds shared state for webhook repository tests.
type webhookTestEnv struct {
	pool     *pgxpool.Pool
	repo     *repository.WebhookRepository
	tenantID string
}

// newWebhookTestEnv creates a pool, runs migrations, creates a test tenant,
// and returns a ready-to-use environment.
func newWebhookTestEnv(t *testing.T) *webhookTestEnv {
	t.Helper()
	pool := testutil.NewTestPool(t)
	repo := repository.NewWebhookRepository(pool, zerolog.Nop())
	tenantID := mustCreateTenant(t, pool, "wh-env-"+uniqueSuffix(t))
	return &webhookTestEnv{pool: pool, repo: repo, tenantID: tenantID}
}

// mustCreateTenant inserts a tenant and returns its DB-generated UUID.
func mustCreateTenant(t *testing.T, pool *pgxpool.Pool, slug string) string {
	t.Helper()
	tenant := &provisioning.Tenant{
		Slug:         slug,
		Name:         "Test Tenant",
		Status:       provisioning.StatusActive,
		ConsumerType: provisioning.ConsumerShared,
	}
	if err := repository.NewTenantRepository(pool).Create(context.Background(), tenant); err != nil {
		t.Fatalf("create tenant %q: %v", slug, err)
	}
	return tenant.ID
}

// uniqueSuffix returns a short suffix derived from the test name, safe for use in slugs.
func uniqueSuffix(t *testing.T) string {
	// Use the last 8 chars of the test name to stay within postgres identifier limits.
	name := t.Name()
	if len(name) > 8 {
		name = name[len(name)-8:]
	}
	// Replace any characters not valid in a slug with underscores.
	out := make([]byte, len(name))
	for i, c := range name {
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
			out[i] = byte(c)
		} else {
			out[i] = 'x'
		}
	}
	return string(out)
}

func baseWebhook(tenantID, id string) *provisioning.Webhook {
	return &provisioning.Webhook{
		ID:             id,
		TenantID:       tenantID,
		URL:            "https://example.com/hook",
		ChannelPattern: "orders.*",
		SecretEnc:      "dGVzdA==", // valid base64 placeholder
		Status:         types.WebhookStatusEnabled,
		MaxRetries:     5,
	}
}

// --- Create + GetByID ---

func TestWebhookRepository_CreateAndGetByID(t *testing.T) {
	t.Parallel()
	env := newWebhookTestEnv(t)
	ctx := context.Background()

	w := baseWebhook(env.tenantID, "wh_crget0001")
	if err := env.repo.Create(ctx, w); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if w.CreatedAt.IsZero() {
		t.Error("Create() did not populate CreatedAt")
	}

	got, err := env.repo.GetByID(ctx, w.ID, env.tenantID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if got.URL != w.URL {
		t.Errorf("URL = %q, want %q", got.URL, w.URL)
	}
	if got.Status != types.WebhookStatusEnabled {
		t.Errorf("Status = %q, want %q", got.Status, types.WebhookStatusEnabled)
	}
}

func TestWebhookRepository_GetByID_NotFound(t *testing.T) {
	t.Parallel()
	env := newWebhookTestEnv(t)
	ctx := context.Background()

	_, err := env.repo.GetByID(ctx, "wh_notfound", env.tenantID)
	if !errors.Is(err, provisioning.ErrWebhookNotFound) {
		t.Errorf("GetByID() error = %v, want ErrWebhookNotFound", err)
	}
}

func TestWebhookRepository_GetByID_CrossTenantIsolation(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	repo := repository.NewWebhookRepository(pool, zerolog.Nop())
	ctx := context.Background()

	t1ID := mustCreateTenant(t, pool, "iso-a")
	t2ID := mustCreateTenant(t, pool, "iso-b")

	w := baseWebhook(t1ID, "wh_iso000001")
	if err := repo.Create(ctx, w); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Tenant B must not be able to read tenant A's webhook.
	_, err := repo.GetByID(ctx, w.ID, t2ID)
	if !errors.Is(err, provisioning.ErrWebhookNotFound) {
		t.Errorf("cross-tenant GetByID() error = %v, want ErrWebhookNotFound", err)
	}
}

// --- List ---

func TestWebhookRepository_List_Pagination(t *testing.T) {
	t.Parallel()
	env := newWebhookTestEnv(t)
	ctx := context.Background()

	for i := range 3 {
		w := baseWebhook(env.tenantID, fmt.Sprintf("wh_pg%04d", i))
		w.URL = fmt.Sprintf("https://example.com/hook%d", i)
		if err := env.repo.Create(ctx, w); err != nil {
			t.Fatalf("Create() #%d error = %v", i, err)
		}
	}

	page1, total, err := env.repo.List(ctx, env.tenantID, provisioning.ListOptions{Limit: 2, Offset: 0})
	if err != nil {
		t.Fatalf("List() page1 error = %v", err)
	}
	if total != 3 {
		t.Errorf("total = %d, want 3", total)
	}
	if len(page1) != 2 {
		t.Errorf("page1 len = %d, want 2", len(page1))
	}

	page2, _, err := env.repo.List(ctx, env.tenantID, provisioning.ListOptions{Limit: 2, Offset: 2})
	if err != nil {
		t.Fatalf("List() page2 error = %v", err)
	}
	if len(page2) != 1 {
		t.Errorf("page2 len = %d, want 1", len(page2))
	}
}

// --- Update ---

func TestWebhookRepository_Update_PartialFields(t *testing.T) {
	t.Parallel()
	env := newWebhookTestEnv(t)
	ctx := context.Background()

	w := baseWebhook(env.tenantID, "wh_upd00001")
	if err := env.repo.Create(ctx, w); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	newURL := "https://updated.example.com/hook"
	got, err := env.repo.Update(ctx, provisioning.UpdateWebhookRequest{
		ID:       w.ID,
		TenantID: env.tenantID,
		URL:      &newURL,
	})
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if got.URL != newURL {
		t.Errorf("URL = %q, want %q", got.URL, newURL)
	}
	// Unchanged fields must be preserved.
	if got.ChannelPattern != w.ChannelPattern {
		t.Errorf("ChannelPattern = %q, want %q (should be unchanged)", got.ChannelPattern, w.ChannelPattern)
	}
}

func TestWebhookRepository_Update_StatusEnabledResetsRetryCount(t *testing.T) {
	t.Parallel()
	env := newWebhookTestEnv(t)
	ctx := context.Background()

	w := baseWebhook(env.tenantID, "wh_rtry0001")
	if err := env.repo.Create(ctx, w); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	// Simulate degraded state with retry_count = 3.
	if err := env.repo.UpdateStatus(ctx, w.ID, env.tenantID, types.WebhookStatusDegraded, 3); err != nil {
		t.Fatalf("UpdateStatus() error = %v", err)
	}

	enabled := types.WebhookStatusEnabled
	got, err := env.repo.Update(ctx, provisioning.UpdateWebhookRequest{
		ID:       w.ID,
		TenantID: env.tenantID,
		Status:   &enabled,
	})
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if got.RetryCount != 0 {
		t.Errorf("RetryCount = %d after →enabled transition, want 0", got.RetryCount)
	}
}

func TestWebhookRepository_Update_NotFound(t *testing.T) {
	t.Parallel()
	env := newWebhookTestEnv(t)
	ctx := context.Background()

	url := "https://example.com"
	_, err := env.repo.Update(ctx, provisioning.UpdateWebhookRequest{
		ID:       "wh_nomatch",
		TenantID: env.tenantID,
		URL:      &url,
	})
	if !errors.Is(err, provisioning.ErrWebhookNotFound) {
		t.Errorf("Update() error = %v, want ErrWebhookNotFound", err)
	}
}

// --- Delete ---

func TestWebhookRepository_Delete(t *testing.T) {
	t.Parallel()
	env := newWebhookTestEnv(t)
	ctx := context.Background()

	w := baseWebhook(env.tenantID, "wh_del00001")
	if err := env.repo.Create(ctx, w); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if err := env.repo.Delete(ctx, w.ID, env.tenantID); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	_, err := env.repo.GetByID(ctx, w.ID, env.tenantID)
	if !errors.Is(err, provisioning.ErrWebhookNotFound) {
		t.Errorf("GetByID() after delete error = %v, want ErrWebhookNotFound", err)
	}
}

func TestWebhookRepository_Delete_CrossTenantProtection(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	repo := repository.NewWebhookRepository(pool, zerolog.Nop())
	ctx := context.Background()

	t1ID := mustCreateTenant(t, pool, "del-a")
	t2ID := mustCreateTenant(t, pool, "del-b")

	w := baseWebhook(t1ID, "wh_cdel0001")
	if err := repo.Create(ctx, w); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Wrong tenant must not delete.
	if err := repo.Delete(ctx, w.ID, t2ID); !errors.Is(err, provisioning.ErrWebhookNotFound) {
		t.Errorf("cross-tenant Delete() error = %v, want ErrWebhookNotFound", err)
	}

	// Webhook must still exist for tenant A.
	if _, err := repo.GetByID(ctx, w.ID, t1ID); err != nil {
		t.Errorf("webhook should still exist after failed cross-tenant delete: %v", err)
	}
}

// --- CountActive ---

func TestWebhookRepository_CountActive(t *testing.T) {
	t.Parallel()
	env := newWebhookTestEnv(t)
	ctx := context.Background()

	statuses := []string{types.WebhookStatusEnabled, types.WebhookStatusDegraded, types.WebhookStatusSuspended}
	for i, status := range statuses {
		w := baseWebhook(env.tenantID, fmt.Sprintf("wh_cnt%04d", i))
		if err := env.repo.Create(ctx, w); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		if err := env.repo.UpdateStatus(ctx, w.ID, env.tenantID, status, 0); err != nil {
			t.Fatalf("UpdateStatus() error = %v", err)
		}
	}

	count, err := env.repo.CountActive(ctx, env.tenantID)
	if err != nil {
		t.Fatalf("CountActive() error = %v", err)
	}
	// enabled(1) + degraded(1) = 2; suspended(1) excluded
	if count != 2 {
		t.Errorf("CountActive() = %d, want 2", count)
	}
}

// --- SuspendAllForDowngrade ---

func TestWebhookRepository_SuspendAllForDowngrade(t *testing.T) {
	t.Parallel()
	env := newWebhookTestEnv(t)
	ctx := context.Background()

	for i := range 3 {
		w := baseWebhook(env.tenantID, fmt.Sprintf("wh_sus%04d", i))
		if err := env.repo.Create(ctx, w); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
	}

	n, err := env.repo.SuspendAllForDowngrade(ctx, time.Now())
	if err != nil {
		t.Fatalf("SuspendAllForDowngrade() error = %v", err)
	}
	if n != 3 {
		t.Errorf("SuspendAllForDowngrade() = %d, want 3", n)
	}

	// Idempotent — second call affects 0 rows.
	n2, err := env.repo.SuspendAllForDowngrade(ctx, time.Now())
	if err != nil {
		t.Fatalf("SuspendAllForDowngrade() idempotent error = %v", err)
	}
	if n2 != 0 {
		t.Errorf("SuspendAllForDowngrade() idempotent = %d, want 0", n2)
	}
}

// --- ListTenantIDs ---

func TestWebhookRepository_ListTenantIDs(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	repo := repository.NewWebhookRepository(pool, zerolog.Nop())
	ctx := context.Background()

	t1ID := mustCreateTenant(t, pool, "lti-a")
	t2ID := mustCreateTenant(t, pool, "lti-b")

	// t1: 1 active webhook; t2: 1 active + 1 suspended.
	w1 := baseWebhook(t1ID, "wh_lti00001")
	w2 := baseWebhook(t2ID, "wh_lti00002")
	w3 := baseWebhook(t2ID, "wh_lti00003")
	for _, w := range []*provisioning.Webhook{w1, w2, w3} {
		if err := repo.Create(ctx, w); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
	}
	if err := repo.UpdateStatus(ctx, w3.ID, t2ID, types.WebhookStatusSuspended, 0); err != nil {
		t.Fatalf("UpdateStatus() error = %v", err)
	}

	ids, err := repo.ListTenantIDs(ctx)
	if err != nil {
		t.Fatalf("ListTenantIDs() error = %v", err)
	}
	idSet := make(map[string]bool)
	for _, id := range ids {
		idSet[id] = true
	}
	if !idSet[t1ID] {
		t.Errorf("ListTenantIDs() missing tenant %q", t1ID)
	}
	if !idSet[t2ID] {
		t.Errorf("ListTenantIDs() missing tenant %q", t2ID)
	}
}

// --- ListByTenantForWorker ---

func TestWebhookRepository_ListByTenantForWorker_SecretEncIsRawBytes(t *testing.T) {
	t.Parallel()
	env := newWebhookTestEnv(t)
	ctx := context.Background()

	// Store a real encrypted secret to verify the base64 decode in the repo.
	key, err := crypto.ParseEncryptionKey(testEncryptionKeyHex)
	if err != nil {
		t.Fatalf("ParseEncryptionKey: %v", err)
	}
	secretEnc, err := crypto.EncryptCredential("webhook-secret", key)
	if err != nil {
		t.Fatalf("EncryptCredential: %v", err)
	}

	w := baseWebhook(env.tenantID, "wh_wkr00001")
	w.SecretEnc = secretEnc
	if err := env.repo.Create(ctx, w); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	records, err := env.repo.ListByTenantForWorker(ctx, env.tenantID)
	if err != nil {
		t.Fatalf("ListByTenantForWorker() error = %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}

	// SecretEnc must be raw ciphertext (not base64) — verified by DecryptRaw.
	plaintext, err := crypto.DecryptRaw(records[0].SecretEnc, key)
	if err != nil {
		t.Fatalf("DecryptRaw() error = %v — SecretEnc must be raw bytes, not base64", err)
	}
	if plaintext != "webhook-secret" {
		t.Errorf("DecryptRaw() = %q, want %q", plaintext, "webhook-secret")
	}
}

// --- RecordDelivery ---

func TestWebhookRepository_RecordDelivery_PrunesOldEntries(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	repo := repository.NewWebhookRepository(pool, zerolog.Nop())
	ctx := context.Background()

	tenantID := mustCreateTenant(t, pool, "prune-tenant")
	w := baseWebhook(tenantID, "wh_prune0001")
	if err := repo.Create(ctx, w); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Insert 51 delivery records — after the 51st, the oldest must be pruned to ≤50.
	for i := range 51 {
		d := &provisioning.WebhookDelivery{
			ID:          fmt.Sprintf("del-%05d", i),
			WebhookID:   w.ID,
			TenantID:    tenantID,
			Attempt:     i + 1,
			StatusCode:  200,
			LatencyMS:   10,
			DeliveredAt: time.Now().Add(time.Duration(i) * time.Millisecond),
		}
		if err := repo.RecordDelivery(ctx, d); err != nil {
			t.Fatalf("RecordDelivery() #%d error = %v", i, err)
		}
	}

	var count int
	if err := pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM webhook_deliveries WHERE webhook_id = $1", w.ID,
	).Scan(&count); err != nil {
		t.Fatalf("count deliveries: %v", err)
	}
	if count > 50 {
		t.Errorf("delivery count = %d after 51 inserts, want ≤50 (prune should have run)", count)
	}
}

func TestWebhookRepository_RecordDelivery_UpdatesLastDelivery(t *testing.T) {
	t.Parallel()
	env := newWebhookTestEnv(t)
	ctx := context.Background()

	w := baseWebhook(env.tenantID, "wh_lastdel01")
	if err := env.repo.Create(ctx, w); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	d := &provisioning.WebhookDelivery{
		ID:          "del-upd-00001",
		WebhookID:   w.ID,
		TenantID:    env.tenantID,
		Attempt:     1,
		StatusCode:  200,
		LatencyMS:   42,
		DeliveredAt: time.Now().UTC().Truncate(time.Millisecond),
	}
	if err := env.repo.RecordDelivery(ctx, d); err != nil {
		t.Fatalf("RecordDelivery() error = %v", err)
	}

	got, err := env.repo.GetByID(ctx, w.ID, env.tenantID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if got.LastDeliveryAt == nil {
		t.Fatal("LastDeliveryAt is nil after RecordDelivery")
	}
	if got.LastStatus == nil || *got.LastStatus != "200" {
		t.Errorf("LastStatus = %v, want \"200\"", got.LastStatus)
	}
}

// TestWebhookRepository_RecordDelivery_Idempotent covers idempotency:
// inserting the same delivery_id twice must be a no-op on the second call,
// leaving exactly 1 row in webhook_deliveries.
func TestWebhookRepository_RecordDelivery_Idempotent(t *testing.T) {
	t.Parallel()
	env := newWebhookTestEnv(t)
	ctx := context.Background()

	w := baseWebhook(env.tenantID, "wh_idem01")
	if err := env.repo.Create(ctx, w); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	d := &provisioning.WebhookDelivery{
		ID:          "del-idem-00001",
		WebhookID:   w.ID,
		TenantID:    env.tenantID,
		Attempt:     1,
		StatusCode:  200,
		LatencyMS:   10,
		DeliveredAt: time.Now().UTC().Truncate(time.Millisecond),
	}

	// First insert — should succeed.
	if err := env.repo.RecordDelivery(ctx, d); err != nil {
		t.Fatalf("first RecordDelivery() error = %v", err)
	}

	// Second insert with the same ID — must be a no-op (not an error).
	if err := env.repo.RecordDelivery(ctx, d); err != nil {
		t.Fatalf("second RecordDelivery() (idempotent) error = %v", err)
	}

	// Verify exactly 1 row.
	var count int
	err := env.pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM webhook_deliveries WHERE id = $1", d.ID,
	).Scan(&count)
	if err != nil {
		t.Fatalf("count query error = %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 row for delivery id %s, got %d", d.ID, count)
	}
}
