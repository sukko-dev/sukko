package testutil

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/shared/types"
)

// MockTenantStore is an in-memory mock implementation of TenantStore.
// Tenants are indexed by both UUID (byUUID) and slug (bySlug) to support
// the dual-identity model introduced by the tenant-slug-separation feature.
type MockTenantStore struct {
	mu     sync.RWMutex
	byUUID map[string]*provisioning.Tenant
	bySlug map[string]*provisioning.Tenant

	// Error injection for testing error paths
	CreateErr             error
	GetBySlugErr          error
	UpdateErr             error
	UpdateSlugErr         error
	UpdateStatusErr       error
	SetDeprovisionAtErr   error
	SetRenameStateErr     error
	ClearRenameStateErr   error
	ListPendingRenamesErr error
	CountActiveErr        error
	PingErr               error
}

// NewMockTenantStore creates a new MockTenantStore.
func NewMockTenantStore() *MockTenantStore {
	return &MockTenantStore{
		byUUID: make(map[string]*provisioning.Tenant),
		bySlug: make(map[string]*provisioning.Tenant),
	}
}

// Ping implements TenantStore.Ping for testing.
func (m *MockTenantStore) Ping(_ context.Context) error {
	return m.PingErr
}

// Create implements TenantStore.Create for testing.
// Stores the tenant indexed by both UUID and slug.
// When tenant.ID is empty (simulating DB-generated UUID), a new UUID is assigned
// to the original tenant pointer — callers can read it back after Create returns.
func (m *MockTenantStore) Create(_ context.Context, tenant *provisioning.Tenant) error {
	if m.CreateErr != nil {
		return m.CreateErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if tenant.ID == "" {
		tenant.ID = uuid.New().String()
	}
	if _, exists := m.byUUID[tenant.ID]; exists {
		return errors.New("tenant already exists")
	}
	if _, exists := m.bySlug[tenant.Slug]; exists {
		return provisioning.ErrSlugAlreadyTaken
	}
	t := *tenant
	if t.CreatedAt.IsZero() {
		t.CreatedAt = time.Now()
	}
	if t.UpdatedAt.IsZero() {
		t.UpdatedAt = time.Now()
	}
	m.byUUID[t.ID] = &t
	m.bySlug[t.Slug] = &t
	return nil
}

// GetBySlug implements TenantStore.GetBySlug for testing.
// Returns a copy of the stored tenant so callers get a stable snapshot — mutations to
// the returned value (or to the stored record via UpdateSlug) do not alias each other.
func (m *MockTenantStore) GetBySlug(_ context.Context, slug string) (*provisioning.Tenant, error) {
	if m.GetBySlugErr != nil {
		return nil, m.GetBySlugErr
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.bySlug[slug]
	if !ok {
		return nil, provisioning.ErrTenantNotFound
	}
	clone := *t
	return &clone, nil
}

// Update implements TenantStore.Update for testing.
func (m *MockTenantStore) Update(_ context.Context, tenant *provisioning.Tenant) error {
	if m.UpdateErr != nil {
		return m.UpdateErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	existing, ok := m.byUUID[tenant.ID]
	if !ok {
		return provisioning.ErrTenantNotFound
	}
	tenant.UpdatedAt = time.Now()
	// Keep slug index consistent if slug hasn't changed.
	if existing.Slug != tenant.Slug {
		delete(m.bySlug, existing.Slug)
		m.bySlug[tenant.Slug] = tenant
	} else {
		m.bySlug[tenant.Slug] = tenant
	}
	m.byUUID[tenant.ID] = tenant
	return nil
}

// UpdateSlug implements TenantStore.UpdateSlug for testing.
func (m *MockTenantStore) UpdateSlug(_ context.Context, tenantUUID, newSlug string) (*provisioning.Tenant, error) {
	if m.UpdateSlugErr != nil {
		return nil, m.UpdateSlugErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.byUUID[tenantUUID]
	if !ok {
		return nil, provisioning.ErrTenantNotFound
	}
	if _, taken := m.bySlug[newSlug]; taken {
		return nil, provisioning.ErrSlugAlreadyTaken
	}
	oldSlug := t.Slug
	now := time.Now()
	t.PreviousSlug = oldSlug
	t.Slug = newSlug
	t.SlugRenameState = provisioning.SlugRenameStateComplete
	t.SlugRenamedAt = &now
	t.UpdatedAt = now
	delete(m.bySlug, oldSlug)
	m.bySlug[newSlug] = t
	return t, nil
}

// SetRenameState implements TenantStore.SetRenameState for testing.
// Returns ErrCASFailed if the current slug_rename_state is already "pending".
func (m *MockTenantStore) SetRenameState(_ context.Context, tenantUUID string, state provisioning.SlugRenameState, previousSlug string) error {
	if m.SetRenameStateErr != nil {
		return m.SetRenameStateErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.byUUID[tenantUUID]
	if !ok {
		return provisioning.ErrTenantNotFound
	}
	// CAS guard: only update when current state is NULL or 'complete'.
	if t.SlugRenameState == provisioning.SlugRenameStatePending {
		return provisioning.ErrCASFailed
	}
	t.SlugRenameState = state
	t.PreviousSlug = previousSlug
	t.UpdatedAt = time.Now()
	return nil
}

// ClearRenameState implements TenantStore.ClearRenameState for testing.
func (m *MockTenantStore) ClearRenameState(_ context.Context, tenantUUID string) error {
	if m.ClearRenameStateErr != nil {
		return m.ClearRenameStateErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.byUUID[tenantUUID]
	if !ok {
		return provisioning.ErrTenantNotFound
	}
	t.SlugRenameState = ""
	t.PreviousSlug = ""
	t.SlugRenamedAt = nil
	t.UpdatedAt = time.Now()
	return nil
}

// ListPendingRenames implements TenantStore.ListPendingRenames for testing.
func (m *MockTenantStore) ListPendingRenames(_ context.Context) ([]*provisioning.Tenant, error) {
	if m.ListPendingRenamesErr != nil {
		return nil, m.ListPendingRenamesErr
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*provisioning.Tenant
	for _, t := range m.byUUID {
		if t.SlugRenameState == provisioning.SlugRenameStatePending ||
			(t.SlugRenameState == provisioning.SlugRenameStateComplete && t.PreviousSlug != "") {
			result = append(result, t)
		}
	}
	return result, nil
}

// List implements TenantStore.List for testing.
func (m *MockTenantStore) List(_ context.Context, opts provisioning.ListOptions) ([]*provisioning.Tenant, int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*provisioning.Tenant, 0, len(m.byUUID))
	for _, t := range m.byUUID {
		if opts.Status != nil && t.Status != *opts.Status {
			continue
		}
		result = append(result, t)
	}
	total := len(result)
	if opts.Offset >= len(result) {
		return []*provisioning.Tenant{}, total, nil
	}
	end := min(opts.Offset+opts.Limit, len(result))
	return result[opts.Offset:end], total, nil
}

// validStatusSources mirrors TenantRepository.UpdateStatus's per-target WHERE
// guards. The mock MUST enforce the same transitions as the SQL: a permissive
// mock let TestService_DeprovisionTenant_Force_Active pass while the real
// repository rejected active→deleted (the force-delete tenant leak).
var validStatusSources = map[provisioning.TenantStatus][]provisioning.TenantStatus{
	provisioning.StatusSuspended:      {provisioning.StatusActive},
	provisioning.StatusActive:         {provisioning.StatusSuspended},
	provisioning.StatusDeprovisioning: {provisioning.StatusActive, provisioning.StatusSuspended},
	provisioning.StatusDeleted:        {provisioning.StatusActive, provisioning.StatusSuspended, provisioning.StatusDeprovisioning},
}

// UpdateStatus implements TenantStore.UpdateStatus for testing, enforcing the
// same status-transition guards as the real repository (see validStatusSources).
func (m *MockTenantStore) UpdateStatus(_ context.Context, tenantID string, status provisioning.TenantStatus) error {
	if m.UpdateStatusErr != nil {
		return m.UpdateStatusErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.byUUID[tenantID]
	if !ok {
		return provisioning.ErrTenantNotFound
	}
	// Disallowed transitions match 0 rows in SQL → the repository reports not-found.
	if !slices.Contains(validStatusSources[status], t.Status) {
		return fmt.Errorf("%w: %s", provisioning.ErrTenantNotFound, tenantID)
	}
	t.Status = status
	t.UpdatedAt = time.Now()
	if status == provisioning.StatusSuspended {
		now := time.Now()
		t.SuspendedAt = &now
	}
	return nil
}

// SetDeprovisionAt implements TenantStore.SetDeprovisionAt for testing.
func (m *MockTenantStore) SetDeprovisionAt(_ context.Context, tenantID string, deprovisionAt *provisioning.Time) error {
	if m.SetDeprovisionAtErr != nil {
		return m.SetDeprovisionAtErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.byUUID[tenantID]
	if !ok {
		return provisioning.ErrTenantNotFound
	}
	if deprovisionAt != nil {
		tt := *deprovisionAt
		t.DeprovisionAt = &tt
	}
	return nil
}

// GetTenantsForDeletion implements TenantStore.GetTenantsForDeletion for testing.
func (m *MockTenantStore) GetTenantsForDeletion(_ context.Context) ([]*provisioning.Tenant, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*provisioning.Tenant
	now := time.Now()
	for _, t := range m.byUUID {
		if t.Status == provisioning.StatusDeprovisioning && t.DeprovisionAt != nil && t.DeprovisionAt.Before(now) {
			result = append(result, t)
		}
	}
	return result, nil
}

// Count returns the number of non-deleted tenants in the mock store.
func (m *MockTenantStore) Count(_ context.Context) (int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	count := 0
	for _, t := range m.byUUID {
		if t.Status != provisioning.StatusDeleted {
			count++
		}
	}
	return count, nil
}

// CountActive returns the number of strictly active tenants in the mock store.
func (m *MockTenantStore) CountActive(_ context.Context) (int64, error) {
	if m.CountActiveErr != nil {
		return 0, m.CountActiveErr
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var count int64
	for _, t := range m.byUUID {
		if t.Status == provisioning.StatusActive {
			count++
		}
	}
	return count, nil
}

// MockKeyStore is an in-memory mock implementation of KeyStore.
type MockKeyStore struct {
	mu   sync.RWMutex
	keys map[string]*provisioning.TenantKey

	CreateErr             error
	GetErr                error
	RevokeErr             error
	RevokeAllForTenantErr error
}

// NewMockKeyStore creates a new MockKeyStore.
func NewMockKeyStore() *MockKeyStore {
	return &MockKeyStore{
		keys: make(map[string]*provisioning.TenantKey),
	}
}

// Create implements KeyStore.Create for testing.
func (m *MockKeyStore) Create(_ context.Context, key *provisioning.TenantKey) error {
	if m.CreateErr != nil {
		return m.CreateErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.keys[key.KeyID]; exists {
		return fmt.Errorf("%w: %s", provisioning.ErrKeyAlreadyExists, key.KeyID)
	}
	k := *key
	k.CreatedAt = time.Now()
	k.IsActive = true
	m.keys[key.KeyID] = &k
	return nil
}

// Get implements KeyStore.Get for testing.
func (m *MockKeyStore) Get(_ context.Context, keyID string) (*provisioning.TenantKey, error) {
	if m.GetErr != nil {
		return nil, m.GetErr
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	k, ok := m.keys[keyID]
	if !ok {
		return nil, provisioning.ErrKeyNotFound
	}
	return k, nil
}

// ListByTenant implements KeyStore.ListByTenant for testing.
func (m *MockKeyStore) ListByTenant(_ context.Context, tenantID string, opts provisioning.ListOptions) ([]*provisioning.TenantKey, int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var all []*provisioning.TenantKey
	for _, k := range m.keys {
		if k.TenantID == tenantID {
			all = append(all, k)
		}
	}
	total := len(all)

	// Apply pagination
	if opts.Offset >= len(all) {
		return []*provisioning.TenantKey{}, total, nil
	}
	all = all[opts.Offset:]
	if opts.Limit > 0 && opts.Limit < len(all) {
		all = all[:opts.Limit]
	}
	return all, total, nil
}

// Revoke implements KeyStore.Revoke for testing.
func (m *MockKeyStore) Revoke(_ context.Context, keyID string) error {
	if m.RevokeErr != nil {
		return m.RevokeErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	k, ok := m.keys[keyID]
	if !ok {
		return errors.New("key not found")
	}
	now := time.Now()
	k.IsActive = false
	k.RevokedAt = &now
	return nil
}

// RevokeAllForTenant implements KeyStore.RevokeAllForTenant for testing.
func (m *MockKeyStore) RevokeAllForTenant(_ context.Context, tenantID string) error {
	if m.RevokeAllForTenantErr != nil {
		return m.RevokeAllForTenantErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	for _, k := range m.keys {
		if k.TenantID == tenantID && k.IsActive {
			k.IsActive = false
			k.RevokedAt = &now
		}
	}
	return nil
}

// GetActiveKeys implements KeyStore.GetActiveKeys for testing.
func (m *MockKeyStore) GetActiveKeys(_ context.Context) ([]*provisioning.TenantKey, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*provisioning.TenantKey
	now := time.Now()
	for _, k := range m.keys {
		if k.IsActive && (k.ExpiresAt == nil || k.ExpiresAt.After(now)) && k.RevokedAt == nil {
			result = append(result, k)
		}
	}
	return result, nil
}

// MockAPIKeyStore is an in-memory mock implementation of APIKeyStore.
type MockAPIKeyStore struct {
	mu   sync.RWMutex
	keys map[string]*provisioning.APIKey

	CreateErr           error
	GetErr              error
	RevokeErr           error
	GetActiveAPIKeysErr error
}

// NewMockAPIKeyStore creates a new MockAPIKeyStore.
func NewMockAPIKeyStore() *MockAPIKeyStore {
	return &MockAPIKeyStore{
		keys: make(map[string]*provisioning.APIKey),
	}
}

// Create implements APIKeyStore.Create for testing.
func (m *MockAPIKeyStore) Create(_ context.Context, key *provisioning.APIKey) error {
	if m.CreateErr != nil {
		return m.CreateErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.keys[key.KeyID]; exists {
		return errors.New("api key already exists")
	}
	k := *key
	k.CreatedAt = time.Now()
	k.IsActive = true
	m.keys[key.KeyID] = &k
	return nil
}

// Get implements APIKeyStore.Get for testing.
func (m *MockAPIKeyStore) Get(_ context.Context, keyID string) (*provisioning.APIKey, error) {
	if m.GetErr != nil {
		return nil, m.GetErr
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	k, ok := m.keys[keyID]
	if !ok {
		return nil, provisioning.ErrAPIKeyNotFound
	}
	return k, nil
}

// ListByTenant implements APIKeyStore.ListByTenant for testing.
func (m *MockAPIKeyStore) ListByTenant(_ context.Context, tenantID string, opts provisioning.ListOptions) ([]*provisioning.APIKey, int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var all []*provisioning.APIKey
	for _, k := range m.keys {
		if k.TenantID == tenantID {
			all = append(all, k)
		}
	}
	total := len(all)

	// Apply pagination
	start := min(opts.Offset, len(all))
	end := min(start+opts.Limit, len(all))

	return all[start:end], total, nil
}

// Revoke implements APIKeyStore.Revoke for testing.
func (m *MockAPIKeyStore) Revoke(_ context.Context, keyID string) error {
	if m.RevokeErr != nil {
		return m.RevokeErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	k, ok := m.keys[keyID]
	if !ok {
		return provisioning.ErrAPIKeyNotFound
	}
	now := time.Now()
	k.IsActive = false
	k.RevokedAt = &now
	return nil
}

// GetActiveAPIKeys implements APIKeyStore.GetActiveAPIKeys for testing.
func (m *MockAPIKeyStore) GetActiveAPIKeys(_ context.Context) ([]*provisioning.APIKey, error) {
	if m.GetActiveAPIKeysErr != nil {
		return nil, m.GetActiveAPIKeysErr
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*provisioning.APIKey
	for _, k := range m.keys {
		if k.IsActive && k.RevokedAt == nil {
			result = append(result, k)
		}
	}
	return result, nil
}

// MockRoutingRulesStore is an in-memory mock implementation of RoutingRulesStore.
type MockRoutingRulesStore struct {
	mu    sync.RWMutex
	rules map[string][]provisioning.TopicRoutingRule // key: tenantID

	GetAllErr  error
	ReplaceErr error
	DeleteErr  error
	AddErr     error
}

// NewMockRoutingRulesStore creates a new MockRoutingRulesStore.
func NewMockRoutingRulesStore() *MockRoutingRulesStore {
	return &MockRoutingRulesStore{
		rules: make(map[string][]provisioning.TopicRoutingRule),
	}
}

// List implements RoutingRulesStore.List for testing.
func (m *MockRoutingRulesStore) List(_ context.Context, tenantID string, limit, offset int) ([]provisioning.TopicRoutingRule, int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	all := m.rules[tenantID]
	total := len(all)
	if offset >= total {
		return []provisioning.TopicRoutingRule{}, total, nil
	}
	end := min(offset+limit, total)
	return all[offset:end], total, nil
}

// Add implements RoutingRulesStore.Add for testing.
func (m *MockRoutingRulesStore) Add(_ context.Context, tenantID string, rule provisioning.TopicRoutingRule) error {
	if m.AddErr != nil {
		return m.AddErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.rules[tenantID] {
		if r.Priority == rule.Priority {
			return provisioning.ErrDuplicatePriority
		}
	}
	m.rules[tenantID] = append(m.rules[tenantID], rule)
	return nil
}

// Replace implements RoutingRulesStore.Replace for testing.
func (m *MockRoutingRulesStore) Replace(_ context.Context, tenantID string, rules []provisioning.TopicRoutingRule) error {
	if m.ReplaceErr != nil {
		return m.ReplaceErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rules[tenantID] = rules
	return nil
}

// DeleteAll implements RoutingRulesStore.DeleteAll for testing.
func (m *MockRoutingRulesStore) DeleteAll(_ context.Context, tenantID string) error {
	if m.DeleteErr != nil {
		return m.DeleteErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.rules, tenantID)
	return nil
}

// GetAll implements RoutingRulesStore.GetAll for testing.
func (m *MockRoutingRulesStore) GetAll(_ context.Context, tenantID string) ([]provisioning.TopicRoutingRule, error) {
	if m.GetAllErr != nil {
		return nil, m.GetAllErr
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	rules := m.rules[tenantID]
	if len(rules) == 0 {
		return nil, nil
	}
	result := make([]provisioning.TopicRoutingRule, len(rules))
	copy(result, rules)
	return result, nil
}

// Seed sets rules directly for test setup.
func (m *MockRoutingRulesStore) Seed(tenantID string, rules []provisioning.TopicRoutingRule) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rules[tenantID] = rules
}

// MockQuotaStore is an in-memory mock implementation of QuotaStore.
type MockQuotaStore struct {
	mu     sync.RWMutex
	quotas map[string]*provisioning.TenantQuota

	CreateErr error
	GetErr    error
	UpdateErr error
}

// NewMockQuotaStore creates a new MockQuotaStore.
func NewMockQuotaStore() *MockQuotaStore {
	return &MockQuotaStore{
		quotas: make(map[string]*provisioning.TenantQuota),
	}
}

// Create implements QuotaStore.Create for testing.
func (m *MockQuotaStore) Create(_ context.Context, quota *provisioning.TenantQuota) error {
	if m.CreateErr != nil {
		return m.CreateErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	q := *quota
	q.UpdatedAt = time.Now()
	m.quotas[quota.TenantID] = &q
	return nil
}

// Get implements QuotaStore.Get for testing.
func (m *MockQuotaStore) Get(_ context.Context, tenantID string) (*provisioning.TenantQuota, error) {
	if m.GetErr != nil {
		return nil, m.GetErr
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	q, ok := m.quotas[tenantID]
	if !ok {
		return nil, provisioning.ErrQuotaNotFound
	}
	return q, nil
}

// Update implements QuotaStore.Update for testing.
func (m *MockQuotaStore) Update(_ context.Context, quota *provisioning.TenantQuota) error {
	if m.UpdateErr != nil {
		return m.UpdateErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	quota.UpdatedAt = time.Now()
	m.quotas[quota.TenantID] = quota
	return nil
}

// MockAuditStore is an in-memory mock implementation of AuditStore.
type MockAuditStore struct {
	mu      sync.RWMutex
	entries []*provisioning.AuditEntry

	LogErr error
}

// NewMockAuditStore creates a new MockAuditStore.
func NewMockAuditStore() *MockAuditStore {
	return &MockAuditStore{
		entries: []*provisioning.AuditEntry{},
	}
}

// Log implements AuditStore.Log for testing.
func (m *MockAuditStore) Log(_ context.Context, entry *provisioning.AuditEntry) error {
	if m.LogErr != nil {
		return m.LogErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	e := *entry
	e.ID = int64(len(m.entries) + 1)
	e.CreatedAt = time.Now()
	m.entries = append(m.entries, &e)
	return nil
}

// ListByTenant implements AuditStore.ListByTenant for testing.
func (m *MockAuditStore) ListByTenant(_ context.Context, tenantID string, opts provisioning.ListOptions) ([]*provisioning.AuditEntry, int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*provisioning.AuditEntry
	for _, e := range m.entries {
		if e.TenantID == tenantID {
			result = append(result, e)
		}
	}
	total := len(result)
	if opts.Offset >= len(result) {
		return []*provisioning.AuditEntry{}, total, nil
	}
	end := min(opts.Offset+opts.Limit, len(result))
	return result[opts.Offset:end], total, nil
}

// GetEntries returns all recorded audit entries (for test assertions).
func (m *MockAuditStore) GetEntries() []*provisioning.AuditEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.entries
}

// MockKafkaAdmin is a mock implementation of KafkaAdmin.
type MockKafkaAdmin struct {
	mu                  sync.RWMutex
	topics              map[string]bool
	acls                []provisioning.ACLBinding
	quotas              map[string]provisioning.QuotaConfig
	CreatedTopicRFs     map[string]int16             // replication factor per topic name
	CreatedTopicParts   map[string]int               // partition count per topic name
	CreatedTopicConfigs map[string]map[string]string // topic config per topic name
	DeleteAttempts      []string                     // topics passed to DeleteTopic regardless of outcome
	DeletedTopics       []string                     // topics successfully deleted (DeleteTopicErr == nil)

	// CreateTopicACLsCalls, DeleteTopicACLsCalls, DeleteQuotaCalls, and SetQuotaCalls
	// record slug arguments for test assertions.
	CreateTopicACLsCalls []string
	DeleteTopicACLsCalls []string
	DeleteQuotaCalls     []string
	SetQuotaCalls        []string

	// CreateTopicErrs is a per-call error queue. Each call to CreateTopic pops the first element;
	// if the queue is empty, nil is returned. CreateTopicErr is used only when the queue is empty.
	CreateTopicErrs    []error
	CreateTopicErr     error
	DeleteTopicErr     error
	SetTopicConfigErr  error
	CreateACLErr       error
	SetQuotaErr        error
	CreateTopicACLsErr error
	DeleteTopicACLsErr error
	DeleteQuotaErr     error
}

// NewMockKafkaAdmin creates a new MockKafkaAdmin.
func NewMockKafkaAdmin() *MockKafkaAdmin {
	return &MockKafkaAdmin{
		topics:              make(map[string]bool),
		acls:                []provisioning.ACLBinding{},
		quotas:              make(map[string]provisioning.QuotaConfig),
		CreatedTopicRFs:     make(map[string]int16),
		CreatedTopicParts:   make(map[string]int),
		CreatedTopicConfigs: make(map[string]map[string]string),
	}
}

// CreateTopic implements KafkaAdmin.CreateTopic for testing.
func (m *MockKafkaAdmin) CreateTopic(_ context.Context, name string, partitions int, rf int16, cfg map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Pop from per-call queue first; fall back to the static error.
	if len(m.CreateTopicErrs) > 0 {
		err := m.CreateTopicErrs[0]
		m.CreateTopicErrs = m.CreateTopicErrs[1:]
		if err != nil {
			return err
		}
	} else if m.CreateTopicErr != nil {
		return m.CreateTopicErr
	}
	m.topics[name] = true
	m.CreatedTopicRFs[name] = rf
	m.CreatedTopicParts[name] = partitions
	cfgCopy := make(map[string]string, len(cfg))
	maps.Copy(cfgCopy, cfg)
	m.CreatedTopicConfigs[name] = cfgCopy
	return nil
}

// DeleteTopic implements KafkaAdmin.DeleteTopic for testing.
func (m *MockKafkaAdmin) DeleteTopic(_ context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.DeleteAttempts = append(m.DeleteAttempts, name)
	if m.DeleteTopicErr != nil {
		return m.DeleteTopicErr
	}
	delete(m.topics, name)
	m.DeletedTopics = append(m.DeletedTopics, name)
	return nil
}

// SetTopicConfig implements KafkaAdmin.SetTopicConfig for testing.
func (m *MockKafkaAdmin) SetTopicConfig(_ context.Context, _ string, _ map[string]string) error {
	if m.SetTopicConfigErr != nil {
		return m.SetTopicConfigErr
	}
	return nil
}

// CreateACL implements KafkaAdmin.CreateACL for testing.
func (m *MockKafkaAdmin) CreateACL(_ context.Context, acl provisioning.ACLBinding) error {
	if m.CreateACLErr != nil {
		return m.CreateACLErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acls = append(m.acls, acl)
	return nil
}

// DeleteACL implements KafkaAdmin.DeleteACL for testing.
func (m *MockKafkaAdmin) DeleteACL(_ context.Context, _ provisioning.ACLBinding) error {
	return nil
}

// SetQuota implements KafkaAdmin.SetQuota for testing.
func (m *MockKafkaAdmin) SetQuota(_ context.Context, tenantID string, quota provisioning.QuotaConfig) error {
	if m.SetQuotaErr != nil {
		return m.SetQuotaErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.quotas[tenantID] = quota
	m.SetQuotaCalls = append(m.SetQuotaCalls, tenantID)
	return nil
}

// CreateTopicACLs implements KafkaAdmin.CreateTopicACLs for testing.
func (m *MockKafkaAdmin) CreateTopicACLs(_ context.Context, slug, _ string) error {
	if m.CreateTopicACLsErr != nil {
		return m.CreateTopicACLsErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.CreateTopicACLsCalls = append(m.CreateTopicACLsCalls, slug)
	return nil
}

// DeleteTopicACLs implements KafkaAdmin.DeleteTopicACLs for testing.
func (m *MockKafkaAdmin) DeleteTopicACLs(_ context.Context, slug, _ string) error {
	if m.DeleteTopicACLsErr != nil {
		return m.DeleteTopicACLsErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.DeleteTopicACLsCalls = append(m.DeleteTopicACLsCalls, slug)
	return nil
}

// DeleteQuota implements KafkaAdmin.DeleteQuota for testing.
func (m *MockKafkaAdmin) DeleteQuota(_ context.Context, slug, _ string) error {
	if m.DeleteQuotaErr != nil {
		return m.DeleteQuotaErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.DeleteQuotaCalls = append(m.DeleteQuotaCalls, slug)
	return nil
}

// GetTopics returns all created topics (for test assertions).
func (m *MockKafkaAdmin) GetTopics() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]string, 0, len(m.topics))
	for name := range m.topics {
		result = append(result, name)
	}
	return result
}

// GetACLs returns all created ACLs (for test assertions).
func (m *MockKafkaAdmin) GetACLs() []provisioning.ACLBinding {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.acls
}

// MockChannelRulesStore is an in-memory mock implementation of ChannelRulesStore.
type MockChannelRulesStore struct {
	mu    sync.RWMutex
	rules map[string]*types.TenantChannelRules

	CreateErr error
	GetErr    error
	UpdateErr error
	DeleteErr error
}

// NewMockChannelRulesStore creates a new MockChannelRulesStore.
func NewMockChannelRulesStore() *MockChannelRulesStore {
	return &MockChannelRulesStore{rules: make(map[string]*types.TenantChannelRules)}
}

// Create stores channel rules for a tenant.
func (m *MockChannelRulesStore) Create(_ context.Context, tenantID string, rules *types.ChannelRules) error {
	if m.CreateErr != nil {
		return m.CreateErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rules[tenantID] = &types.TenantChannelRules{
		TenantID:  tenantID,
		Rules:     *rules,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	return nil
}

// Get returns channel rules for a tenant.
func (m *MockChannelRulesStore) Get(_ context.Context, tenantID string) (*types.TenantChannelRules, error) {
	if m.GetErr != nil {
		return nil, m.GetErr
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.rules[tenantID]
	if !ok {
		return nil, types.ErrChannelRulesNotFound
	}
	return r, nil
}

// GetRules returns just the rules portion for a tenant.
func (m *MockChannelRulesStore) GetRules(_ context.Context, tenantID string) (*types.ChannelRules, error) {
	if m.GetErr != nil {
		return nil, m.GetErr
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.rules[tenantID]
	if !ok {
		return nil, types.ErrChannelRulesNotFound
	}
	return &r.Rules, nil
}

// Update replaces channel rules for a tenant.
func (m *MockChannelRulesStore) Update(_ context.Context, tenantID string, rules *types.ChannelRules) error {
	if m.UpdateErr != nil {
		return m.UpdateErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	existing, ok := m.rules[tenantID]
	if ok {
		existing.Rules = *rules
		existing.UpdatedAt = now
	} else {
		m.rules[tenantID] = &types.TenantChannelRules{
			TenantID:  tenantID,
			Rules:     *rules,
			CreatedAt: now,
			UpdatedAt: now,
		}
	}
	return nil
}

// Delete removes channel rules for a tenant.
func (m *MockChannelRulesStore) Delete(_ context.Context, tenantID string) error {
	if m.DeleteErr != nil {
		return m.DeleteErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.rules[tenantID]; !ok {
		return types.ErrChannelRulesNotFound
	}
	delete(m.rules, tenantID)
	return nil
}

// List returns all stored channel rules.
func (m *MockChannelRulesStore) List(_ context.Context) ([]*types.TenantChannelRules, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*types.TenantChannelRules, 0, len(m.rules))
	for _, r := range m.rules {
		result = append(result, r)
	}
	return result, nil
}

// MockLicenseStateStore is an in-memory mock implementation of LicenseStateStore.
type MockLicenseStateStore struct {
	mu        sync.Mutex
	key       string
	UpsertErr error // injected error for Upsert
	LoadErr   error // injected error for Load
}

// NewMockLicenseStateStore creates a new MockLicenseStateStore.
func NewMockLicenseStateStore() *MockLicenseStateStore {
	return &MockLicenseStateStore{}
}

// Upsert stores the license key in memory.
func (m *MockLicenseStateStore) Upsert(_ context.Context, licenseKey, _, _ string, _ *time.Time) error {
	if m.UpsertErr != nil {
		return m.UpsertErr
	}
	m.mu.Lock()
	m.key = licenseKey
	m.mu.Unlock()
	return nil
}

// Load returns the stored license key.
func (m *MockLicenseStateStore) Load(_ context.Context) (string, error) {
	if m.LoadErr != nil {
		return "", m.LoadErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.key, nil
}

// MockWebhookStore is an in-memory implementation of provisioning.WebhookStore.
type MockWebhookStore struct {
	mu       sync.RWMutex
	webhooks map[string]*provisioning.Webhook // keyed by ID

	// Error injection
	CreateErr                error
	GetByIDErr               error
	ListErr                  error
	UpdateErr                error
	DeleteErr                error
	CountActiveErr           error
	ListTenantIDsErr         error
	ListByTenantForWorkerErr error
	UpdateStatusErr          error
	RecordDeliveryErr        error
	SuspendAllErr            error
	ListForDowngradeErr      error
}

// NewMockWebhookStore creates a new MockWebhookStore.
func NewMockWebhookStore() *MockWebhookStore {
	return &MockWebhookStore{webhooks: make(map[string]*provisioning.Webhook)}
}

// Create inserts a new webhook into the mock store.
func (m *MockWebhookStore) Create(_ context.Context, w *provisioning.Webhook) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.CreateErr != nil {
		return m.CreateErr
	}
	cp := *w
	m.webhooks[w.ID] = &cp
	return nil
}

// GetByID retrieves a webhook by ID scoped to the given tenantID.
func (m *MockWebhookStore) GetByID(_ context.Context, id, tenantID string) (*provisioning.Webhook, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.GetByIDErr != nil {
		return nil, m.GetByIDErr
	}
	w, ok := m.webhooks[id]
	if !ok || w.TenantID != tenantID {
		return nil, provisioning.ErrWebhookNotFound
	}
	cp := *w
	return &cp, nil
}

// List returns paginated webhooks for a tenant.
func (m *MockWebhookStore) List(_ context.Context, tenantID string, opts provisioning.ListOptions) ([]*provisioning.Webhook, int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.ListErr != nil {
		return nil, 0, m.ListErr
	}
	var all []*provisioning.Webhook
	for _, w := range m.webhooks {
		if w.TenantID == tenantID {
			cp := *w
			all = append(all, &cp)
		}
	}
	total := len(all)
	if opts.Limit == 0 {
		return []*provisioning.Webhook{}, total, nil
	}
	start := min(opts.Offset, total)
	end := min(start+opts.Limit, total)
	return all[start:end], total, nil
}

// Update applies a partial update from UpdateWebhookRequest.
func (m *MockWebhookStore) Update(_ context.Context, req provisioning.UpdateWebhookRequest) (*provisioning.Webhook, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.UpdateErr != nil {
		return nil, m.UpdateErr
	}
	w, ok := m.webhooks[req.ID]
	if !ok || w.TenantID != req.TenantID {
		return nil, provisioning.ErrWebhookNotFound
	}
	if req.URL != nil {
		w.URL = *req.URL
	}
	if req.ChannelPattern != nil {
		w.ChannelPattern = *req.ChannelPattern
	}
	if req.MaxRetries != nil {
		w.MaxRetries = *req.MaxRetries
	}
	if req.Status != nil {
		w.Status = *req.Status
		if *req.Status == types.WebhookStatusEnabled {
			w.RetryCount = 0
		}
	}
	cp := *w
	return &cp, nil
}

// Delete removes a webhook scoped to tenantID.
func (m *MockWebhookStore) Delete(_ context.Context, id, tenantID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.DeleteErr != nil {
		return m.DeleteErr
	}
	w, ok := m.webhooks[id]
	if !ok || w.TenantID != tenantID {
		return provisioning.ErrWebhookNotFound
	}
	delete(m.webhooks, id)
	return nil
}

// CountActive returns the number of non-suspended webhooks for a tenant.
func (m *MockWebhookStore) CountActive(_ context.Context, tenantID string) (int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.CountActiveErr != nil {
		return 0, m.CountActiveErr
	}
	count := 0
	for _, w := range m.webhooks {
		if w.TenantID == tenantID && w.Status != types.WebhookStatusSuspended {
			count++
		}
	}
	return count, nil
}

// ListTenantIDs returns all tenant IDs with at least one active webhook.
func (m *MockWebhookStore) ListTenantIDs(_ context.Context) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.ListTenantIDsErr != nil {
		return nil, m.ListTenantIDsErr
	}
	seen := make(map[string]struct{})
	for _, w := range m.webhooks {
		if w.Status != types.WebhookStatusSuspended {
			seen[w.TenantID] = struct{}{}
		}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	return ids, nil
}

// ListByTenantForWorker returns webhook records for worker cache hydration.
func (m *MockWebhookStore) ListByTenantForWorker(_ context.Context, tenantID string) ([]*provisioning.WebhookRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.ListByTenantForWorkerErr != nil {
		return nil, m.ListByTenantForWorkerErr
	}
	var recs []*provisioning.WebhookRecord
	for _, w := range m.webhooks {
		if w.TenantID == tenantID {
			// Decode base64 TEXT (Webhook.SecretEnc) to raw ciphertext bytes
			// (WebhookRecord.SecretEnc), mirroring WebhookRepository.ListByTenantForWorker.
			rawCiphertext, err := base64.StdEncoding.DecodeString(w.SecretEnc)
			if err != nil {
				return nil, fmt.Errorf("mock: decode secret_enc for webhook %s: %w", w.ID, err)
			}
			recs = append(recs, &provisioning.WebhookRecord{
				ID:             w.ID,
				TenantID:       w.TenantID,
				URL:            w.URL,
				ChannelPattern: w.ChannelPattern,
				SecretEnc:      rawCiphertext,
				Status:         w.Status,
				MaxRetries:     w.MaxRetries,
			})
		}
	}
	return recs, nil
}

// UpdateStatus transitions a webhook's status and sets retry_count.
func (m *MockWebhookStore) UpdateStatus(_ context.Context, id, tenantID, status string, retryCount int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.UpdateStatusErr != nil {
		return m.UpdateStatusErr
	}
	w, ok := m.webhooks[id]
	if !ok || w.TenantID != tenantID {
		return provisioning.ErrWebhookNotFound
	}
	w.Status = status
	w.RetryCount = retryCount
	return nil
}

// RecordDelivery records a webhook delivery attempt (no-op in mock; returns injected error).
func (m *MockWebhookStore) RecordDelivery(_ context.Context, _ *provisioning.WebhookDelivery) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.RecordDeliveryErr
}

// SuspendAllForDowngrade bulk-suspends all non-suspended webhooks in the mock store.
func (m *MockWebhookStore) SuspendAllForDowngrade(_ context.Context, _ time.Time) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.SuspendAllErr != nil {
		return 0, m.SuspendAllErr
	}
	n := 0
	for _, w := range m.webhooks {
		if w.Status != types.WebhookStatusSuspended {
			w.Status = types.WebhookStatusSuspended
			n++
		}
	}
	return n, nil
}

// ListForDowngrade returns all non-suspended webhooks eligible for suspension.
func (m *MockWebhookStore) ListForDowngrade(_ context.Context, _ time.Time) ([]*provisioning.Webhook, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.ListForDowngradeErr != nil {
		return nil, m.ListForDowngradeErr
	}
	var result []*provisioning.Webhook
	for _, w := range m.webhooks {
		if w.Status != types.WebhookStatusSuspended {
			cp := *w
			result = append(result, &cp)
		}
	}
	return result, nil
}

// MockTopicStore implements provisioning.TopicStore for testing (ADR-0006
// Phase 2). Create/List/Delete/Count are stateful (keyed by tenant+suffix) so
// topics-API tests exercise real behavior. Exists is decoupled: it returns
// ExistsResult (default true) so routing-rule validation tests need no per-test
// setup; set ExistsErr for the store-error path or ExistsResult=false for the
// not-provisioned path. The per-method *Err fields inject failures.
type MockTopicStore struct {
	mu           sync.Mutex
	created      map[string]map[string]time.Time // tenantID → suffix → createdAt
	ExistsResult bool
	ExistsErr    error
	CreateErr    error
	ListErr      error
	DeleteErr    error
	CountErr     error
}

// NewMockTopicStore returns a MockTopicStore that reports every topic as
// provisioned via Exists (ExistsResult=true) and starts with no created topics.
func NewMockTopicStore() *MockTopicStore {
	return &MockTopicStore{ExistsResult: true, created: make(map[string]map[string]time.Time)}
}

// Exists implements provisioning.TopicStore.Exists for testing.
func (m *MockTopicStore) Exists(_ context.Context, _, _ string) (bool, error) {
	if m.ExistsErr != nil {
		return false, m.ExistsErr
	}
	return m.ExistsResult, nil
}

// Create implements provisioning.TopicStore.Create for testing.
func (m *MockTopicStore) Create(_ context.Context, tenantID, suffix string) (time.Time, error) {
	if m.CreateErr != nil {
		return time.Time{}, m.CreateErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.created[tenantID] == nil {
		m.created[tenantID] = make(map[string]time.Time)
	}
	if _, ok := m.created[tenantID][suffix]; ok {
		return time.Time{}, fmt.Errorf("%w: %s", provisioning.ErrTopicAlreadyExists, suffix)
	}
	ts := time.Unix(0, 0)
	m.created[tenantID][suffix] = ts
	return ts, nil
}

// List implements provisioning.TopicStore.List for testing (ordered by suffix).
func (m *MockTopicStore) List(_ context.Context, tenantID string) ([]provisioning.Topic, error) {
	if m.ListErr != nil {
		return nil, m.ListErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	suffixes := make([]string, 0, len(m.created[tenantID]))
	for s := range m.created[tenantID] {
		suffixes = append(suffixes, s)
	}
	slices.Sort(suffixes)
	topics := make([]provisioning.Topic, 0, len(suffixes))
	for _, s := range suffixes {
		topics = append(topics, provisioning.Topic{Suffix: s, CreatedAt: m.created[tenantID][s]})
	}
	return topics, nil
}

// Delete implements provisioning.TopicStore.Delete for testing.
func (m *MockTopicStore) Delete(_ context.Context, tenantID, suffix string) error {
	if m.DeleteErr != nil {
		return m.DeleteErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.created[tenantID][suffix]; !ok {
		return fmt.Errorf("%w: %s", provisioning.ErrTopicNotFound, suffix)
	}
	delete(m.created[tenantID], suffix)
	return nil
}

// Count implements provisioning.TopicStore.Count for testing.
func (m *MockTopicStore) Count(_ context.Context, tenantID string) (int, error) {
	if m.CountErr != nil {
		return 0, m.CountErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.created[tenantID]), nil
}
