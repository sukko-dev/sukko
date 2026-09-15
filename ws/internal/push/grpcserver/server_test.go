package grpcserver_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"sync"
	"testing"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	provisioningv1 "github.com/sukko-dev/sukko/gen/proto/sukko/provisioning/v1"
	pushv1 "github.com/sukko-dev/sukko/gen/proto/sukko/push/v1"
	"github.com/sukko-dev/sukko/internal/push/grpcserver"
	"github.com/sukko-dev/sukko/internal/push/repository"
	"github.com/sukko-dev/sukko/internal/shared/license"
)

// ---------------------------------------------------------------------------
// Mock: SubscriptionRepository (in-memory map)
// ---------------------------------------------------------------------------

type mockRepo struct {
	mu     sync.Mutex
	subs   map[int64]*repository.PushSubscription
	nextID int64
}

func newMockRepo() *mockRepo {
	return &mockRepo{
		subs:   make(map[int64]*repository.PushSubscription),
		nextID: 1,
	}
}

func (r *mockRepo) Create(_ context.Context, sub *repository.PushSubscription) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	id := r.nextID
	r.nextID++
	sub.ID = id
	r.subs[id] = sub
	return id, nil
}

func (r *mockRepo) Delete(_ context.Context, id int64, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.subs, id)
	return nil
}

func (r *mockRepo) DeleteByToken(_ context.Context, _, _ string) error { return nil }

func (r *mockRepo) FindByTenant(_ context.Context, tenantID string) ([]repository.PushSubscription, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var result []repository.PushSubscription
	for _, sub := range r.subs {
		if sub.TenantID == tenantID {
			result = append(result, *sub)
		}
	}
	return result, nil
}

func (r *mockRepo) UpdateLastSuccess(_ context.Context, _ int64) error               { return nil }
func (r *mockRepo) DeleteByJTI(_ context.Context, _, _ string) (int, error)          { return 0, nil }
func (r *mockRepo) DeleteBySub(_ context.Context, _, _ string, _ int64) (int, error) { return 0, nil }

// ---------------------------------------------------------------------------
// Mock: ConfigCache
// ---------------------------------------------------------------------------

type mockConfigCache struct {
	credentials map[string]map[string]json.RawMessage // tenantID -> provider -> credential JSON
}

func newMockConfigCache() *mockConfigCache {
	return &mockConfigCache{
		credentials: make(map[string]map[string]json.RawMessage),
	}
}

func (m *mockConfigCache) GetCredential(tenantID, provider string) (json.RawMessage, error) {
	provCreds, ok := m.credentials[tenantID]
	if !ok {
		return nil, nil
	}
	cred, ok := provCreds[provider]
	if !ok {
		return nil, nil
	}
	return cred, nil
}

// setCredential sets a credential in the mock cache.
func (m *mockConfigCache) setCredential(tenantID, provider string, data json.RawMessage) {
	if _, ok := m.credentials[tenantID]; !ok {
		m.credentials[tenantID] = make(map[string]json.RawMessage)
	}
	m.credentials[tenantID][provider] = data
}

// ---------------------------------------------------------------------------
// Mock: ProvisioningInternalServiceClient
// ---------------------------------------------------------------------------

type storedCredential struct {
	TenantID       string
	Provider       string
	CredentialData string
}

type mockProvClient struct {
	provisioningv1.ProvisioningInternalServiceClient

	mu          sync.Mutex
	storedCreds []storedCredential
}

func (m *mockProvClient) StorePushCredentials(_ context.Context, req *provisioningv1.StorePushCredentialsRequest, _ ...grpc.CallOption) (*provisioningv1.StorePushCredentialsResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.storedCreds = append(m.storedCreds, storedCredential{
		TenantID:       req.GetTenantSlug(),
		Provider:       req.GetProvider(),
		CredentialData: req.GetCredentialData(),
	})
	return &provisioningv1.StorePushCredentialsResponse{}, nil
}

func (m *mockProvClient) getStoredCreds() []storedCredential {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]storedCredential, len(m.storedCreds))
	copy(cp, m.storedCreds)
	return cp
}

// ---------------------------------------------------------------------------
// Helper: create server with default mocks
// ---------------------------------------------------------------------------

type testEnv struct {
	server      *grpcserver.Server
	repo        *mockRepo
	configCache *mockConfigCache
	provClient  *mockProvClient
}

func setupTestServer(t *testing.T) *testEnv {
	t.Helper()

	repo := newMockRepo()
	cache := newMockConfigCache()
	prov := &mockProvClient{}

	srv, err := grpcserver.NewServer(grpcserver.ServerConfig{
		Repo:        repo,
		ConfigCache: cache,
		ProvClient:  prov,
		Logger:      zerolog.Nop(),
		Manager:     license.NewTestManager(license.Enterprise),
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	return &testEnv{
		server:      srv,
		repo:        repo,
		configCache: cache,
		provClient:  prov,
	}
}

// ---------------------------------------------------------------------------
// Tests: RegisterDevice
// ---------------------------------------------------------------------------

func TestRegisterDevice_WebPush(t *testing.T) {
	t.Parallel()

	env := setupTestServer(t)
	ctx := context.Background()

	resp, err := env.server.RegisterDevice(ctx, &pushv1.RegisterDeviceRequest{
		TenantSlug: "acme",
		Principal:  "user1",
		Platform:   "web",
		Endpoint:   "https://push.example.com/sub/abc",
		P256DhKey:  "BNcRdreALRFXTkOOUHK1EtK2wtaz5Ry4YfYCA_0QTpQtUbVlUls0VJXg7A8u-Ts1XbjhazAkj7I99e8p8v0w",
		AuthSecret: "tBHItJI5svnpDal0rdE10w",
		Channels:   []string{"acme.alerts.BTC", "acme.market.ETH"},
	})
	if err != nil {
		t.Fatalf("RegisterDevice() error = %v", err)
	}

	if resp.GetDeviceId() == 0 {
		t.Error("expected non-zero device_id")
	}

	// Verify persisted in repo.
	env.repo.mu.Lock()
	sub, ok := env.repo.subs[resp.GetDeviceId()]
	env.repo.mu.Unlock()
	if !ok {
		t.Fatal("subscription not found in repo")
	}
	if sub.Platform != "web" {
		t.Errorf("platform = %q, want %q", sub.Platform, "web")
	}
	if sub.Endpoint != "https://push.example.com/sub/abc" {
		t.Errorf("endpoint = %q, want %q", sub.Endpoint, "https://push.example.com/sub/abc")
	}
	if len(sub.Channels) != 2 {
		t.Errorf("channels count = %d, want 2", len(sub.Channels))
	}
}

func TestRegisterDevice_FCM(t *testing.T) {
	t.Parallel()

	env := setupTestServer(t)
	ctx := context.Background()

	resp, err := env.server.RegisterDevice(ctx, &pushv1.RegisterDeviceRequest{
		TenantSlug: "acme",
		Principal:  "user2",
		Platform:   "android",
		Token:      "fcm-token-abc123",
		Channels:   []string{"acme.notifications.general"},
	})
	if err != nil {
		t.Fatalf("RegisterDevice() error = %v", err)
	}

	if resp.GetDeviceId() == 0 {
		t.Error("expected non-zero device_id")
	}

	env.repo.mu.Lock()
	sub, ok := env.repo.subs[resp.GetDeviceId()]
	env.repo.mu.Unlock()
	if !ok {
		t.Fatal("subscription not found in repo")
	}
	if sub.Platform != "android" {
		t.Errorf("platform = %q, want %q", sub.Platform, "android")
	}
	if sub.Token != "fcm-token-abc123" {
		t.Errorf("token = %q, want %q", sub.Token, "fcm-token-abc123")
	}
}

func TestRegisterDevice_InvalidPlatform(t *testing.T) {
	t.Parallel()

	env := setupTestServer(t)
	ctx := context.Background()

	_, err := env.server.RegisterDevice(ctx, &pushv1.RegisterDeviceRequest{
		TenantSlug: "acme",
		Platform:   "unknown",
		Channels:   []string{"acme.alerts.BTC"},
	})
	if err == nil {
		t.Fatal("expected error for invalid platform")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %T", err)
	}
	if st.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want %v", st.Code(), codes.InvalidArgument)
	}
}

func TestRegisterDevice_MissingEndpoint(t *testing.T) {
	t.Parallel()

	env := setupTestServer(t)
	ctx := context.Background()

	_, err := env.server.RegisterDevice(ctx, &pushv1.RegisterDeviceRequest{
		TenantSlug: "acme",
		Platform:   "web",
		P256DhKey:  "some-key",
		AuthSecret: "some-secret",
		Channels:   []string{"acme.alerts.BTC"},
	})
	if err == nil {
		t.Fatal("expected error for missing endpoint")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %T", err)
	}
	if st.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want %v", st.Code(), codes.InvalidArgument)
	}
}

func TestRegisterDevice_MissingToken(t *testing.T) {
	t.Parallel()

	env := setupTestServer(t)
	ctx := context.Background()

	_, err := env.server.RegisterDevice(ctx, &pushv1.RegisterDeviceRequest{
		TenantSlug: "acme",
		Platform:   "android",
		Channels:   []string{"acme.alerts.BTC"},
	})
	if err == nil {
		t.Fatal("expected error for missing token")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %T", err)
	}
	if st.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want %v", st.Code(), codes.InvalidArgument)
	}
}

func TestRegisterDevice_InvalidChannelPrefix(t *testing.T) {
	t.Parallel()

	env := setupTestServer(t)
	ctx := context.Background()

	_, err := env.server.RegisterDevice(ctx, &pushv1.RegisterDeviceRequest{
		TenantSlug: "acme",
		Platform:   "web",
		Endpoint:   "https://push.example.com/sub/abc",
		P256DhKey:  "some-key",
		AuthSecret: "some-secret",
		Channels:   []string{"other-tenant.alerts.BTC"},
	})
	if err == nil {
		t.Fatal("expected error for invalid channel prefix")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %T", err)
	}
	if st.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want %v", st.Code(), codes.InvalidArgument)
	}
}

func TestRegisterDevice_EmptyChannels(t *testing.T) {
	t.Parallel()

	env := setupTestServer(t)
	ctx := context.Background()

	_, err := env.server.RegisterDevice(ctx, &pushv1.RegisterDeviceRequest{
		TenantSlug: "acme",
		Platform:   "web",
		Endpoint:   "https://push.example.com/sub/abc",
		P256DhKey:  "some-key",
		AuthSecret: "some-secret",
		Channels:   []string{},
	})
	if err == nil {
		t.Fatal("expected error for empty channels")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %T", err)
	}
	if st.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want %v", st.Code(), codes.InvalidArgument)
	}
}

// ---------------------------------------------------------------------------
// Tests: UnregisterDevice
// ---------------------------------------------------------------------------

func TestUnregisterDevice(t *testing.T) {
	t.Parallel()

	env := setupTestServer(t)
	ctx := context.Background()

	// Register first.
	regResp, err := env.server.RegisterDevice(ctx, &pushv1.RegisterDeviceRequest{
		TenantSlug: "acme",
		Principal:  "user1",
		Platform:   "web",
		Endpoint:   "https://push.example.com/sub/abc",
		P256DhKey:  "some-key",
		AuthSecret: "some-secret",
		Channels:   []string{"acme.alerts.BTC"},
	})
	if err != nil {
		t.Fatalf("RegisterDevice() error = %v", err)
	}

	deviceID := regResp.GetDeviceId()

	// Unregister.
	_, err = env.server.UnregisterDevice(ctx, &pushv1.UnregisterDeviceRequest{
		TenantSlug: "acme",
		DeviceId:   deviceID,
	})
	if err != nil {
		t.Fatalf("UnregisterDevice() error = %v", err)
	}

	// Verify removed from repo.
	env.repo.mu.Lock()
	_, exists := env.repo.subs[deviceID]
	env.repo.mu.Unlock()
	if exists {
		t.Error("subscription should have been deleted from repo")
	}
}

// ---------------------------------------------------------------------------
// Tests: GetVAPIDKey
// ---------------------------------------------------------------------------

func TestGetVAPIDKey_Cached(t *testing.T) {
	t.Parallel()

	env := setupTestServer(t)
	ctx := context.Background()

	// Pre-populate cache with a VAPID credential.
	vapidCred := `{"vapidPublicKey":"BExisting_VAPID_Key","vapidPrivateKey":"cHJpdmF0ZQ","vapidContact":"mailto:test@example.com"}`
	env.configCache.setCredential("acme", "vapid", json.RawMessage(vapidCred))

	resp, err := env.server.GetVAPIDKey(ctx, &pushv1.GetVAPIDKeyRequest{
		TenantSlug: "acme",
	})
	if err != nil {
		t.Fatalf("GetVAPIDKey() error = %v", err)
	}

	if resp.GetPublicKey() != "BExisting_VAPID_Key" {
		t.Errorf("public key = %q, want %q", resp.GetPublicKey(), "BExisting_VAPID_Key")
	}

	// Verify StorePushCredentials was NOT called (used cached value).
	creds := env.provClient.getStoredCreds()
	if len(creds) != 0 {
		t.Errorf("expected 0 StorePushCredentials calls, got %d", len(creds))
	}
}

func TestGetVAPIDKey_AutoGenerate(t *testing.T) {
	t.Parallel()

	env := setupTestServer(t)
	ctx := context.Background()

	// No VAPID credential in cache — should auto-generate.
	resp, err := env.server.GetVAPIDKey(ctx, &pushv1.GetVAPIDKeyRequest{
		TenantSlug: "acme",
	})
	if err != nil {
		t.Fatalf("GetVAPIDKey() error = %v", err)
	}

	publicKey := resp.GetPublicKey()
	if publicKey == "" {
		t.Fatal("expected non-empty public key")
	}

	// Verify the returned public key is valid base64url.
	decoded, err := base64.RawURLEncoding.DecodeString(publicKey)
	if err != nil {
		t.Fatalf("public key is not valid base64url: %v", err)
	}

	// Uncompressed EC P-256 public key should be 65 bytes (0x04 || X || Y).
	if len(decoded) != 65 {
		t.Errorf("decoded public key length = %d, want 65 (uncompressed P-256)", len(decoded))
	}
	if decoded[0] != 0x04 {
		t.Errorf("decoded public key prefix = 0x%02x, want 0x04 (uncompressed)", decoded[0])
	}

	// Verify StorePushCredentials was called exactly once.
	creds := env.provClient.getStoredCreds()
	if len(creds) != 1 {
		t.Fatalf("expected 1 StorePushCredentials call, got %d", len(creds))
	}
	if creds[0].TenantID != "acme" {
		t.Errorf("stored tenant_id = %q, want %q", creds[0].TenantID, "acme")
	}
	if creds[0].Provider != "vapid" {
		t.Errorf("stored provider = %q, want %q", creds[0].Provider, "vapid")
	}

	// Verify the stored credential JSON contains the same public key.
	var storedCred struct {
		PublicKey  string `json:"vapidPublicKey"`
		PrivateKey string `json:"vapidPrivateKey"`
		Contact    string `json:"vapidContact"`
	}
	if err := json.Unmarshal([]byte(creds[0].CredentialData), &storedCred); err != nil {
		t.Fatalf("unmarshal stored credential: %v", err)
	}
	if storedCred.PublicKey != publicKey {
		t.Errorf("stored public key = %q, want %q", storedCred.PublicKey, publicKey)
	}
	if storedCred.PrivateKey == "" {
		t.Error("stored private key should not be empty")
	}
	if storedCred.Contact == "" {
		t.Error("stored contact should not be empty")
	}
}

// ---------------------------------------------------------------------------
// Tests: NewServer validation
// ---------------------------------------------------------------------------

func TestNewServer_NilRepo(t *testing.T) {
	t.Parallel()

	_, err := grpcserver.NewServer(grpcserver.ServerConfig{
		Repo:        nil,
		ConfigCache: newMockConfigCache(),
		ProvClient:  &mockProvClient{},
		Logger:      zerolog.Nop(),
	})
	if err == nil {
		t.Fatal("expected error for nil repo")
	}
}

func TestNewServer_NilConfigCache(t *testing.T) {
	t.Parallel()

	_, err := grpcserver.NewServer(grpcserver.ServerConfig{
		Repo:        newMockRepo(),
		ConfigCache: nil,
		ProvClient:  &mockProvClient{},
		Logger:      zerolog.Nop(),
	})
	if err == nil {
		t.Fatal("expected error for nil config cache")
	}
}

func TestNewServer_NilProvClient(t *testing.T) {
	t.Parallel()

	_, err := grpcserver.NewServer(grpcserver.ServerConfig{
		Repo:        newMockRepo(),
		ConfigCache: newMockConfigCache(),
		ProvClient:  nil,
		Logger:      zerolog.Nop(),
	})
	if err == nil {
		t.Fatal("expected error for nil provisioning client")
	}
}

func TestNewServer_NilManager(t *testing.T) {
	t.Parallel()

	_, err := grpcserver.NewServer(grpcserver.ServerConfig{
		Repo:        newMockRepo(),
		ConfigCache: newMockConfigCache(),
		ProvClient:  &mockProvClient{},
		Logger:      zerolog.Nop(),
		// Manager intentionally nil
	})
	if err == nil {
		t.Fatal("expected error for nil manager")
	}
}

// ---------------------------------------------------------------------------
// Tests: Community edition gate (PermissionDenied on all handlers)
// ---------------------------------------------------------------------------

func TestRegisterDevice_CommunityEdition(t *testing.T) {
	t.Parallel()

	srv, err := grpcserver.NewServer(grpcserver.ServerConfig{
		Repo:        newMockRepo(),
		ConfigCache: newMockConfigCache(),
		ProvClient:  &mockProvClient{},
		Logger:      zerolog.Nop(),
		Manager:     license.NewTestManager(license.Community),
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	_, err = srv.RegisterDevice(context.Background(), &pushv1.RegisterDeviceRequest{
		TenantSlug: "t1",
		Platform:   "web",
		Endpoint:   "https://example.com/push/sub",
		P256DhKey:  "BNcRdreALRFXTkOOUHK1EtK2wtaz5Ry4YfYCA_0QTpQtUbVlUls0VJXg7A8u-Ts1XbjhazAkj7I99e8p8v0w",
		AuthSecret: "tBHItJI5svnpDal0rdE10w",
		Channels:   []string{"t1.alerts.BTC"},
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}
}

func TestUnregisterDevice_CommunityEdition(t *testing.T) {
	t.Parallel()

	srv, err := grpcserver.NewServer(grpcserver.ServerConfig{
		Repo:        newMockRepo(),
		ConfigCache: newMockConfigCache(),
		ProvClient:  &mockProvClient{},
		Logger:      zerolog.Nop(),
		Manager:     license.NewTestManager(license.Community),
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	_, err = srv.UnregisterDevice(context.Background(), &pushv1.UnregisterDeviceRequest{
		TenantSlug: "t1",
		DeviceId:   1,
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}
}

func TestGetVAPIDKey_CommunityEdition(t *testing.T) {
	t.Parallel()

	srv, err := grpcserver.NewServer(grpcserver.ServerConfig{
		Repo:        newMockRepo(),
		ConfigCache: newMockConfigCache(),
		ProvClient:  &mockProvClient{},
		Logger:      zerolog.Nop(),
		Manager:     license.NewTestManager(license.Community),
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	_, err = srv.GetVAPIDKey(context.Background(), &pushv1.GetVAPIDKeyRequest{
		TenantSlug: "t1",
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Tests: per-platform edition gates (ADR-0009: WebPush=Pro, MobilePush=Enterprise)
// ---------------------------------------------------------------------------

// proTestServer builds a Server with a Pro edition manager.
func proTestServer(t *testing.T) *grpcserver.Server {
	t.Helper()
	srv, err := grpcserver.NewServer(grpcserver.ServerConfig{
		Repo:        newMockRepo(),
		ConfigCache: newMockConfigCache(),
		ProvClient:  &mockProvClient{},
		Logger:      zerolog.Nop(),
		Manager:     license.NewTestManager(license.Pro),
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return srv
}

func TestRegisterDevice_ProEdition_WebAllowed(t *testing.T) {
	t.Parallel()

	srv := proTestServer(t)
	resp, err := srv.RegisterDevice(context.Background(), &pushv1.RegisterDeviceRequest{
		TenantSlug: "t1",
		Principal:  "user1",
		Platform:   "web",
		Endpoint:   "https://push.example.com/sub/pro",
		P256DhKey:  "BNcRdreALRFXTkOOUHK1EtK2wtaz5Ry4YfYCA_0QTpQtUbVlUls0VJXg7A8u-Ts1XbjhazAkj7I99e8p8v0w",
		AuthSecret: "tBHItJI5svnpDal0rdE10w",
		Channels:   []string{"t1.alerts.BTC"},
	})
	if err != nil {
		t.Fatalf("Pro edition must allow Web Push registration (WebPush is Pro per ADR-0009): %v", err)
	}
	if resp.GetDeviceId() == 0 {
		t.Error("expected non-zero device id")
	}
}

func TestRegisterDevice_ProEdition_MobileDenied(t *testing.T) {
	t.Parallel()

	srv := proTestServer(t)
	_, err := srv.RegisterDevice(context.Background(), &pushv1.RegisterDeviceRequest{
		TenantSlug: "t1",
		Principal:  "user1",
		Platform:   "android",
		Token:      "fcm-token-abc",
		Channels:   []string{"t1.alerts.BTC"},
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("Pro edition must deny FCM/APNs registration (MobilePush is Enterprise): got %v", err)
	}
}

func TestGetVAPIDKey_ProEditionAllowed(t *testing.T) {
	t.Parallel()

	srv := proTestServer(t)
	_, err := srv.GetVAPIDKey(context.Background(), &pushv1.GetVAPIDKeyRequest{
		TenantSlug: "t1",
	})
	if status.Code(err) == codes.PermissionDenied {
		t.Fatalf("Pro edition must allow GetVAPIDKey (WebPush is Pro): %v", err)
	}
}
