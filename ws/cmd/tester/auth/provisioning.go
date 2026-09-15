package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// provisioningTimeout is the HTTP client timeout for provisioning API calls.
const provisioningTimeout = 10 * time.Second

// maxResponseBody is the maximum response body size read for error messages.
const maxResponseBody = 4096

// RegisterKeyRequest is the request body for registering a public key.
type RegisterKeyRequest struct {
	KeyID     string     `json:"key_id"`
	Algorithm string     `json:"algorithm"`
	PublicKey string     `json:"public_key"`
	ExpiresAt *time.Time `json:"expires_at,omitzero"`
}

// ProvisioningClient is an HTTP client for the provisioning API.
// Admin auth is handled by the Provider (signs each request with an admin JWT).
// Tenant auth (GetTenant) uses a per-call JWT parameter — not the Provider.
type ProvisioningClient struct {
	baseURL      string
	authProvider Provider
	httpClient   *http.Client
	logger       zerolog.Logger
}

// NewProvisioningClient creates a new provisioning API client.
// The authProvider signs every admin request with a JWT. Pass nil to skip auth
// (only valid for unauthenticated endpoints like /edition).
func NewProvisioningClient(baseURL string, authProvider Provider, logger zerolog.Logger) *ProvisioningClient {
	return &ProvisioningClient{
		baseURL:      baseURL,
		authProvider: authProvider,
		httpClient:   &http.Client{Timeout: provisioningTimeout},
		logger:       logger.With().Str("component", "provisioning_client").Logger(),
	}
}

// AuthProvider returns the underlying auth provider for direct HTTP calls
// that don't go through ProvisioningClient's API methods (e.g., license reload).
func (c *ProvisioningClient) AuthProvider() Provider {
	return c.authProvider
}

// CreateTenant creates a new tenant via the provisioning API.
func (c *ProvisioningClient) CreateTenant(ctx context.Context, tenantID, name string) error {
	body, err := json.Marshal(map[string]string{
		"slug":          tenantID,
		"name":          name,
		"consumer_type": "shared",
	})
	if err != nil {
		return fmt.Errorf("create tenant: marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v1/tenants", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create tenant: build request: %w", err)
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("create tenant: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return c.readError("create tenant", resp)
	}

	c.logger.Info().Str(logging.LogKeyTenantSlug, tenantID).Msg("tenant created")
	return nil
}

// DeleteTenant deletes a tenant via the provisioning API with force=true.
func (c *ProvisioningClient) DeleteTenant(ctx context.Context, tenantID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.baseURL+"/api/v1/tenants/"+tenantID+"?force=true", http.NoBody)
	if err != nil {
		return fmt.Errorf("delete tenant: build request: %w", err)
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("delete tenant: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return c.readError("delete tenant", resp)
	}

	c.logger.Info().Str(logging.LogKeyTenantSlug, tenantID).Msg("tenant deleted")
	return nil
}

// RegisterKey registers a public key for a tenant.
func (c *ProvisioningClient) RegisterKey(ctx context.Context, tenantID string, keyReq RegisterKeyRequest) error {
	body, err := json.Marshal(keyReq)
	if err != nil {
		return fmt.Errorf("register key: marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v1/tenants/"+tenantID+"/keys", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("register key: build request: %w", err)
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("register key: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return c.readError("register key", resp)
	}

	c.logger.Info().Str(logging.LogKeyTenantSlug, tenantID).Str("key_id", keyReq.KeyID).Msg("key registered")
	return nil
}

// RevokeKey revokes a key for a tenant.
func (c *ProvisioningClient) RevokeKey(ctx context.Context, tenantID, keyID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.baseURL+"/api/v1/tenants/"+tenantID+"/keys/"+keyID, http.NoBody)
	if err != nil {
		return fmt.Errorf("revoke key: build request: %w", err)
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("revoke key: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return c.readError("revoke key", resp)
	}

	c.logger.Info().Str(logging.LogKeyTenantSlug, tenantID).Str("key_id", keyID).Msg("key revoked")
	return nil
}

// GetTenant fetches a tenant by ID using the given token (admin or tenant JWT).
// Returns the HTTP status code and any error.
func (c *ProvisioningClient) GetTenant(ctx context.Context, tenantID, token string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/v1/tenants/"+tenantID, http.NoBody)
	if err != nil {
		return 0, fmt.Errorf("get tenant: build request: %w", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("get tenant: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	// Drain body so connection can be reused.
	_, _ = io.Copy(io.Discard, resp.Body)

	return resp.StatusCode, nil
}

// SetChannelRules sets channel rules for a tenant via PUT /api/v1/tenants/{id}/channel-rules.
func (c *ProvisioningClient) SetChannelRules(ctx context.Context, tenantID string, rules map[string]any) error {
	body, err := json.Marshal(rules)
	if err != nil {
		return fmt.Errorf("set channel rules: marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.baseURL+"/api/v1/tenants/"+tenantID+"/channel-rules", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("set channel rules: build request: %w", err)
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("set channel rules: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return c.readError("set channel rules", resp)
	}

	c.logger.Info().Str(logging.LogKeyTenantSlug, tenantID).Msg("channel rules set")
	return nil
}

// SetRoutingRules sets routing rules for a tenant via PUT /api/v1/tenants/{id}/routing-rules.
func (c *ProvisioningClient) SetRoutingRules(ctx context.Context, tenantID string, rules []map[string]any) error {
	body, err := json.Marshal(map[string]any{"rules": rules})
	if err != nil {
		return fmt.Errorf("set routing rules: marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.baseURL+"/api/v1/tenants/"+tenantID+"/routing-rules", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("set routing rules: build request: %w", err)
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("set routing rules: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return c.readError("set routing rules", resp)
	}

	c.logger.Info().Str(logging.LogKeyTenantSlug, tenantID).Msg("routing rules set")
	return nil
}

// SetRoutingRulesRaw sets routing rules and returns the HTTP status code.
// Used by edition limit boundary testing which needs to distinguish 200 vs 403.
func (c *ProvisioningClient) SetRoutingRulesRaw(ctx context.Context, tenantID string, rules []map[string]any) (int, error) {
	body, err := json.Marshal(map[string]any{"rules": rules})
	if err != nil {
		return 0, fmt.Errorf("set routing rules: marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.baseURL+"/api/v1/tenants/"+tenantID+"/routing-rules", bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("set routing rules: build request: %w", err)
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("set routing rules: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		return resp.StatusCode, c.readError("set routing rules", resp)
	}

	return resp.StatusCode, nil
}

// DeleteRoutingRules deletes routing rules for a tenant via DELETE /api/v1/tenants/{id}/routing-rules.
// Returns nil if rules don't exist (404 is acceptable).
func (c *ProvisioningClient) DeleteRoutingRules(ctx context.Context, tenantID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.baseURL+"/api/v1/tenants/"+tenantID+"/routing-rules", http.NoBody)
	if err != nil {
		return fmt.Errorf("delete routing rules: build request: %w", err)
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("delete routing rules: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// 200 and 404 are both acceptable — rules may not exist
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return c.readError("delete routing rules", resp)
	}

	c.logger.Info().Str(logging.LogKeyTenantSlug, tenantID).Msg("routing rules deleted")
	return nil
}

// --- Raw routing-rules methods for the routing-rules coverage suite ---
//
// These return (status int, err error) matching SetRoutingRulesRaw; on a >=400 response the
// error embeds the response body via readError's "HTTP %d: %s" convention so callers can
// derive the `code` field with extractErrorCode(err.Error()). Token variants set
// Authorization: Bearer <token> per the GetTenant precedent (no admin Provider); the admin
// variant signs via setHeaders. There is no dual-purpose "empty token means admin" parameter
// (Constitution XV) — admin and token-parameterized are separate methods.

// routingRulesPath is the routing-rules subpath for a tenant.
func (c *ProvisioningClient) routingRulesPath(tenantID string) string {
	return c.baseURL + "/api/v1/tenants/" + tenantID + "/routing-rules"
}

// doRawAdmin issues an admin-signed request with an optional JSON body and returns the status.
// On a >=400 response it returns the status plus a readError-wrapped error embedding the body.
func (c *ProvisioningClient) doRawAdmin(ctx context.Context, method, url, operation string, payload []byte) (int, error) {
	var bodyReader io.Reader = http.NoBody
	if payload != nil {
		bodyReader = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return 0, fmt.Errorf("%s: build request: %w", operation, err)
	}
	c.setHeaders(req)
	return c.doRaw(req, operation)
}

// doRawWithToken issues a request authenticated with an explicit bearer token (not the admin
// Provider) and an optional JSON body, returning the status; >=400 embeds the body in the error.
func (c *ProvisioningClient) doRawWithToken(ctx context.Context, method, url, token, operation string, payload []byte) (int, error) {
	var bodyReader io.Reader = http.NoBody
	if payload != nil {
		bodyReader = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return 0, fmt.Errorf("%s: build request: %w", operation, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	return c.doRaw(req, operation)
}

// doRaw executes req and returns the HTTP status; a >=400 status yields a readError-wrapped
// error whose text embeds the response body ("HTTP %d: %s") for extractErrorCode.
func (c *ProvisioningClient) doRaw(req *http.Request, operation string) (int, error) {
	// The request URL is always c.baseURL (the operator-configured provisioning API) + a fixed
	// path + a tenant slug; the Authorization bearer is a JWT the tester itself mints, never
	// attacker-controlled input. No untrusted data reaches the URL.
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", operation, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		return resp.StatusCode, c.readError(operation, resp)
	}
	// Drain body so the connection can be reused.
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

// AddRoutingRuleRaw POSTs a single routing rule as admin and returns the HTTP status.
func (c *ProvisioningClient) AddRoutingRuleRaw(ctx context.Context, tenantID string, rule map[string]any) (int, error) {
	body, err := json.Marshal(map[string]any{"rule": rule})
	if err != nil {
		return 0, fmt.Errorf("add routing rule: marshal: %w", err)
	}
	return c.doRawAdmin(ctx, http.MethodPost, c.routingRulesPath(tenantID), "add routing rule", body)
}

// AddRoutingRuleRawWithToken POSTs a single routing rule using an explicit bearer token.
func (c *ProvisioningClient) AddRoutingRuleRawWithToken(ctx context.Context, tenantID, token string, rule map[string]any) (int, error) {
	body, err := json.Marshal(map[string]any{"rule": rule})
	if err != nil {
		return 0, fmt.Errorf("add routing rule: marshal: %w", err)
	}
	return c.doRawWithToken(ctx, http.MethodPost, c.routingRulesPath(tenantID), token, "add routing rule", body)
}

// SetRoutingRulesRawWithToken PUTs a routing-rules replacement using an explicit bearer token.
func (c *ProvisioningClient) SetRoutingRulesRawWithToken(ctx context.Context, tenantID, token string, rules []map[string]any) (int, error) {
	body, err := json.Marshal(map[string]any{"rules": rules})
	if err != nil {
		return 0, fmt.Errorf("set routing rules: marshal: %w", err)
	}
	return c.doRawWithToken(ctx, http.MethodPut, c.routingRulesPath(tenantID), token, "set routing rules", body)
}

// DeleteRoutingRulesRaw DELETEs routing rules as admin and returns the HTTP status. Unlike
// DeleteRoutingRules (which tolerates 404), this surfaces the exact status so the idempotent
// second-delete probe can assert 200 (the service has no not-found branch — handlers.go:560-572).
func (c *ProvisioningClient) DeleteRoutingRulesRaw(ctx context.Context, tenantID string) (int, error) {
	return c.doRawAdmin(ctx, http.MethodDelete, c.routingRulesPath(tenantID), "delete routing rules", nil)
}

// GetRoutingRulesWithToken GETs routing rules using an explicit bearer token and returns the
// HTTP status (used to assert user-role GET is allowed). Delegates to doRawWithToken: a 2xx
// yields (status, nil); a >=400 yields (status, err) with the body embedded for extractErrorCode.
func (c *ProvisioningClient) GetRoutingRulesWithToken(ctx context.Context, tenantID, token string) (int, error) {
	return c.doRawWithToken(ctx, http.MethodGet, c.routingRulesPath(tenantID), token, "get routing rules", nil)
}

// GetRoutingRulesPage GETs routing rules as admin with a ?limit=N query and returns the raw
// JSON body (used to assert pagination honoring and the max-page-limit cap echo).
func (c *ProvisioningClient) GetRoutingRulesPage(ctx context.Context, tenantID string, limit int) ([]byte, error) {
	path := fmt.Sprintf("/api/v1/tenants/%s/routing-rules?limit=%d", tenantID, limit)
	return c.doGet(ctx, path, "get routing rules")
}

// --- Rename + test-access methods for the rename/test-access coverage suite ---
//
// Mirror the routing-rules raw method shapes: (status int, err error) for rename (admin +
// token variants), and (status int, body []byte, err error) for test-access so the success
// body's allowed_patterns is surfaced (the body-discarding doRawWithToken cannot). On a >=400
// response the error embeds the body via readError so callers derive `code` with extractErrorCode.

// renamePath is the rename subpath for a tenant.
func (c *ProvisioningClient) renamePath(tenantID string) string {
	return c.baseURL + "/api/v1/tenants/" + tenantID + "/rename"
}

// RenameTenantRaw renames a tenant's slug as admin (POST /tenants/{slug}/rename, body
// {"slug":newSlug}) and returns the HTTP status. Used by the rename coverage suite's admin cases.
func (c *ProvisioningClient) RenameTenantRaw(ctx context.Context, slug, newSlug string) (int, error) {
	body, err := json.Marshal(map[string]string{"slug": newSlug})
	if err != nil {
		return 0, fmt.Errorf("rename tenant: marshal: %w", err)
	}
	return c.doRawAdmin(ctx, http.MethodPost, c.renamePath(slug), "rename tenant", body)
}

// RenameTenantRawWithToken renames a tenant's slug using an explicit bearer token (not the admin
// Provider). Used by the rename role-gate negative — a user-role token must be rejected 403
// INSUFFICIENT_ROLE, so the caller token (not the admin key) MUST be the one sent.
func (c *ProvisioningClient) RenameTenantRawWithToken(ctx context.Context, slug, token, newSlug string) (int, error) {
	body, err := json.Marshal(map[string]string{"slug": newSlug})
	if err != nil {
		return 0, fmt.Errorf("rename tenant: marshal: %w", err)
	}
	return c.doRawWithToken(ctx, http.MethodPost, c.renamePath(slug), token, "rename tenant", body)
}

// TestAccessRaw POSTs a groups list to /tenants/{slug}/test-access using an explicit bearer
// token (tenant-accessible, not admin-gated) and returns the HTTP status plus the raw success
// body — the caller parses allowed_patterns from it. A >=400 response yields (status, nil, err)
// with the body embedded for extractErrorCode.
func (c *ProvisioningClient) TestAccessRaw(ctx context.Context, slug, token string, groups []string) (status int, body []byte, err error) {
	payload, err := json.Marshal(map[string]any{"groups": groups})
	if err != nil {
		return 0, nil, fmt.Errorf("test access: marshal: %w", err)
	}
	url := c.baseURL + "/api/v1/tenants/" + slug + "/test-access"
	return c.doRawWithTokenBody(ctx, http.MethodPost, url, token, "test access", payload)
}

// PostRawAdmin POSTs a caller-supplied raw body (may be malformed JSON) to path as admin and
// returns the status. Used by the rename coverage suite's bad-JSON negative — the typed rename
// methods marshal valid JSON, so this exercises the handler's INVALID_REQUEST branch directly.
// path is a fixed subpath ("/api/v1/tenants/{slug}/rename") — never attacker-controlled.
func (c *ProvisioningClient) PostRawAdmin(ctx context.Context, path, operation string, body []byte) (int, error) {
	return c.doRawAdmin(ctx, http.MethodPost, c.baseURL+path, operation, body)
}

// PostRawWithTokenBody POSTs a caller-supplied raw body (may be malformed JSON) to path with an
// explicit bearer token and returns the status + success body. Used by the test-access coverage
// suite's bad-JSON negative.
func (c *ProvisioningClient) PostRawWithTokenBody(ctx context.Context, path, token, operation string, body []byte) (status int, respBody []byte, err error) {
	return c.doRawWithTokenBody(ctx, http.MethodPost, c.baseURL+path, token, operation, body)
}

// doRawWithTokenBody issues a request authenticated with an explicit bearer token, returning the
// status AND the success body (unlike doRawWithToken, which discards it). On a >=400 response it
// returns (status, nil, err) with the body embedded via readError for extractErrorCode.
func (c *ProvisioningClient) doRawWithTokenBody(ctx context.Context, method, url, token, operation string, payload []byte) (status int, body []byte, err error) {
	var bodyReader io.Reader = http.NoBody
	if payload != nil {
		bodyReader = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return 0, nil, fmt.Errorf("%s: build request: %w", operation, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("%s: %w", operation, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
	if resp.StatusCode >= 400 {
		return resp.StatusCode, nil, formatHTTPError(operation, resp.StatusCode, body)
	}
	if readErr != nil {
		return resp.StatusCode, nil, fmt.Errorf("%s: read response body: %w", operation, readErr)
	}
	return resp.StatusCode, body, nil
}

// ListTenants fetches the tenant list. Returns raw JSON response body.
func (c *ProvisioningClient) ListTenants(ctx context.Context) ([]byte, error) {
	return c.doGet(ctx, "/api/v1/tenants", "list tenants")
}

// UpdateTenant partially updates a tenant via PATCH.
func (c *ProvisioningClient) UpdateTenant(ctx context.Context, tenantID string, updates map[string]any) error {
	return c.doPatchJSON(ctx, "/api/v1/tenants/"+tenantID, updates, "update tenant")
}

// GetTenantByID fetches a tenant using the admin token. Returns raw JSON response body.
// Unlike GetTenant (which accepts an arbitrary token for JWT testing), this always authenticates
// as the admin — suitable for validate:provisioning CRUD checks.
func (c *ProvisioningClient) GetTenantByID(ctx context.Context, tenantID string) ([]byte, error) {
	return c.doGet(ctx, "/api/v1/tenants/"+tenantID, "get tenant")
}

// SuspendTenant suspends a tenant via POST /api/v1/tenants/{id}/suspend.
func (c *ProvisioningClient) SuspendTenant(ctx context.Context, tenantID string) error {
	return c.doPost(ctx, "/api/v1/tenants/"+tenantID+"/suspend", nil, "suspend tenant")
}

// ReactivateTenant reactivates a suspended tenant via POST /api/v1/tenants/{id}/reactivate.
func (c *ProvisioningClient) ReactivateTenant(ctx context.Context, tenantID string) error {
	return c.doPost(ctx, "/api/v1/tenants/"+tenantID+"/reactivate", nil, "reactivate tenant")
}

// CreateAPIKey creates an API key for a tenant. Returns raw JSON response body (contains the key value).
func (c *ProvisioningClient) CreateAPIKey(ctx context.Context, tenantID, name string) ([]byte, error) {
	body, err := json.Marshal(map[string]string{"name": name})
	if err != nil {
		return nil, fmt.Errorf("create api key: marshal: %w", err)
	}
	return c.doPostForBody(ctx, "/api/v1/tenants/"+tenantID+"/api-keys", body, "create api key")
}

// ListAPIKeys lists API keys for a tenant. Returns raw JSON response body.
func (c *ProvisioningClient) ListAPIKeys(ctx context.Context, tenantID string) ([]byte, error) {
	return c.doGet(ctx, "/api/v1/tenants/"+tenantID+"/api-keys", "list api keys")
}

// RevokeAPIKey revokes an API key.
func (c *ProvisioningClient) RevokeAPIKey(ctx context.Context, tenantID, keyID string) error {
	return c.doDelete(ctx, "/api/v1/tenants/"+tenantID+"/api-keys/"+keyID, "revoke api key")
}

// ListKeys lists signing keys for a tenant. Returns raw JSON response body.
func (c *ProvisioningClient) ListKeys(ctx context.Context, tenantID string) ([]byte, error) {
	return c.doGet(ctx, "/api/v1/tenants/"+tenantID+"/keys", "list keys")
}

// GetRoutingRules fetches routing rules for a tenant. Returns raw JSON response body.
func (c *ProvisioningClient) GetRoutingRules(ctx context.Context, tenantID string) ([]byte, error) {
	return c.doGet(ctx, "/api/v1/tenants/"+tenantID+"/routing-rules", "get routing rules")
}

// GetChannelRules fetches channel rules for a tenant. Returns raw JSON response body.
func (c *ProvisioningClient) GetChannelRules(ctx context.Context, tenantID string) ([]byte, error) {
	return c.doGet(ctx, "/api/v1/tenants/"+tenantID+"/channel-rules", "get channel rules")
}

// DeleteChannelRules deletes channel rules for a tenant.
func (c *ProvisioningClient) DeleteChannelRules(ctx context.Context, tenantID string) error {
	return c.doDelete(ctx, "/api/v1/tenants/"+tenantID+"/channel-rules", "delete channel rules")
}

// GetQuota fetches quotas for a tenant. Returns raw JSON response body.
func (c *ProvisioningClient) GetQuota(ctx context.Context, tenantID string) ([]byte, error) {
	return c.doGet(ctx, "/api/v1/tenants/"+tenantID+"/quotas", "get quota")
}

// UpdateQuota partially updates quotas for a tenant via PATCH.
func (c *ProvisioningClient) UpdateQuota(ctx context.Context, tenantID string, updates map[string]any) error {
	return c.doPatchJSON(ctx, "/api/v1/tenants/"+tenantID+"/quotas", updates, "update quota")
}

// GetAuditLog fetches the audit log for a tenant. Returns raw JSON response body.
func (c *ProvisioningClient) GetAuditLog(ctx context.Context, tenantID string) ([]byte, error) {
	return c.doGet(ctx, "/api/v1/tenants/"+tenantID+"/audit", "get audit log")
}

// --- Push credential + channel methods ---

// SetPushCredentials uploads push provider credentials for a tenant.
func (c *ProvisioningClient) SetPushCredentials(ctx context.Context, tenantID, provider, credentialData string) error {
	payload, _ := json.Marshal(map[string]string{ // json.Marshal on literal map of strings cannot fail
		"tenant_id":       tenantID,
		"provider":        provider,
		"credential_data": credentialData,
	})
	return c.doPost(ctx, "/api/v1/push/credentials", payload, "set push credentials")
}

// DeletePushCredentials removes push provider credentials for a tenant.
func (c *ProvisioningClient) DeletePushCredentials(ctx context.Context, tenantID, provider string) error {
	payload, _ := json.Marshal(map[string]string{
		"tenant_id": tenantID,
		"provider":  provider,
	})
	return c.doDeleteWithBody(ctx, "/api/v1/push/credentials", payload, "delete push credentials")
}

// SetPushChannels creates or updates push channel configuration for a tenant.
func (c *ProvisioningClient) SetPushChannels(ctx context.Context, tenantID string, patterns []string, ttl int, urgency string) error {
	payload, _ := json.Marshal(map[string]any{
		"tenant_id":       tenantID,
		"patterns":        patterns,
		"default_ttl":     ttl,
		"default_urgency": urgency,
	})
	return c.doPost(ctx, "/api/v1/push/channels", payload, "set push channels")
}

// GetPushChannels retrieves push channel configuration for a tenant. Returns raw JSON.
func (c *ProvisioningClient) GetPushChannels(ctx context.Context, tenantID string) ([]byte, error) {
	return c.doGet(ctx, "/api/v1/push/channels?tenant_id="+tenantID, "get push channels")
}

// DeletePushChannels removes push channel configuration for a tenant.
func (c *ProvisioningClient) DeletePushChannels(ctx context.Context, tenantID string) error {
	payload, _ := json.Marshal(map[string]string{
		"tenant_id": tenantID,
	})
	return c.doDeleteWithBody(ctx, "/api/v1/push/channels", payload, "delete push channels")
}

// --- HTTP helpers ---

func (c *ProvisioningClient) doGet(ctx context.Context, path, operation string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("%s: build request: %w", operation, err)
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", operation, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
	if resp.StatusCode != http.StatusOK {
		return nil, formatHTTPError(operation, resp.StatusCode, body)
	}
	if readErr != nil {
		return nil, fmt.Errorf("%s: read response body: %w", operation, readErr)
	}
	return body, nil
}

func (c *ProvisioningClient) doPost(ctx context.Context, path string, payload []byte, operation string) error {
	var bodyReader io.Reader = http.NoBody
	if payload != nil {
		bodyReader = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bodyReader)
	if err != nil {
		return fmt.Errorf("%s: build request: %w", operation, err)
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return c.readError(operation, resp)
	}
	return nil
}

func (c *ProvisioningClient) doPostForBody(ctx context.Context, path string, payload []byte, operation string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("%s: build request: %w", operation, err)
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", operation, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, formatHTTPError(operation, resp.StatusCode, body)
	}
	if readErr != nil {
		return nil, fmt.Errorf("%s: read response body: %w", operation, readErr)
	}
	return body, nil
}

func (c *ProvisioningClient) doDelete(ctx context.Context, path, operation string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.baseURL+path, http.NoBody)
	if err != nil {
		return fmt.Errorf("%s: build request: %w", operation, err)
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return c.readError(operation, resp)
	}
	return nil
}

func (c *ProvisioningClient) doDeleteWithBody(ctx context.Context, path string, payload []byte, operation string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("%s: build request: %w", operation, err)
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return c.readError(operation, resp)
	}
	return nil
}

func (c *ProvisioningClient) doPatchJSON(ctx context.Context, path string, data map[string]any, operation string) error {
	body, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("%s: marshal: %w", operation, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("%s: build request: %w", operation, err)
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return c.readError(operation, resp)
	}
	return nil
}

type createWebhookRequest struct {
	URL            string `json:"url"`
	ChannelPattern string `json:"channel_pattern"`
	Secret         string `json:"secret"`
	MaxRetries     int    `json:"max_retries"`
}

// CreateWebhook registers a new webhook for the given tenant.
// Returns the webhook ID assigned by the provisioning service.
func (c *ProvisioningClient) CreateWebhook(ctx context.Context, tenantSlug, url, channelPattern, secret string, maxRetries int) (string, error) {
	body, err := json.Marshal(createWebhookRequest{ //nolint:gosec // G117 false positive: Secret field carries the webhook signing secret intentionally sent to the provisioning API; this is not a credential leak
		URL:            url,
		ChannelPattern: channelPattern,
		Secret:         secret,
		MaxRetries:     maxRetries,
	})
	if err != nil {
		return "", fmt.Errorf("create webhook: marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/api/v1/tenants/"+tenantSlug+"/webhooks",
		bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("create webhook: build request: %w", err)
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("create webhook: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return "", c.readError("create webhook", resp)
	}

	var result struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBody)).Decode(&result); err != nil {
		return "", fmt.Errorf("create webhook: decode response: %w", err)
	}
	return result.ID, nil
}

// GetWebhookByID fetches a webhook by ID and returns its current status.
func (c *ProvisioningClient) GetWebhookByID(ctx context.Context, tenantSlug, webhookID string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.baseURL+"/api/v1/tenants/"+tenantSlug+"/webhooks/"+webhookID,
		http.NoBody)
	if err != nil {
		return "", fmt.Errorf("get webhook: build request: %w", err)
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("get webhook: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", c.readError("get webhook", resp)
	}

	var result struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBody)).Decode(&result); err != nil {
		return "", fmt.Errorf("get webhook: decode response: %w", err)
	}
	return result.Status, nil
}

// DeleteWebhook deletes a webhook registration.
func (c *ProvisioningClient) DeleteWebhook(ctx context.Context, tenantSlug, webhookID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		c.baseURL+"/api/v1/tenants/"+tenantSlug+"/webhooks/"+webhookID,
		http.NoBody)
	if err != nil {
		return fmt.Errorf("delete webhook: build request: %w", err)
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("delete webhook: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return c.readError("delete webhook", resp)
	}
	return nil
}

func (c *ProvisioningClient) setHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
	if c.authProvider != nil {
		c.authProvider.SignRequest(req)
	}
}

func (c *ProvisioningClient) readError(operation string, resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody)) // best-effort: error body for diagnostics only
	return formatHTTPError(operation, resp.StatusCode, body)
}

// formatHTTPError builds the standard "<operation>: HTTP <status>: <body>" error. The body is
// embedded so extractErrorCode can recover the response's `code` field (it scans from the first
// '{'). Single-sourced here so readError and the body-reading doRaw*/doGet/doPostForBody paths
// share one format and cannot silently diverge (§X/§XVIII).
func formatHTTPError(operation string, status int, body []byte) error {
	return fmt.Errorf("%s: HTTP %d: %s", operation, status, string(body))
}
