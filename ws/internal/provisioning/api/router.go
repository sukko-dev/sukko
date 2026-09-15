// Package api provides HTTP handlers for the provisioning service.
package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning"
	provauth "github.com/sukko-dev/sukko/internal/provisioning/auth"
	"github.com/sukko-dev/sukko/internal/provisioning/eventbus"
	"github.com/sukko-dev/sukko/internal/provisioning/repository"
	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/license"
	"github.com/sukko-dev/sukko/internal/shared/platform"
	"github.com/sukko-dev/sukko/internal/shared/profiling"
	"github.com/sukko-dev/sukko/internal/shared/version"
)

// RouterConfig holds configuration for the HTTP router.
type RouterConfig struct {
	Service            *provisioning.Service
	ProvisioningConfig platform.ProvisioningConfig
	Logger             zerolog.Logger
	RateLimit          int // requests per minute

	// Validator validates tenant JWTs. Required.
	Validator *auth.MultiTenantValidator

	// AdminValidator validates admin JWTs (Ed25519 keypair auth).
	AdminValidator *provauth.AdminValidator

	// AdminKeyRegistry is the in-memory admin key cache.
	AdminKeyRegistry *provauth.AdminKeyRegistry

	// LicenseHandler handles POST /api/v1/license for license hot-reload.
	// Required — provisioning fails to start without CREDENTIALS_ENCRYPTION_KEY.
	LicenseHandler *LicenseHandler

	// AdminKeyRepo is the PostgreSQL admin key repository.
	AdminKeyRepo *repository.AdminKeyRepository

	// EventBus for publishing admin key change events.
	EventBus *eventbus.Bus

	// PushCredentialHandler handles push credential upload/deletion.
	// When set, push credential routes are registered.
	PushCredentialHandler *PushCredentialHandler

	// PushChannelHandler handles push channel config CRUD.
	// When set, push channel config routes are registered.
	PushChannelHandler *PushChannelHandler

	// CORS configuration
	CORSAllowedOrigins []string // Allowed origins (e.g., ["http://localhost:3000"])
	CORSMaxAge         int      // Preflight cache duration in seconds

	// ConfigHandler serves the /config endpoint (set via platform.ConfigHandler)
	ConfigHandler http.HandlerFunc

	// EditionManager for the /edition endpoint (expiry-aware).
	EditionManager *license.Manager

	// PprofEnabled registers /debug/pprof/ handlers when true.
	// Disabled by default (Constitution IX: debug endpoints must be opt-in).
	PprofEnabled bool

	// RevocationHandler handles token revocation (POST /api/v1/tenants/{tenantSlug}/tokens/revoke).
	// When set, revocation routes are registered with Pro edition gate.
	RevocationHandler *RevocationHandler

	// ConnectionsHandler handles the connections management API (Pro edition).
	// When nil, connections routes are not registered.
	ConnectionsHandler *ConnectionsHandler

	// WebhookHandler handles webhook CRUD (Pro edition).
	// When nil, webhook routes are not registered.
	WebhookHandler *WebhookHandler

	// WebhookTestHandler handles POST /webhooks/{id}/test (Pro edition).
	// Independent from WebhookHandler — guarded by its own nil check.
	// When nil, the /test route is not registered even if WebhookHandler is set.
	WebhookTestHandler *WebhookTestHandler

	// AnalyticsSSEHandler serves GET /api/v1/admin/analytics/stream (Pro edition).
	// When nil, the analytics stream route is not registered.
	AnalyticsSSEHandler *AnalyticsSSEHandler

	// AdminUIHandler serves all /admin/* routes (session-cookie auth, HTML).
	// When nil, /admin/ is not mounted and returns 404 from chi's default handler.
	// Set via adminui.NewHandler — accepted as http.Handler to avoid api→adminui import.
	AdminUIHandler http.Handler
}

// NewRouter creates a new HTTP router with all provisioning endpoints.
func NewRouter(cfg RouterConfig) (http.Handler, error) {
	if cfg.Validator == nil {
		return nil, errors.New("router: Validator is required")
	}

	r := chi.NewRouter()

	// CORS middleware - must be first so preflight OPTIONS requests succeed without auth
	if len(cfg.CORSAllowedOrigins) > 0 {
		r.Use(cors.Handler(cors.Options{
			AllowedOrigins: cfg.CORSAllowedOrigins,
			AllowedMethods: []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
			AllowedHeaders: []string{"Accept", "Authorization", "Content-Type", "X-Request-ID"},
			ExposedHeaders: []string{"X-Request-ID"},
			// AllowCredentials is intentionally omitted (defaults to false): the
			// provisioning API is bearer-authed via the Authorization header, never
			// cookies. Enabling credentialed CORS here has no consumer and would be a
			// latent footgun — it silently arms a real cross-origin credential leak if
			// an untrusted origin were ever added to CORS_ALLOWED_ORIGINS.
			MaxAge: cfg.CORSMaxAge,
		}))
	}

	// Middleware stack
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(LoggingMiddleware(cfg.Logger))
	r.Use(middleware.Recoverer)
	// NOTE: Content-Type: application/json is set on the /api/v1 group only.
	// Admin UI routes under /admin/ set text/html or delegate to httputil.WriteError.

	// Create handler
	h, err := NewHandler(cfg.Service, cfg.ProvisioningConfig, cfg.Logger)
	if err != nil {
		return nil, fmt.Errorf("create handler: %w", err)
	}

	// Health endpoints (no auth required)
	r.Get("/health", h.Health)
	r.Get("/ready", h.Ready)
	r.Get("/version", version.Handler("provisioning"))
	r.Get("/edition", license.EditionHandler(cfg.EditionManager, func(ctx context.Context) *license.EditionUsage {
		count, err := cfg.Service.CountTenants(ctx)
		if err != nil {
			return nil // usage unavailable — edition info still returned without tenant count
		}
		return &license.EditionUsage{Tenants: &count}
	}))
	if cfg.ConfigHandler != nil {
		r.Get("/config", cfg.ConfigHandler)
	}
	r.Get("/metrics", h.Metrics)

	// Register pprof endpoints if enabled (Constitution IX: opt-in only)
	profiling.InitPprof(func(pattern string, handler func(http.ResponseWriter, *http.Request)) {
		r.HandleFunc(pattern, handler)
	}, cfg.PprofEnabled, cfg.Logger)

	// Admin UI — mounted before /api/v1 so it can set text/html independently.
	// AdminUIHandler internally builds its full chi sub-router (AdminUIGate, SessionMiddleware,
	// all route registrations). No api→adminui import needed: accepts http.Handler.
	if cfg.AdminUIHandler != nil {
		r.Mount("/admin", cfg.AdminUIHandler)
	}

	// API v1 routes — Content-Type: application/json applied to this group only.
	r.Route("/api/v1", func(r chi.Router) {
		r.Use(middleware.SetHeader("Content-Type", "application/json"))
		// Apply rate limiting if configured
		if cfg.RateLimit > 0 {
			r.Use(RateLimitMiddleware(cfg.RateLimit))
		}

		// Apply admin JWT auth first (checks iss:"sukko-admin", falls through to tenant JWT)
		if cfg.AdminValidator != nil {
			r.Use(AdminJWTMiddleware(cfg.AdminValidator, cfg.Logger))
		}

		// Apply tenant JWT auth middleware
		r.Use(AuthMiddleware(cfg.Validator, cfg.Logger))

		// License hot-reload — admin auth required unconditionally (defense in depth).
		r.Group(func(r chi.Router) {
			r.Use(RequireRole("admin", "system"))
			r.Post("/license", cfg.LicenseHandler.HandleReload)
		})

		// Tenant management
		r.Route("/tenants", func(r chi.Router) {
			// Admin-only operations
			r.Group(func(r chi.Router) {
				r.Use(RequireRole("admin", "system"))
				r.Post("/", h.CreateTenant)
				r.Get("/", h.ListTenants)
			})

			r.Route("/{tenantSlug}", func(r chi.Router) {
				// Tenant isolation - users can only access their own tenant
				r.Use(RequireTenant(cfg.Service.GetTenantBySlug, cfg.ProvisioningConfig.SlugRenameTopicHoldPeriod))

				r.Get("/", h.GetTenant)
				r.Patch("/", h.UpdateTenant)

				// Admin-only operations
				r.Group(func(r chi.Router) {
					r.Use(RequireRole("admin", "system"))
					r.Delete("/", h.DeprovisionTenant)
					r.Post("/rename", h.RenameTenant)
				})

				// Tenant lifecycle — requires Pro
				r.Group(func(r chi.Router) {
					r.Use(RequireFeature(cfg.EditionManager, license.TenantLifecycleManager))
					r.Use(RequireRole("admin", "system"))
					r.Post("/suspend", h.SuspendTenant)
					r.Post("/reactivate", h.ReactivateTenant)
				})

				// Key management (JWT signing keys)
				r.Route("/keys", func(r chi.Router) {
					r.Post("/", h.CreateKey)
					r.Get("/", h.ListKeys)
					r.Delete("/{keyID}", h.RevokeKey)
				})

				// API key management
				r.Route("/api-keys", func(r chi.Router) {
					r.Post("/", h.CreateAPIKey)
					r.Get("/", h.ListAPIKeys)
					r.Delete("/{keyID}", h.RevokeAPIKey)
				})

				// Routing rules management — ungated (ADR-0014); the per-edition
				// MaxRoutingRulesPerTenant count is the wall, enforced in the service.
				r.Route("/routing-rules", func(r chi.Router) {
					r.Get("/", h.GetRoutingRules)
					r.Group(func(r chi.Router) {
						r.Use(RequireRole("admin", "system"))
						r.Post("/", h.AddRoutingRule)
						r.Put("/", h.ReplaceRoutingRules)
						r.Delete("/", h.DeleteRoutingRules)
					})
				})

				// Quota management — requires Pro
				r.Group(func(r chi.Router) {
					r.Use(RequireFeature(cfg.EditionManager, license.PerTenantConfigurableQuotas))
					r.Get("/quotas", h.GetQuota)
					r.Group(func(r chi.Router) {
						r.Use(RequireRole("admin", "system"))
						r.Patch("/quotas", h.UpdateQuota)
					})
				})

				// Audit log — requires Enterprise
				r.Group(func(r chi.Router) {
					r.Use(RequireFeature(cfg.EditionManager, license.AuditLogging))
					r.Get("/audit", h.GetAuditLog)
				})

				// Channel rules — read is open to tenant; write requires admin.
				// Ungated (Community): rules are the sole channel-authorization
				// mechanism, so every edition must be able to manage them.
				r.Route("/channel-rules", func(r chi.Router) {
					r.Get("/", h.GetChannelRules)
					r.Group(func(r chi.Router) {
						r.Use(RequireRole("admin", "system"))
						r.Put("/", h.SetChannelRules)
						r.Delete("/", h.DeleteChannelRules)
					})
				})

				// Token revocation — requires Pro
				if cfg.RevocationHandler != nil {
					r.Route("/tokens", func(r chi.Router) {
						r.Use(RequireFeature(cfg.EditionManager, license.TokenRevocation))
						r.Post("/revoke", cfg.RevocationHandler.HandleRevoke)
					})
				}

				// Connections management API — requires Pro edition
				if cfg.ConnectionsHandler != nil {
					r.Group(func(r chi.Router) {
						r.Use(RequireFeature(cfg.EditionManager, license.ConnectionsAPI))
						r.Get("/connections", cfg.ConnectionsHandler.HandleListConnections)
						r.Delete("/connections", cfg.ConnectionsHandler.HandleBulkDisconnect)
						r.Route("/connections/{connId}", func(r chi.Router) {
							r.Get("/", cfg.ConnectionsHandler.HandleGetConnection)
							r.Delete("/", cfg.ConnectionsHandler.HandleDeleteConnection)
						})
					})
				}

				// Webhook management API — requires Pro edition (§XIII)
				if cfg.WebhookHandler != nil {
					r.Group(func(r chi.Router) {
						r.Use(RequireFeature(cfg.EditionManager, license.Webhooks))
						r.Post("/webhooks", cfg.WebhookHandler.HandleCreate)
						r.Get("/webhooks", cfg.WebhookHandler.HandleList)
						r.Route("/webhooks/{webhookID}", func(r chi.Router) {
							r.Get("/", cfg.WebhookHandler.HandleGet)
							r.Patch("/", cfg.WebhookHandler.HandleUpdate)
							r.Delete("/", cfg.WebhookHandler.HandleDelete)
							// Test delivery endpoint — independent nil guard: WebhookTestHandler
							// may be nil even when WebhookHandler is set (e.g. Community edition,
							// tests, or missing worker gRPC address). Do NOT assume the two are coupled.
							if cfg.WebhookTestHandler != nil {
								r.Post("/test", cfg.WebhookTestHandler.HandleTestDeliver)
							}
						})
					})
				}

				// Test access endpoint
				r.Post("/test-access", h.TestAccess)
			})
		})

		// Active keys endpoint (for WS Gateway) - requires system role
		r.Group(func(r chi.Router) {
			r.Use(RequireRole("system", "admin"))
			r.Get("/keys/active", h.GetActiveKeys)
			r.Get("/api-keys/active", h.GetActiveAPIKeys)
		})

		// Push notification management — requires Pro (WebPush, ADR-0009).
		// fcm/apns credentials are additionally MobilePush-gated in the handler.
		r.Route("/push", func(r chi.Router) {
			r.Use(RequireFeature(cfg.EditionManager, license.WebPush))
			r.Use(RequireRole("admin", "system"))

			// Push credentials
			if cfg.PushCredentialHandler != nil {
				r.Post("/credentials", cfg.PushCredentialHandler.HandleUploadCredentials)
				r.Delete("/credentials", cfg.PushCredentialHandler.HandleDeleteCredentials)
			}

			// Push channel config
			if cfg.PushChannelHandler != nil {
				r.Post("/channels", cfg.PushChannelHandler.HandleCreateChannelConfig)
				r.Get("/channels", cfg.PushChannelHandler.HandleGetChannelConfig)
				r.Delete("/channels", cfg.PushChannelHandler.HandleDeleteChannelConfig)
			}
		})

		// Admin key management — requires admin JWT auth
		if cfg.AdminKeyRepo != nil && cfg.AdminKeyRegistry != nil && cfg.EventBus != nil {
			adminKeysHandler := NewAdminKeysHandler(cfg.AdminKeyRepo, cfg.AdminKeyRegistry, cfg.EventBus, cfg.Logger)
			r.Route("/admin/keys", func(r chi.Router) {
				r.Post("/", adminKeysHandler.Register)
				r.Get("/", adminKeysHandler.List)
				r.Delete("/{id}", adminKeysHandler.Revoke)
			})
		}

		// Admin connections listing — requires admin role + Pro edition
		if cfg.ConnectionsHandler != nil {
			r.Group(func(r chi.Router) {
				r.Use(RequireRole("admin", "system"))
				r.Use(RequireFeature(cfg.EditionManager, license.ConnectionsAPI))
				r.Get("/admin/connections", cfg.ConnectionsHandler.HandleAdminListConnections)
			})
		}

		// Analytics SSE stream — requires admin role + Pro edition
		if cfg.AnalyticsSSEHandler != nil {
			r.Group(func(r chi.Router) {
				r.Use(RequireRole("admin", "system"))
				r.Use(RequireFeature(cfg.EditionManager, license.Analytics))
				r.Get("/admin/analytics/stream", cfg.AnalyticsSSEHandler.ServeHTTP)
			})
		}
	})

	return r, nil
}
