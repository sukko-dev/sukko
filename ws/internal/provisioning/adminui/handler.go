package adminui

import (
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/shared/httputil"
	"github.com/sukko-dev/sukko/internal/shared/license"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/platform"
	"github.com/sukko-dev/sukko/internal/shared/types"
)

// SSEDelegate allows adminui to accept the analytics SSE handler without importing the api package.
// *api.AnalyticsSSEHandler satisfies this interface structurally.
type SSEDelegate interface {
	ServeHTTP(http.ResponseWriter, *http.Request)
}

// Handler implements http.Handler and routes all /admin/* requests.
type Handler struct {
	tmpl        *template.Template
	auth        AdminAuthProvider
	svc         *provisioning.Service
	sseDelegate SSEDelegate
	edMgr       *license.Manager
	sessionTTL  time.Duration
	healthCache systemHealthCache
	cfg         platform.AdminUIConfig
	logger      zerolog.Logger
	router      http.Handler
}

// NewHandler creates an Handler. Returns (nil, nil) when AdminUIEnabled=false.
func NewHandler(cfg platform.AdminUIConfig, svc *provisioning.Service, sseDelegate SSEDelegate,
	edMgr *license.Manager, logger zerolog.Logger) (*Handler, error) {
	if !cfg.AdminUIEnabled {
		return nil, nil
	}

	tmpl, err := template.New("").Funcs(templateFuncs()).ParseFS(TemplateFS(), "*.html", "partials/*.html")
	if err != nil {
		return nil, fmt.Errorf("adminui: parse templates: %w", err)
	}

	auth := NewTokenAuthProvider(cfg.AdminToken, cfg.AdminSessionTTL)
	h := &Handler{
		tmpl:        tmpl,
		auth:        auth,
		svc:         svc,
		sseDelegate: sseDelegate,
		edMgr:       edMgr,
		sessionTTL:  cfg.AdminSessionTTL,
		cfg:         cfg,
		logger:      logger.With().Str("component", "admin_ui").Logger(),
	}
	h.router = h.buildRouter()
	return h, nil
}

// ServeHTTP implements http.Handler — delegates to the embedded chi router.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.router.ServeHTTP(w, r)
}

// buildRouter builds the full chi sub-router for /admin/. All routing logic lives here;
// the outer api/router.go only calls r.Mount("/admin", handler).
func (h *Handler) buildRouter() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)

	// Static assets — no auth.
	r.Get("/static/*", func(w http.ResponseWriter, req *http.Request) {
		http.StripPrefix("/static", StaticHandler()).ServeHTTP(w, req)
	})

	// JSON schema — no auth (CodeMirror fetches it from the editor).
	r.Get("/schemas/channel-rules.json", h.channelRulesSchema)

	// Edition gate — all /admin/ routes pass through this; Community gets upgrade page.
	r.Group(func(r chi.Router) {
		r.Use(Gate(h.edMgr))

		// Auth routes — no session required.
		r.Get("/login", h.getLogin)
		r.Post("/login", h.postLogin)
		r.Post("/logout", h.postLogout)

		// Session-protected routes.
		r.Group(func(r chi.Router) {
			r.Use(SessionMiddleware(h.auth, h.logger))

			// Pages
			r.Get("/", h.tenantList)
			r.Get("/status", h.statusPage)
			r.Get("/system/health", h.systemHealth)
			r.Get("/analytics/stream", h.analyticsStream)

			// Tenant rows HTMX partial (filtered)
			r.Get("/tenants/rows", h.tenantRows)

			// Tenant detail page + partials
			r.Route("/tenants/{tenantSlug}", func(r chi.Router) {
				r.Get("/", h.tenantDetail)
				r.Get("/routing-rules", h.routingRulesPartial)
				r.Get("/connections", h.connectionsPartial)
				r.Delete("/connections", h.bulkDisconnect)
				r.Delete("/connections/{connID}", h.disconnectOne)
				r.Get("/audit", h.auditPartial)
				r.Put("/channel-rules", h.setChannelRules)
			})
		})
	})

	return r
}

// templateFuncs returns template.FuncMap for adminui templates.
func templateFuncs() template.FuncMap {
	return template.FuncMap{}
}

// --- Page handlers ---

func (h *Handler) tenantList(w http.ResponseWriter, r *http.Request) {
	tenants, total, err := h.svc.ListTenants(r.Context(), provisioning.ListOptions{Limit: adminTenantListLimit})
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "LIST_FAILED", "failed to list tenants")
		return
	}
	q := r.URL.Query().Get("q")
	rows := make([]TenantRowData, 0, len(tenants))
	for _, t := range tenants {
		if q != "" && !strings.Contains(strings.ToLower(t.Slug), strings.ToLower(q)) &&
			!strings.Contains(strings.ToLower(t.Name), strings.ToLower(q)) {
			continue
		}
		rows = append(rows, TenantRowData{
			Slug:      t.Slug,
			Name:      t.Name,
			Status:    string(t.Status),
			CreatedAt: t.CreatedAt.Format("2006-01-02"),
		})
	}
	h.renderHTML(w, "tenants.html", TenantListData{
		Rows:       rows,
		TotalCount: total,
		Filter:     q,
	})
}

func (h *Handler) tenantRows(w http.ResponseWriter, r *http.Request) {
	tenants, _, err := h.svc.ListTenants(r.Context(), provisioning.ListOptions{Limit: adminTenantListLimit})
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "LIST_FAILED", "failed to list tenants")
		return
	}
	q := r.URL.Query().Get("q")
	rows := make([]TenantRowData, 0, len(tenants))
	for _, t := range tenants {
		if q != "" && !strings.Contains(strings.ToLower(t.Slug), strings.ToLower(q)) &&
			!strings.Contains(strings.ToLower(t.Name), strings.ToLower(q)) {
			continue
		}
		rows = append(rows, TenantRowData{
			Slug:      t.Slug,
			Name:      t.Name,
			Status:    string(t.Status),
			CreatedAt: t.CreatedAt.Format("2006-01-02"),
		})
	}
	h.renderPartial(w, "partials/tenant_rows.html", rows)
}

func (h *Handler) tenantDetail(w http.ResponseWriter, r *http.Request) {
	slug := chi.URLParam(r, "tenantSlug")
	t, err := h.svc.GetTenantBySlug(r.Context(), slug)
	if err != nil {
		if errors.Is(err, provisioning.ErrTenantNotFound) {
			httputil.WriteError(w, http.StatusNotFound, "NOT_FOUND", "tenant not found")
			return
		}
		httputil.WriteError(w, http.StatusInternalServerError, "GET_FAILED", "failed to get tenant")
		return
	}
	ed := h.edition()
	h.renderHTML(w, "tenant_detail.html", TenantDetailData{
		TemplateData:   TemplateData{NeedsCodeMirror: true}, // rules editor is ungated (Community)
		Tenant:         TenantSummary{Slug: t.Slug, Name: t.Name, Status: string(t.Status)},
		ActiveTab:      "overview",
		CanConnections: license.EditionHasFeature(ed, license.ConnectionsAPI),
		CanAnalytics:   license.EditionHasFeature(ed, license.Analytics),
		CanAudit:       license.EditionHasFeature(ed, license.AuditLogging),
		TenantID:       t.ID,
	})
}

func (h *Handler) statusPage(w http.ResponseWriter, r *http.Request) {
	h.renderHTML(w, "status.html", StatusData{
		PollInterval: pollIntervalString(),
	})
}

// --- Auth handlers ---

func (h *Handler) getLogin(w http.ResponseWriter, r *http.Request) {
	h.renderLoginPage(w, LoginData{})
}

func (h *Handler) postLogin(w http.ResponseWriter, r *http.Request) {
	// Limit request body to prevent memory exhaustion from large form submissions (gosec G120).
	r.Body = http.MaxBytesReader(w, r.Body, adminLoginMaxBodyBytes)
	if err := r.ParseForm(); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid form data")
		return
	}
	submitted := r.FormValue("token")
	sessionID, err := h.auth.Login(r.Context(), submitted)
	if err != nil {
		if errors.Is(err, ErrTooManySessions) {
			loginAttempts.WithLabelValues(loginResultTooManySessions).Inc()
			h.logger.Warn().Bool("audit", true).Str("result", loginResultTooManySessions).Msg("Admin UI login rejected: session capacity reached")
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(http.StatusServiceUnavailable)
			if tmplErr := h.tmpl.ExecuteTemplate(w, "login.html", LoginData{Error: "Session capacity reached — sign out an existing session and try again."}); tmplErr != nil {
				h.logger.Error().Err(tmplErr).Msg("login template execute failed (too_many_sessions)")
			}
			return
		}
		result := loginResultInvalidToken
		if submitted == "" {
			result = loginResultMissingToken
		}
		loginAttempts.WithLabelValues(result).Inc()
		h.logger.Warn().Bool("audit", true).Str("result", result).Msg("Admin UI login failed")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusUnauthorized)
		if tmplErr := h.tmpl.ExecuteTemplate(w, "login.html", LoginData{Error: "Invalid token"}); tmplErr != nil {
			h.logger.Error().Err(tmplErr).Msg("login template execute failed (invalid_token)")
		}
		return
	}
	loginAttempts.WithLabelValues(loginResultSuccess).Inc()
	h.logger.Info().Bool("audit", true).Str("result", loginResultSuccess).Msg("Admin UI login success")
	http.SetCookie(w, &http.Cookie{
		Name:     AdminSessionCookieName,
		Value:    sessionID,
		Path:     AdminSessionCookiePath,
		MaxAge:   int(h.sessionTTL.Seconds()),
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteStrictMode,
	})
	http.Redirect(w, r, "/admin/", http.StatusSeeOther)
}

func (h *Handler) postLogout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(AdminSessionCookieName); err == nil && c.Value != "" {
		h.auth.Logout(r.Context(), c.Value)
	}
	http.SetCookie(w, &http.Cookie{
		Name:     AdminSessionCookieName,
		Value:    "",
		Path:     AdminSessionCookiePath,
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteStrictMode,
	})
	http.Redirect(w, r, "/admin/login", http.StatusSeeOther)
}

// --- API/partial handlers ---

func (h *Handler) channelRulesSchema(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if _, err := w.Write(ChannelRulesSchema()); err != nil {
		h.logger.Warn().Err(err).Msg("channel rules schema write failed")
	}
}

func (h *Handler) analyticsStream(w http.ResponseWriter, r *http.Request) {
	if !license.EditionHasFeature(h.edition(), license.Analytics) {
		httputil.WriteError(w, http.StatusForbidden, "EDITION_REQUIRED", "Analytics requires Pro edition")
		return
	}
	if h.sseDelegate == nil {
		httputil.WriteError(w, http.StatusServiceUnavailable, "SSE_UNAVAILABLE", "analytics not configured")
		return
	}
	h.sseDelegate.ServeHTTP(w, r)
}

func (h *Handler) routingRulesPartial(w http.ResponseWriter, r *http.Request) {
	slug := chi.URLParam(r, "tenantSlug")
	tenantRules, err := h.svc.GetChannelRules(r.Context(), slug)
	if err != nil && !errors.Is(err, provisioning.ErrChannelRulesNotConfigured) {
		httputil.WriteError(w, http.StatusInternalServerError, "FETCH_FAILED", "failed to load routing rules")
		return
	}
	var view *ChannelRulesView
	if tenantRules != nil {
		view = &ChannelRulesView{
			Public:        tenantRules.Rules.Public,
			GroupMappings: flattenGroupMappings(tenantRules.Rules.GroupMappings),
		}
	}
	h.renderPartial(w, "partials/routing_rules.html", RoutingRulesData{
		TenantSlug: slug,
		Rules:      view,
	})
}

func (h *Handler) connectionsPartial(w http.ResponseWriter, r *http.Request) {
	if !license.EditionHasFeature(h.edition(), license.ConnectionsAPI) {
		httputil.WriteError(w, http.StatusForbidden, "EDITION_REQUIRED", "Connections API requires Pro edition")
		return
	}
	slug := chi.URLParam(r, "tenantSlug")
	h.renderPartial(w, "partials/connections.html", ConnectionsData{
		TenantSlug:  slug,
		Connections: nil,
	})
}

func (h *Handler) bulkDisconnect(w http.ResponseWriter, r *http.Request) {
	if !license.EditionHasFeature(h.edition(), license.ConnectionsAPI) {
		httputil.WriteError(w, http.StatusForbidden, "EDITION_REQUIRED", "Connections API requires Pro edition")
		return
	}
	httputil.WriteError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", "bulk disconnect requires ConnectionsHandler wired in main.go")
}

func (h *Handler) disconnectOne(w http.ResponseWriter, r *http.Request) {
	if !license.EditionHasFeature(h.edition(), license.ConnectionsAPI) {
		httputil.WriteError(w, http.StatusForbidden, "EDITION_REQUIRED", "Connections API requires Pro edition")
		return
	}
	httputil.WriteError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", "disconnect requires ConnectionsHandler wired in main.go")
}

func (h *Handler) auditPartial(w http.ResponseWriter, r *http.Request) {
	if !license.EditionHasFeature(h.edition(), license.AuditLogging) {
		httputil.WriteError(w, http.StatusForbidden, "EDITION_REQUIRED", "Audit log requires Enterprise edition")
		return
	}
	slug := chi.URLParam(r, "tenantSlug")
	entries, _, err := h.svc.GetAuditLog(r.Context(), slug, provisioning.ListOptions{Limit: adminAuditLogLimit})
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "FETCH_FAILED", "failed to load audit log")
		return
	}
	rows := make([]AuditEntryData, len(entries))
	for i, e := range entries {
		details := ""
		for k, v := range e.Details {
			if details != "" {
				details += ", "
			}
			details += fmt.Sprintf("%s=%v", k, v)
		}
		rows[i] = AuditEntryData{
			Timestamp: e.CreatedAt.Format("2006-01-02 15:04:05"),
			Action:    e.Action,
			Actor:     e.Actor,
			Details:   details,
		}
	}
	h.renderPartial(w, "partials/audit.html", AuditData{TenantSlug: slug, AuditEntries: rows})
}

func (h *Handler) setChannelRules(w http.ResponseWriter, r *http.Request) {
	// Ungated (Community): channel rules are the sole channel-authorization
	// mechanism — every edition manages them.
	slug := chi.URLParam(r, "tenantSlug")
	var body types.ChannelRules
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "INVALID_JSON", "invalid channel rules JSON")
		return
	}
	// Nil-initialize slices/maps and validate before calling service (§II: every layer validates;
	// §XVIII: mirrors the REST handler pattern in api/handlers_channel_rules.go).
	if body.Public == nil {
		body.Public = []string{}
	}
	if body.GroupMappings == nil {
		body.GroupMappings = make(map[string][]string)
	}
	if body.Default == nil {
		body.Default = []string{}
	}
	if body.PublishPublic == nil {
		body.PublishPublic = []string{}
	}
	if body.PublishGroupMappings == nil {
		body.PublishGroupMappings = make(map[string][]string)
	}
	if body.PublishDefault == nil {
		body.PublishDefault = []string{}
	}
	if err := body.Validate(); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "VALIDATION_FAILED", err.Error())
		return
	}
	if err := h.svc.SetChannelRules(r.Context(), slug, &body); err != nil {
		if errors.Is(err, provisioning.ErrTenantNotFound) {
			httputil.WriteError(w, http.StatusNotFound, "NOT_FOUND", "tenant not found")
			return
		}
		httputil.WriteError(w, http.StatusInternalServerError, "SET_FAILED", "failed to set channel rules")
		return
	}
	if err := httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"}); err != nil {
		h.logger.Warn().Err(err).Msg("channel rules: write JSON response failed")
	}
}

func (h *Handler) systemHealth(w http.ResponseWriter, r *http.Request) {
	if cached, ok := h.healthCache.get(); ok {
		h.renderPartial(w, "partials/system_health.html", cached)
		return
	}

	type probeResult struct {
		name   string
		status string
	}
	ch := make(chan probeResult, 2)
	// Capture context before goroutines to avoid contextcheck closure lint warning.
	ctx := r.Context()

	// §VII: wg.Go() with defer RecoverPanic for all goroutines.
	var wg sync.WaitGroup
	wg.Go(func() {
		defer logging.RecoverPanic(h.logger, "systemHealth:ws-server", nil)
		ch <- probeResult{"ws-server", probeHealth(ctx, h.logger, healthProbeClient, h.cfg.WSServerHealthURL)}
	})
	wg.Go(func() {
		defer logging.RecoverPanic(h.logger, "systemHealth:gateway", nil)
		ch <- probeResult{"gateway", probeHealth(ctx, h.logger, healthProbeClient, h.cfg.GatewayHealthURL)}
	})
	wg.Wait()
	close(ch)

	services := make([]ServiceStatus, 0, 3)
	services = append(services, ServiceStatus{Name: "provisioning", Status: "ok"})
	statuses := []string{"ok"}
	for res := range ch {
		services = append(services, ServiceStatus{Name: res.name, Status: res.status})
		statuses = append(statuses, res.status)
	}
	data := SystemHealthData{
		Overall:  worstStatus(statuses...),
		Services: services,
	}
	h.healthCache.set(data)
	h.renderPartial(w, "partials/system_health.html", data)
}

// --- Helpers ---

func (h *Handler) renderHTML(w http.ResponseWriter, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.tmpl.ExecuteTemplate(w, name, data); err != nil {
		h.logger.Error().Err(err).Str("template", name).Msg("template execute failed")
	}
}

func (h *Handler) renderPartial(w http.ResponseWriter, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.tmpl.ExecuteTemplate(w, name, data); err != nil {
		h.logger.Error().Err(err).Str("template", name).Msg("partial execute failed")
	}
}

func (h *Handler) renderLoginPage(w http.ResponseWriter, data LoginData) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.tmpl.ExecuteTemplate(w, "login.html", data); err != nil {
		h.logger.Error().Err(err).Msg("login template execute failed")
	}
}

func (h *Handler) edition() license.Edition {
	if h.edMgr == nil {
		return license.Community
	}
	return h.edMgr.Edition()
}

// flattenGroupMappings converts map[string][]string to map[string]string
// for the simplified ChannelRulesView (joins patterns with ", ").
func flattenGroupMappings(m map[string][]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = strings.Join(v, ", ")
	}
	return out
}
