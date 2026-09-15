package adminui

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// systemHealthResult is the cached result of the last system health fan-out.
type systemHealthResult struct {
	Data       SystemHealthData
	ComputedAt time.Time
}

// systemHealthCache is the handler-level cache for system health results.
type systemHealthCache struct {
	mu     sync.Mutex
	result *systemHealthResult
}

// get returns cached data if within TTL.
func (c *systemHealthCache) get() (SystemHealthData, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.result != nil && time.Since(c.result.ComputedAt) < adminHealthCacheTTL {
		return c.result.Data, true
	}
	return SystemHealthData{}, false
}

// set stores a new result.
func (c *systemHealthCache) set(d SystemHealthData) {
	c.mu.Lock()
	c.result = &systemHealthResult{Data: d, ComputedAt: time.Now()}
	c.mu.Unlock()
}

// healthProbeClient is the dedicated HTTP client for downstream health probes.
// A dedicated client (not http.DefaultClient) isolates probe connections from any
// other in-process HTTP traffic and from tests that swap the global client; the
// per-probe deadline comes from the context in probeHealth, so no Timeout is set here.
var healthProbeClient = &http.Client{}

// probeHealth makes a single HTTP GET to url via client with a timeout.
// Returns "ok" (2xx), "degraded" (non-2xx), "unknown" (error), "unconfigured" (empty url).
func probeHealth(ctx context.Context, logger zerolog.Logger, client *http.Client, url string) string {
	if url == "" {
		return "unconfigured"
	}
	ctx, cancel := context.WithTimeout(ctx, adminHealthCheckTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		// Debug (not silent): surfaces WHY a probe returned "unknown" for CI/developer diagnosis
		// (§III — an undiagnosable "unknown" is what made #224's flake hard to root-cause).
		logger.Debug().Str("url", url).Err(err).Msg("admin health probe: request build failed")
		return "unknown"
	}
	resp, err := client.Do(req)
	if err != nil {
		logger.Debug().Str("url", url).Err(err).Msg("admin health probe: transport error")
		return "unknown"
	}
	_ = resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return "ok"
	}
	return "degraded"
}

// worstStatus returns the most severe status among provisioning (always "ok"), ws, and gateway.
func worstStatus(statuses ...string) string {
	order := map[string]int{"unknown": 3, "degraded": 2, "unconfigured": 1, "ok": 0}
	worst := "ok"
	for _, s := range statuses {
		if order[s] > order[worst] {
			worst = s
		}
	}
	return worst
}
