package platform

import "os"

// PodIdentityConfig provides a stable pod identity string for use as a database
// row discriminator in analytics tables. Embedded by all four service configs.
//
// Migration from WS_POD_ID: reads SUKKO_POD_ID first (new convention), then falls
// back to WS_POD_ID for one release cycle, then os.Hostname(), then "unknown-pod".
// Callers in main.go should log a deprecation warning when WSPodID is used:
//
//	if cfg.PodIdentityConfig.WSPodID != "" && cfg.PodIdentityConfig.SukkoPodID == "" {
//	    logger.Warn().Msg("WS_POD_ID is deprecated; set SUKKO_POD_ID via Kubernetes Downward API")
//	}
type PodIdentityConfig struct {
	// SukkoPodID is the canonical pod identity env var, injected by Kubernetes Downward API.
	SukkoPodID string `env:"SUKKO_POD_ID"` // Unique identifier for this pod instance, injected by Kubernetes via the Downward API.
	// WSPodID is the legacy field — backward compat for one release cycle.
	// TODO: remove WS_POD_ID fallback in next release (feat/analytics-pod-id-cleanup).
	WSPodID string `env:"WS_POD_ID"` // Deprecated alias for SUKKO_POD_ID. Use SUKKO_POD_ID in new deployments.
}

// PodID returns the stable pod identity string using a fallback chain.
// This is a pure function — no logging occurs here; callers emit deprecation warnings.
// Called once at startup; result is stored in the Collector constructor.
func (c PodIdentityConfig) PodID() string {
	if c.SukkoPodID != "" {
		return c.SukkoPodID
	}
	if c.WSPodID != "" {
		// TODO: remove WS_POD_ID fallback in next release (feat/analytics-pod-id-cleanup)
		return c.WSPodID
	}
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "unknown-pod"
}
