package metrics

import "testing"

func TestNoopCacheMetrics(t *testing.T) {
	t.Parallel()

	// Verify no-op implementations don't panic
	var m CacheMetrics = NoopCacheMetrics{}

	m.OnCacheHit()
	m.OnCacheMiss()
	m.OnCacheRefresh(true, 10)
	m.OnCacheRefresh(false, 0)
}

func TestNoopAccessDenialMetrics(t *testing.T) {
	t.Parallel()

	// Verify no-op implementations don't panic
	var m AccessDenialMetrics = NoopAccessDenialMetrics{}

	m.OnAccessDenied("channel", "tenant_mismatch")
	m.OnAccessDenied("topic", "unauthorized")
}

func TestNoopPoolMetrics(t *testing.T) {
	t.Parallel()

	// Verify no-op implementations don't panic
	var m PoolMetrics = NoopPoolMetrics{}

	m.OnMessageRouted()
	m.OnRefresh(true, 10, 5)
	m.OnRefresh(false, 0, 0)
}

func TestInterfaceCompliance(t *testing.T) {
	t.Parallel()

	// Compile-time interface compliance checks
	var _ CacheMetrics = NoopCacheMetrics{}
	var _ CacheMetrics = (*NoopCacheMetrics)(nil)

	var _ AccessDenialMetrics = NoopAccessDenialMetrics{}
	var _ AccessDenialMetrics = (*NoopAccessDenialMetrics)(nil)

	var _ PoolMetrics = NoopPoolMetrics{}
	var _ PoolMetrics = (*NoopPoolMetrics)(nil)
}
