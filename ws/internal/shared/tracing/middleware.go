package tracing

import (
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// HTTPMiddleware wraps an http.Handler with OpenTelemetry tracing.
// Extracts http.method, http.route, and http.status_code attributes.
// When tracing is disabled (noop TracerProvider), this adds zero overhead.
func HTTPMiddleware(next http.Handler) http.Handler {
	return otelhttp.NewHandler(next, "http.request")
}
