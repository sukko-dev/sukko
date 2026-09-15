package platform

import (
	"maps"
	"net/http"
	"reflect"

	"github.com/sukko-dev/sukko/internal/shared/httputil"
)

// ConfigHandler returns an http.HandlerFunc that serves the runtime configuration
// as JSON with sensitive fields redacted. The redaction runs ONCE at handler creation
// time — the result is cached in the closure (zero reflection at request time).
func ConfigHandler(cfg any) http.HandlerFunc {
	snapshot := RedactConfig(cfg)

	return func(w http.ResponseWriter, _ *http.Request) {
		// WriteJSON error is ignored: if the client disconnected, there is
		// no way to communicate a secondary error on the already-committed response.
		_ = httputil.WriteJSON(w, http.StatusOK, snapshot)
	}
}

// RedactConfig converts a config struct to a map with sensitive fields replaced
// by "[REDACTED]". Fields tagged with `redact:"true"` are redacted when non-empty.
// The `env` struct tag is used as the JSON key (falls back to field name).
func RedactConfig(cfg any) map[string]any {
	result := make(map[string]any)

	v := reflect.ValueOf(cfg)
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return result
	}

	t := v.Type()
	for i := range t.NumField() {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}

		// Use env tag as key, fall back to field name
		key := field.Tag.Get("env")
		if key == "" {
			key = field.Name
		}

		val := v.Field(i)

		// Dereference pointer fields before struct check
		if val.Kind() == reflect.Pointer && !val.IsNil() {
			val = val.Elem()
		}

		// Recurse into nested structs to ensure redaction tags are applied
		if val.Kind() == reflect.Struct {
			nested := RedactConfig(val.Interface())
			maps.Copy(result, nested)
			continue
		}

		// Redact sensitive fields when non-empty
		if field.Tag.Get("redact") == "true" {
			if !val.IsZero() {
				result[key] = "[REDACTED]"
			} else {
				result[key] = ""
			}
			continue
		}

		result[key] = val.Interface()
	}

	return result
}
