package platform

import (
	"maps"
	"reflect"
	"testing"
)

// TestSharedDefaultsConsistency uses reflection to verify that envDefault tags
// for shared environment variables are consistent across GatewayConfig,
// ServerConfig, and ProvisioningConfig.
//
// Most shared defaults are now structurally guaranteed by sub-config embedding
// (AuthConfig, ProvisioningClientConfig, KafkaNamespaceConfig, HTTPTimeoutConfig).
// This test verifies defaults that are NOT structurally guaranteed:
//   - HTTP timeout semantic consistency: gateway uses GATEWAY_*_TIMEOUT env vars,
//     server/provisioning use HTTP_*_TIMEOUT — different names, same intended defaults
func TestSharedDefaultsConsistency(t *testing.T) {
	t.Parallel()

	// Build env var → envDefault maps for each config struct.
	gatewayDefaults := extractEnvDefaults(reflect.TypeFor[GatewayConfig]())
	serverDefaults := extractEnvDefaults(reflect.TypeFor[ServerConfig]())
	provisioningDefaults := extractEnvDefaults(reflect.TypeFor[ProvisioningConfig]())

	// Semantic consistency: HTTP timeout defaults should match across all services.
	// Gateway uses GATEWAY_*_TIMEOUT env vars; server and provisioning use HTTP_*_TIMEOUT.
	// The default values should be identical since they serve the same purpose.
	httpTimeoutPairs := []struct {
		name            string
		gatewayEnvVar   string
		serverEnvVar    string
		expectedDefault string
	}{
		{"read timeout", "GATEWAY_READ_TIMEOUT", "HTTP_READ_TIMEOUT", DefaultHTTPReadTimeout},
		{"write timeout", "GATEWAY_WRITE_TIMEOUT", "HTTP_WRITE_TIMEOUT", DefaultHTTPWriteTimeout},
		{"idle timeout", "GATEWAY_IDLE_TIMEOUT", "HTTP_IDLE_TIMEOUT", DefaultHTTPIdleTimeout},
	}

	for _, tc := range httpTimeoutPairs {
		gwVal, gwOk := gatewayDefaults[tc.gatewayEnvVar]
		srvVal, srvOk := serverDefaults[tc.serverEnvVar]
		provVal, provOk := provisioningDefaults[tc.serverEnvVar]

		if !gwOk {
			t.Errorf("GatewayConfig missing %s", tc.gatewayEnvVar)
		}
		if !srvOk {
			t.Errorf("ServerConfig missing %s", tc.serverEnvVar)
		}
		if !provOk {
			t.Errorf("ProvisioningConfig missing %s", tc.serverEnvVar)
		}
		if gwOk && gwVal != tc.expectedDefault {
			t.Errorf("GatewayConfig %s = %q, want %q", tc.gatewayEnvVar, gwVal, tc.expectedDefault)
		}
		if srvOk && srvVal != tc.expectedDefault {
			t.Errorf("ServerConfig %s = %q, want %q", tc.serverEnvVar, srvVal, tc.expectedDefault)
		}
		if provOk && provVal != tc.expectedDefault {
			t.Errorf("ProvisioningConfig %s = %q, want %q", tc.serverEnvVar, provVal, tc.expectedDefault)
		}
	}
}

// extractEnvDefaults uses reflection to build a map of env var name → envDefault value
// from a struct's field tags.
func extractEnvDefaults(t reflect.Type) map[string]string {
	defaults := make(map[string]string)
	for field := range t.Fields() {
		if field.Anonymous && field.Type.Kind() == reflect.Struct {
			maps.Copy(defaults, extractEnvDefaults(field.Type))
			continue
		}
		envVar := field.Tag.Get("env")
		envDefault := field.Tag.Get("envDefault")
		if envVar != "" {
			defaults[envVar] = envDefault
		}
	}
	return defaults
}
