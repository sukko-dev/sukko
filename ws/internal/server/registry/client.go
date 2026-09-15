package registry

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	valkey "github.com/valkey-io/valkey-go"

	"github.com/sukko-dev/sukko/internal/shared/platform"
)

// BuildRegistryValkeyClient creates a dedicated Valkey client for registry write operations.
// Uses DB 0 (platform.RegistryValkeyDB) regardless of the broadcast bus ValkeyDB setting.
// Mirrors buildHistoryValkeyClient in server.go.
func BuildRegistryValkeyClient(cfg *platform.ServerConfig) (valkey.Client, error) {
	return buildValkeyClient(cfg, "registry")
}

// BuildAdminValkeyClient creates a dedicated Valkey client for per-shard admin pub/sub.
// Uses DB 0 (platform.RegistryValkeyDB) — same database as the registry writer.
func BuildAdminValkeyClient(cfg *platform.ServerConfig) (valkey.Client, error) {
	return buildValkeyClient(cfg, "admin")
}

// buildValkeyClient is the shared implementation for BuildRegistryValkeyClient and BuildAdminValkeyClient.
// label is used in error messages to distinguish the two client types.
func buildValkeyClient(cfg *platform.ServerConfig, label string) (valkey.Client, error) {
	opt := valkey.ClientOption{
		InitAddress: cfg.ValkeyAddrs,
		Password:    cfg.ValkeyPassword,
		SelectDB:    platform.RegistryValkeyDB,
	}
	if platform.UseValkeySentinel(cfg.ValkeyAddrs, cfg.ValkeyMasterName) {
		opt.Sentinel = valkey.SentinelOption{
			MasterSet: cfg.ValkeyMasterName,
		}
	}
	if cfg.ValkeyTLSEnabled {
		tlsCfg := &tls.Config{
			InsecureSkipVerify: cfg.ValkeyTLSInsecure, //nolint:gosec // Controlled by ValkeyTLSInsecure config for dev/testing environments
			MinVersion:         tls.VersionTLS12,
		}
		if cfg.ValkeyTLSCAPath != "" {
			caCert, err := os.ReadFile(cfg.ValkeyTLSCAPath)
			if err != nil {
				return nil, fmt.Errorf("%s valkey: read CA cert: %w", label, err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(caCert) {
				return nil, fmt.Errorf("%s valkey: parse CA cert from %s", label, cfg.ValkeyTLSCAPath)
			}
			tlsCfg.RootCAs = pool
		}
		opt.TLSConfig = tlsCfg
	}
	client, err := valkey.NewClient(opt)
	if err != nil {
		return nil, fmt.Errorf("%s valkey client: %w", label, err)
	}
	return client, nil
}
