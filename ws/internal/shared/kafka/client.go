package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// SASL mechanism constants — used for validation and switch cases across all services.
const (
	MechanismSCRAMSHA256 = "scram-sha-256"
	MechanismSCRAMSHA512 = "scram-sha-512"
	MechanismPLAIN       = "plain"
)

// SASLConfig holds SASL authentication configuration for Kafka.
type SASLConfig struct {
	Mechanism string // "plain", "scram-sha-256", or "scram-sha-512"
	Username  string
	Password  string
}

// TLSConfig holds TLS encryption configuration for Kafka.
type TLSConfig struct {
	Enabled            bool
	InsecureSkipVerify bool   // Skip server certificate verification (not for production)
	CAPath             string // Path to CA certificate file (optional, uses system CA pool if empty)
}

// BuildKgoOpts builds franz-go client options for the given brokers and optional
// SASL/TLS configuration. Either or both of sasl and tlsCfg may be nil (disabled).
// The returned opts always include kgo.SeedBrokers as the first element.
func BuildKgoOpts(brokers []string, sasl *SASLConfig, tlsCfg *TLSConfig) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
	}

	if sasl != nil {
		switch sasl.Mechanism {
		case MechanismSCRAMSHA256:
			opts = append(opts, kgo.SASL(scram.Auth{User: sasl.Username, Pass: sasl.Password}.AsSha256Mechanism()))
		case MechanismSCRAMSHA512:
			opts = append(opts, kgo.SASL(scram.Auth{User: sasl.Username, Pass: sasl.Password}.AsSha512Mechanism()))
		case MechanismPLAIN:
			opts = append(opts, kgo.SASL(plain.Auth{User: sasl.Username, Pass: sasl.Password}.AsMechanism()))
		default:
			return nil, fmt.Errorf("unsupported SASL mechanism: %s", sasl.Mechanism)
		}
	}

	if tlsCfg != nil && tlsCfg.Enabled {
		tc, err := buildTLSConfig(tlsCfg)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tc))
	}

	return opts, nil
}

func buildTLSConfig(cfg *TLSConfig) (*tls.Config, error) {
	tc := &tls.Config{
		InsecureSkipVerify: cfg.InsecureSkipVerify, //nolint:gosec // InsecureSkipVerify is set only when KAFKA_TLS_INSECURE=true; default is false. Acceptable risk for dev/test environments only — never enable in production.
		MinVersion:         tls.VersionTLS12,
	}

	if cfg.CAPath != "" {
		pem, err := os.ReadFile(cfg.CAPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate from %s: %w", cfg.CAPath, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("failed to parse CA certificate from %s", cfg.CAPath)
		}
		tc.RootCAs = pool
	}

	return tc, nil
}
