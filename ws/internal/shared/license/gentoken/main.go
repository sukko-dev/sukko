// Command gentoken generates a signed ECDSA P-256 license token for testing.
// The token is printed to stdout (only the token, no decoration) so it
// can be captured via $(go run ...) or piped. Warnings/errors go to stderr.
//
// Usage:
//
//	go run ./internal/shared/license/gentoken \
//	  --key internal/shared/license/keys/sukko.dev.key \
//	  --edition pro --org "Test Corp" --expires +1y
//
//nolint:forbidigo // standalone CLI tool — fmt output is intentional, not a logging violation
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/sukko-dev/sukko/internal/shared/license"
)

func main() {
	var (
		keyPath             = flag.String("key", "", "path to PEM PKCS#8 ECDSA P-256 private key file (required)")
		edition             = flag.String("edition", "", "edition tier: community, pro, enterprise (required)")
		org                 = flag.String("org", "", "organization name (required)")
		expires             = flag.String("expires", "", "expiry: YYYY-MM-DD or relative +30d/+1y/-1d (required)")
		iat                 = flag.Int64("iat", 0, "issued-at Unix timestamp (0 = now)")
		nodes               = flag.Int("nodes", 0, "advisory node count (optional)")
		tenants             = flag.Int("tenants", 0, "override max tenants limit (0 = edition default)")
		connections         = flag.Int("connections", 0, "override max total connections (0 = edition default)")
		shards              = flag.Int("shards", 0, "override max shards (0 = edition default)")
		topicsPerTenant     = flag.Int("topics-per-tenant", 0, "override max topics per tenant (0 = edition default)")
		routingRulesPerTent = flag.Int("routing-rules-per-tenant", 0, "override max routing rules per tenant (0 = edition default)")
	)
	flag.Parse()

	if err := validateRequired(*keyPath, *edition, *org, *expires); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		flag.Usage()
		os.Exit(1)
	}

	exp, err := parseExpiry(*expires)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: invalid --expires: %v\n", err)
		os.Exit(1)
	}

	if exp.Before(time.Now()) {
		fmt.Fprintf(os.Stderr, "Warning: token expires in the past (%s). Sukko will degrade to Community edition.\n", exp.Format("2006-01-02"))
	}

	iatValue := *iat
	if iatValue == 0 {
		iatValue = time.Now().Unix()
	}

	claims, err := buildClaims(*edition, *org, exp, iatValue, *nodes, license.Limits{
		MaxTenants:               *tenants,
		MaxTotalConnections:      *connections,
		MaxShards:                *shards,
		MaxTopicsPerTenant:       *topicsPerTenant,
		MaxRoutingRulesPerTenant: *routingRulesPerTent,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	privKey, err := loadPrivateKey(*keyPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	token := license.SignTestLicense(claims, privKey)
	fmt.Print(token)
}

// loadPrivateKey reads a PEM-encoded PKCS#8 ECDSA P-256 private key from path.
//
// §XVIII: this parallels cmd/tester/auth.ParseECDSAP256PrivateKeyPEM but deliberately
// omits its sign-then-verify round-trip consistency check — gentoken signs a token
// immediately after loading (via license.SignTestLicense), so an inconsistent key
// surfaces at once, whereas the tester loads a key to hold and sign with later.
func loadPrivateKey(path string) (*ecdsa.PrivateKey, error) {
	//nolint:gosec // G304: path is the operator-supplied --key flag for a standalone dev CLI; reading it is the tool's purpose, not untrusted input.
	pemBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read private key: %w", err)
	}
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, fmt.Errorf("no PEM block found in %s", path)
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse PKCS#8 private key: %w", err)
	}
	key, ok := parsed.(*ecdsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("key is %T, want *ecdsa.PrivateKey (ECDSA P-256)", parsed)
	}
	// x509.ParsePKCS8PrivateKey also parses P-384/P-521 EC keys as *ecdsa.PrivateKey;
	// reject any non-P-256 curve so gentoken never signs with the wrong curve.
	if key.Curve != elliptic.P256() {
		return nil, fmt.Errorf("key curve is %s, want P-256", key.Curve.Params().Name)
	}
	return key, nil
}

// validateRequired checks that all required flags are set.
func validateRequired(keyPath, edition, org, expires string) error {
	var missing []string
	if keyPath == "" {
		missing = append(missing, "--key")
	}
	if edition == "" {
		missing = append(missing, "--edition")
	}
	if org == "" {
		missing = append(missing, "--org")
	}
	if expires == "" {
		missing = append(missing, "--expires")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required flags: %s", strings.Join(missing, ", "))
	}
	return nil
}

// parseExpiry parses an expiry string into a time.Time.
// Supports: YYYY-MM-DD, +Nd (days), +Ny (years), -Nd (past days).
func parseExpiry(s string) (time.Time, error) {
	// Relative duration: +30d, +1y, -1d
	if len(s) > 1 && (s[0] == '+' || s[0] == '-') {
		sign := 1
		if s[0] == '-' {
			sign = -1
		}
		numStr := s[1 : len(s)-1]
		unit := s[len(s)-1]

		n, err := strconv.Atoi(numStr)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid relative duration %q: %w", s, err)
		}

		now := time.Now().UTC()
		switch unit {
		case 'd':
			return now.AddDate(0, 0, sign*n), nil
		case 'y':
			return now.AddDate(sign*n, 0, 0), nil
		default:
			return time.Time{}, fmt.Errorf("unknown duration unit %q in %q (use 'd' for days or 'y' for years)", string(unit), s)
		}
	}

	// Absolute date: YYYY-MM-DD → end of day UTC
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid date %q: expected YYYY-MM-DD or relative (+30d, +1y, -1d): %w", s, err)
	}
	return t.Add(23*time.Hour + 59*time.Minute + 59*time.Second), nil
}

// buildClaims constructs license.Claims from validated inputs.
func buildClaims(edition, org string, exp time.Time, iatUnix int64, nodes int, limits license.Limits) (license.Claims, error) {
	ed, err := parseEdition(edition)
	if err != nil {
		return license.Claims{}, err
	}

	return license.Claims{
		Edition: ed,
		Org:     org,
		Exp:     exp.Unix(),
		Iat:     iatUnix,
		Nodes:   nodes,
		Limits:  limits,
	}, nil
}

// parseEdition validates and converts an edition string.
func parseEdition(s string) (license.Edition, error) {
	switch license.Edition(s) {
	case license.Community, license.Pro, license.Enterprise:
		return license.Edition(s), nil
	default:
		return "", errors.New("--edition must be one of: community, pro, enterprise (got: " + s + ")")
	}
}
