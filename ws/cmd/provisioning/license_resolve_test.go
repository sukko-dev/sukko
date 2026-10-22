package main

import (
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/license"
)

// errAnyResolveError is a table marker for "expect some error" (not a sentinel).
var errAnyResolveError = errors.New("any resolve error")

// TestResolveProvisioningLicense pins the DB-precedence resolution (ADR-0038):
// the DB key wins when valid+newer, the env manager is kept on a replay/invalid DB
// key, a nil env manager still boots from a valid DB key (the expired/empty-env
// rotation case — the no-regression guard), and the absence of any valid license
// fails fast.
//
//nolint:paralleltest // SetPublicKeyForTesting mutates package-level license state
func TestResolveProvisioningLicense(t *testing.T) {
	priv, pub := license.GenerateTestKeyPair()
	license.SetPublicKeyForTesting(pub)
	log := zerolog.Nop()
	future := time.Now().Add(time.Hour).Unix()
	mint := func(e license.Edition, iat int64) string {
		return license.SignTestLicense(license.Claims{Edition: e, Org: "T", Exp: future, Iat: iat}, priv)
	}
	proEnv := mint(license.Pro, 10)
	entDBNewer := mint(license.Enterprise, 20) // newer iat → reload succeeds (DB precedence)
	proDBOlder := mint(license.Pro, 5)         // older iat → replay rejected, env kept
	// An env key that WAS set but has lapsed. provisioning_config.go tolerates
	// ErrLicenseExpired (editionManager left nil), so this reaches resolve as
	// envKey!="" with envMgr==nil — the restart-after-expiry scenario at ADR-0038's core.
	expiredEnv := license.SignTestLicense(license.Claims{Edition: license.Pro, Org: "T", Exp: time.Now().Add(-time.Hour).Unix(), Iat: 10}, priv)

	tests := []struct {
		name    string
		envKey  string // "" => never set; non-empty+expired => set-but-lapsed (envMgr still nil)
		dbKey   string
		wantKey string
		wantEd  license.Edition
		wantErr error // nil=success; errAnyResolveError=any error; else errors.Is match
	}{
		{"env valid, no DB → env", proEnv, "", proEnv, license.Pro, nil},
		{"env valid, DB newer → DB precedence", proEnv, entDBNewer, entDBNewer, license.Enterprise, nil},
		{"env valid, DB older (replay) → keep env", proEnv, proDBOlder, proEnv, license.Pro, nil},
		{"env valid, DB identical (no-op) → DB key effective", proEnv, proEnv, proEnv, license.Pro, nil},
		{"env nil, DB valid → DB (rotation fix)", "", entDBNewer, entDBNewer, license.Enterprise, nil},
		{"env expired, DB valid → boots from DB (restart-after-expiry)", expiredEnv, entDBNewer, entDBNewer, license.Enterprise, nil},
		{"env valid, DB expired → keep env", proEnv, expiredEnv, proEnv, license.Pro, nil},
		{"env unset, DB empty → ErrNoLicense (fail fast)", "", "", "", "", license.ErrNoLicense},
		{"env expired, DB empty → names expiry, not ErrNoLicense", expiredEnv, "", "", "", license.ErrLicenseExpired},
		{"env nil, DB invalid → error", "", "not-a-valid-key", "", "", errAnyResolveError},
	}
	for _, tc := range tests {
		//nolint:paralleltest // shares the package-level public key set above (SetPublicKeyForTesting)
		t.Run(tc.name, func(t *testing.T) {
			// Mirror provisioning_config.go: a set-but-unusable env key (empty or
			// expired) leaves envMgr nil; an invalid signature would have failed
			// config load before resolve is ever reached.
			var envMgr *license.Manager
			if tc.envKey != "" {
				m, err := license.NewManager(tc.envKey, log)
				switch {
				case err == nil:
					envMgr = m
				case errors.Is(err, license.ErrLicenseExpired), errors.Is(err, license.ErrNoLicense):
					// tolerated → envMgr stays nil
				default:
					t.Fatalf("env NewManager: %v", err)
				}
			}

			mgr, key, err := resolveProvisioningLicense(envMgr, tc.envKey, tc.dbKey, log)

			switch {
			case errors.Is(tc.wantErr, errAnyResolveError):
				if err == nil || mgr != nil {
					t.Fatalf("want an error, got mgr=%v err=%v", mgr, err)
				}
				return
			case tc.wantErr != nil:
				if !errors.Is(err, tc.wantErr) || mgr != nil {
					t.Fatalf("want %v + nil mgr, got mgr=%v err=%v", tc.wantErr, mgr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if key != tc.wantKey {
				t.Errorf("effective key = %q, want %q", key, tc.wantKey)
			}
			if mgr.Edition() != tc.wantEd {
				t.Errorf("edition = %q, want %q", mgr.Edition(), tc.wantEd)
			}
		})
	}
}
