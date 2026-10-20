// Package license provides edition detection, feature gating, and hard limit
// enforcement for the Sukko platform's Community/Pro/Enterprise editions.
//
// The edition is determined by the SUKKO_LICENSE_KEY environment variable, which is
// required — every edition, Community included, runs on an account-issued signed key
// (ADR-0038). The key is validated once at startup (and on each live reload); the booted
// edition is fixed for the process lifetime and is never downgraded mid-flight.
//   - Valid key → the edition encoded in its claims (Community, Pro, or Enterprise)
//   - No key → startup failure (Community is no longer the keyless default)
//   - Expired key → startup failure (an operator must re-key before the service will boot)
//   - Invalid key → startup failure (corrupt or forged)
package license

// Edition represents a Sukko product tier.
type Edition string

const (
	// Community is the free edition with hard limits. It is the zero-value default.
	Community Edition = "community"

	// Pro is the mid-tier commercial edition.
	Pro Edition = "pro"

	// Enterprise is the top-tier commercial edition with unlimited scale.
	Enterprise Edition = "enterprise"
)

// editionRank maps each edition to a numeric rank for ordering comparisons.
// Higher rank = more features/limits.
var editionRank = map[Edition]int{
	Community:  0,
	Pro:        1,
	Enterprise: 2,
}

// String returns the edition name. For the zero value (""), returns "community".
func (e Edition) String() string {
	if e == "" {
		return string(Community)
	}
	return string(e)
}

// IsAtLeast returns true if this edition is at or above the given edition.
// Community < Pro < Enterprise.
func (e Edition) IsAtLeast(other Edition) bool {
	return editionRank[e.normalize()] >= editionRank[other.normalize()]
}

// normalize returns Community for the zero value, otherwise the edition as-is.
func (e Edition) normalize() Edition {
	if e == "" {
		return Community
	}
	return e
}
