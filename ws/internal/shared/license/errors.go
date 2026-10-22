package license

import (
	"errors"
	"fmt"
	"strings"
)

// UpgradeURL is the URL included in all edition error messages.
const UpgradeURL = "https://docs.sukko.dev/editions/upgrade"

// Sentinel errors for license validation failures.
var (
	// ErrNoLicense indicates no license key was provided. Community is no longer
	// keyless-by-default (ADR-0038): every service requires an account-issued key,
	// so an empty key fails construction and the service fails fast at startup.
	ErrNoLicense = errors.New("no license key: Community requires a free account-issued license — set SUKKO_LICENSE_KEY (get one at " + UpgradeURL + ")")

	// ErrLicenseExpired indicates the license key has passed its expiration date.
	ErrLicenseExpired = errors.New("license expired")

	// ErrLicenseInvalidSignature indicates the ECDSA P-256 signature verification failed.
	// This means the key is corrupt or was forged.
	ErrLicenseInvalidSignature = errors.New("license signature invalid")

	// ErrLicenseInvalidFormat indicates the license key is not in the expected
	// base64url(claims).base64url(signature) format.
	ErrLicenseInvalidFormat = errors.New("license format invalid")

	// ErrReplayDetected indicates the pushed key is a replay: its iat (issued-at)
	// is older than the current key's, or equal with different claims. Prevents
	// downgrade attacks with leaked older (or forged same-iat) keys.
	ErrReplayDetected = errors.New("license replay detected: iat not newer than current")

	// ErrLicenseUnchanged signals that a reload re-pushed the byte-equivalent current
	// key (same iat AND same claims) — an idempotent no-op, NOT a failure. It is
	// returned instead of applying a redundant swap, so callers that re-push the
	// current key on a schedule (`sukko up`'s push-service reconcile) or on every
	// stream reconnect can treat it as "already current" rather than a replay. Like
	// io.EOF, it is a signal carried as an error; callers check it with errors.Is.
	ErrLicenseUnchanged = errors.New("license unchanged: re-push of the current key")
)

// EditionLimitError is returned when a runtime operation exceeds an edition's
// hard limit (e.g., tenant count, connection count).
type EditionLimitError struct {
	// Dimension is the limit that was exceeded (e.g., "tenants", "total_connections").
	Dimension string

	// Current is the current count at the time of the check.
	Current int

	// Max is the edition's maximum allowed value.
	Max int

	// Edition is the active edition when the limit was hit.
	Edition Edition
}

// Error returns a human-readable message including the limit details and upgrade URL.
func (e *EditionLimitError) Error() string {
	return fmt.Sprintf("%s limit reached: %d/%d (%s edition). Upgrade at %s",
		e.Dimension, e.Current, e.Max, e.Edition, UpgradeURL)
}

// Code returns a machine-readable error code for API responses.
// Format: EDITION_LIMIT_{DIMENSION} (e.g., EDITION_LIMIT_TENANTS).
func (e *EditionLimitError) Code() string {
	return "EDITION_LIMIT_" + strings.ToUpper(e.Dimension)
}

// NewLimitError creates an EditionLimitError.
func NewLimitError(dimension string, current, limit int, edition Edition) *EditionLimitError {
	return &EditionLimitError{
		Dimension: dimension,
		Current:   current,
		Max:       limit,
		Edition:   edition,
	}
}

// EditionFeatureError is returned when a configuration references a feature
// that requires a higher edition than the current one.
type EditionFeatureError struct {
	// Feature is the gated feature that was requested.
	Feature Feature

	// RequiredEdition is the minimum edition that includes this feature.
	RequiredEdition Edition

	// CurrentEdition is the active edition.
	CurrentEdition Edition
}

// Error returns a human-readable message including the feature, required edition, and upgrade URL.
func (e *EditionFeatureError) Error() string {
	return fmt.Sprintf("%s requires %s edition (current: %s). Upgrade at %s",
		e.Feature, e.RequiredEdition, e.CurrentEdition, UpgradeURL)
}

// Code returns a machine-readable error code for API responses.
func (e *EditionFeatureError) Code() string {
	return "EDITION_FEATURE_REQUIRED"
}

// NewFeatureError creates an EditionFeatureError.
func NewFeatureError(feature Feature, currentEdition Edition) *EditionFeatureError {
	return &EditionFeatureError{
		Feature:         feature,
		RequiredEdition: RequiredEdition(feature),
		CurrentEdition:  currentEdition,
	}
}
