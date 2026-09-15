package provapi

import (
	"bytes"
	"context"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/license"
)

func TestLicenseDowngradeShutdown_InvokesCancel(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cb := LicenseDowngradeShutdown(zerolog.Nop(), cancel)

	cb(license.Enterprise, license.Pro)

	select {
	case <-ctx.Done():
		// expected
	default:
		t.Error("cancel() was not called — context is still active")
	}
}

func TestLicenseDowngradeShutdown_LogsCorrectFields(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	cb := LicenseDowngradeShutdown(logger, cancel)
	cb(license.Enterprise, license.Pro)

	out := buf.String()
	if !bytes.Contains([]byte(out), []byte(`"from_edition":"enterprise"`)) {
		t.Errorf("log missing from_edition field: %s", out)
	}
	if !bytes.Contains([]byte(out), []byte(`"to_edition":"pro"`)) {
		t.Errorf("log missing to_edition field: %s", out)
	}
}

func TestLicenseDowngradeShutdown_SafeWithMultipleCalls(t *testing.T) {
	t.Parallel()
	_, cancel := context.WithCancel(context.Background())
	cb := LicenseDowngradeShutdown(zerolog.Nop(), cancel)

	// cancel() is idempotent — multiple calls must not panic.
	cb(license.Enterprise, license.Pro)
	cb(license.Enterprise, license.Community)
}
