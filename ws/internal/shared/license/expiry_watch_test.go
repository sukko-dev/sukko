package license

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestClassifyExpiry(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0)
	window := 30 * 24 * time.Hour

	tests := []struct {
		name    string
		claim   *Claims
		want    expiryState
		wantRem time.Duration // expected signed remaining (now and Exp are whole seconds, so exact)
	}{
		{"nil claims → OK", nil, expiryOK, 0},
		{"zero exp (no expiry recorded) → OK", &Claims{Exp: 0}, expiryOK, 0},
		{"far future → OK", &Claims{Exp: now.Add(365 * 24 * time.Hour).Unix()}, expiryOK, 365 * 24 * time.Hour},
		{"just outside window (31d) → OK", &Claims{Exp: now.Add(31 * 24 * time.Hour).Unix()}, expiryOK, 31 * 24 * time.Hour},
		{"inside window (10d) → Warn", &Claims{Exp: now.Add(10 * 24 * time.Hour).Unix()}, expiryWarn, 10 * 24 * time.Hour},
		{"at window edge (30d) → Warn", &Claims{Exp: now.Add(window).Unix()}, expiryWarn, window},
		{"expired (yesterday) → Expired", &Claims{Exp: now.Add(-24 * time.Hour).Unix()}, expiryExpired, -24 * time.Hour},
		{"expired exactly now → Expired", &Claims{Exp: now.Unix()}, expiryExpired, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, rem := classifyExpiry(tt.claim, now, window)
			if got != tt.want {
				t.Errorf("classifyExpiry(%+v) state = %v, want %v", tt.claim, got, tt.want)
			}
			if rem != tt.wantRem {
				t.Errorf("classifyExpiry(%+v) remaining = %v, want %v", tt.claim, rem, tt.wantRem)
			}
		})
	}
}

func TestLogExpiryState_Silent(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0)
	for _, tt := range []struct {
		name  string
		claim *Claims
	}{
		{"far future → silent", &Claims{Edition: Pro, Exp: now.Add(365 * 24 * time.Hour).Unix()}},
		{"nil claims → silent", nil},
		{"zero exp → silent", &Claims{Edition: Pro, Exp: 0}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			logExpiryState(zerolog.New(&buf), Pro, tt.claim, now)
			if buf.String() != "" {
				t.Errorf("expected no log, got %q", buf.String())
			}
		})
	}
}

// TestLogExpiryState_Fields decodes the JSON log line and asserts the computed numeric
// fields — guarding the sign and the /24 divisor that bare substring checks miss.
func TestLogExpiryState_Fields(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0)

	t.Run("expiring soon → days_left", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		// 10 days minus an hour → ceil → still 10 days left (never rounds down a valid license).
		logExpiryState(zerolog.New(&buf), Pro, &Claims{Edition: Pro, Exp: now.Add(10*24*time.Hour - time.Hour).Unix()}, now)
		var line map[string]any
		if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
			t.Fatalf("decode log line %q: %v", buf.String(), err)
		}
		if line["days_left"] != float64(10) {
			t.Errorf("days_left = %v, want 10", line["days_left"])
		}
		if line["days_expired"] != nil {
			t.Errorf("days_expired should be absent on a not-yet-expired license, got %v", line["days_expired"])
		}
	})

	t.Run("expired → days_expired", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		logExpiryState(zerolog.New(&buf), Enterprise, &Claims{Edition: Enterprise, Exp: now.Add(-48 * time.Hour).Unix()}, now)
		var line map[string]any
		if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
			t.Fatalf("decode log line %q: %v", buf.String(), err)
		}
		if line["days_expired"] != float64(2) {
			t.Errorf("days_expired = %v, want 2", line["days_expired"])
		}
		if !strings.Contains(buf.String(), "EXPIRED") {
			t.Errorf("expected EXPIRED message, got %q", buf.String())
		}
	})
}

// TestLogExpiry_ReloadQuietsWarning: the whole feature's value rests on a renewal
// silencing the warning (a stale warning after re-key trains operators to ignore it).
// logExpiry reads LIVE claims, so a reload to a far-future key must quiet it.
//
//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestLogExpiry_ReloadQuietsWarning(t *testing.T) {
	priv, pub := GenerateTestKeyPair()
	SetPublicKeyForTesting(pub)

	near := SignTestLicense(Claims{Edition: Pro, Org: "Acme", Exp: time.Now().Add(10 * 24 * time.Hour).Unix(), Iat: 1}, priv)
	mgr, err := NewManager(near, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	var before bytes.Buffer
	logExpiry(mgr, zerolog.New(&before), time.Now())
	if !strings.Contains(before.String(), "expiring soon") {
		t.Fatalf("precondition: near-expiry license should warn, got %q", before.String())
	}

	renewed := SignTestLicense(Claims{Edition: Pro, Org: "Acme", Exp: time.Now().Add(365 * 24 * time.Hour).Unix(), Iat: 2}, priv)
	if err := mgr.Reload(renewed); err != nil {
		t.Fatalf("Reload(renewed): %v", err)
	}

	var after bytes.Buffer
	logExpiry(mgr, zerolog.New(&after), time.Now())
	if after.String() != "" {
		t.Errorf("after renewal the warning must be silent, got %q", after.String())
	}
}

// TestExpiryWatcher_LogsAtStartAndClosesCleanly: the watcher emits the warning once
// immediately (not only after a 24h tick) for a near-expiry license, and Close() returns
// promptly. Uses a real near-expiry Manager (NewManager accepts a valid, not-yet-expired
// key) so the goroutine, RecoverPanic, and lifecycle are exercised end to end.
//
//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestExpiryWatcher_LogsAtStartAndClosesCleanly(t *testing.T) {
	priv, pub := GenerateTestKeyPair()
	SetPublicKeyForTesting(pub)
	key := SignTestLicense(Claims{
		Edition: Pro,
		Org:     "Acme",
		Exp:     time.Now().Add(10 * 24 * time.Hour).Unix(), // within the 30d window
		Iat:     time.Now().Unix(),
	}, priv)
	mgr, err := NewManager(key, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	var buf bytes.Buffer
	w := NewExpiryWatcher(mgr, zerolog.New(&buf))

	done := make(chan struct{})
	go func() { w.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close() did not return within 5s — goroutine leak")
	}

	if !strings.Contains(buf.String(), "expiring soon") {
		t.Errorf("expected a startup expiry warning, got %q", buf.String())
	}
}

// TestExpiryWatcher_FarFutureSilent: a license well outside the window logs nothing.
//
//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestExpiryWatcher_FarFutureSilent(t *testing.T) {
	priv, pub := GenerateTestKeyPair()
	SetPublicKeyForTesting(pub)
	key := SignTestLicense(Claims{
		Edition: Pro,
		Org:     "Acme",
		Exp:     time.Now().Add(365 * 24 * time.Hour).Unix(),
		Iat:     time.Now().Unix(),
	}, priv)
	mgr, err := NewManager(key, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	var buf bytes.Buffer
	w := NewExpiryWatcher(mgr, zerolog.New(&buf))
	w.Close()

	if buf.String() != "" {
		t.Errorf("expected no log for a far-future license, got %q", buf.String())
	}
}
