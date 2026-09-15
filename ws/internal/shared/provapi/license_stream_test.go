package provapi

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/license"
)

// newDowngradeTestWatcher creates a StreamLicenseWatcher for downgrade-drain unit tests.
// It uses a non-existent gRPC address so the stream loop stays disconnected.
// The manager is pre-loaded with Enterprise edition. Cleanup is registered automatically.
func newDowngradeTestWatcher(t *testing.T, metricPrefix string, cfg StreamLicenseWatcherConfig) *StreamLicenseWatcher {
	t.Helper()
	if cfg.Manager == nil {
		cfg.Manager = license.NewTestManager(license.Enterprise)
	}
	cfg.GRPCAddr = "localhost:19997"
	cfg.MetricPrefix = metricPrefix
	cfg.Logger = zerolog.Nop()
	w, err := NewStreamLicenseWatcher(cfg)
	if err != nil {
		t.Fatalf("NewStreamLicenseWatcher() error = %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func TestStreamLicenseWatcher_StartGracePeriod_SetsTimer(t *testing.T) {
	t.Parallel()
	w := newDowngradeTestWatcher(t, "test_drain_set", StreamLicenseWatcherConfig{
		DowngradeGracePeriod: 10 * time.Millisecond,
	})

	w.startGracePeriod(license.Enterprise, license.Pro)

	w.timerMu.Lock()
	timerSet := w.timer != nil
	w.timerMu.Unlock()

	if !timerSet {
		t.Error("timer should be set after startGracePeriod")
	}
}

func TestStreamLicenseWatcher_GracePeriodExpiry_CallsOnDowngrade(t *testing.T) {
	t.Parallel()
	called := make(chan struct{ from, to license.Edition }, 1)
	w := newDowngradeTestWatcher(t, "test_drain_expiry", StreamLicenseWatcherConfig{
		DowngradeGracePeriod: 20 * time.Millisecond,
		OnDowngrade: func(from, to license.Edition) {
			called <- struct{ from, to license.Edition }{from, to}
		},
	})

	w.startGracePeriod(license.Enterprise, license.Pro)

	select {
	case result := <-called:
		if result.from != license.Enterprise {
			t.Errorf("from = %v, want Enterprise", result.from)
		}
		if result.to != license.Pro {
			t.Errorf("to = %v, want Pro", result.to)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("OnDowngrade was not called within timeout")
	}
}

func TestStreamLicenseWatcher_CancelGracePeriod_StopsTimer(t *testing.T) {
	t.Parallel()
	var callCount atomic.Int32
	w := newDowngradeTestWatcher(t, "test_drain_cancel", StreamLicenseWatcherConfig{
		DowngradeGracePeriod: 100 * time.Millisecond,
		OnDowngrade: func(from, to license.Edition) {
			callCount.Add(1)
		},
	})

	w.startGracePeriod(license.Enterprise, license.Pro)
	w.cancelGracePeriod()

	// Wait longer than the timer duration to confirm callback was not called.
	time.Sleep(200 * time.Millisecond)

	if n := callCount.Load(); n != 0 {
		t.Errorf("OnDowngrade called %d times after cancel, want 0", n)
	}

	w.timerMu.Lock()
	timerNil := w.timer == nil
	w.timerMu.Unlock()

	if !timerNil {
		t.Error("timer should be nil after cancelGracePeriod")
	}
}

func TestStreamLicenseWatcher_DuplicateDowngrade_NoTimerReset(t *testing.T) {
	t.Parallel()
	var callCount atomic.Int32
	w := newDowngradeTestWatcher(t, "test_drain_dup", StreamLicenseWatcherConfig{
		DowngradeGracePeriod: 50 * time.Millisecond,
		OnDowngrade: func(from, to license.Edition) {
			callCount.Add(1)
		},
	})

	w.startGracePeriod(license.Enterprise, license.Pro)

	// Capture timer pointer after first call.
	w.timerMu.Lock()
	t1 := w.timer
	w.timerMu.Unlock()

	// Second downgrade event — must be a no-op (timer already running).
	w.startGracePeriod(license.Enterprise, license.Pro)

	w.timerMu.Lock()
	t2 := w.timer
	w.timerMu.Unlock()

	if t1 != t2 {
		t.Error("timer pointer changed on second startGracePeriod — timer was reset unexpectedly")
	}

	// Wait for timer to fire and verify OnDowngrade fires exactly once.
	time.Sleep(200 * time.Millisecond)
	if n := callCount.Load(); n != 1 {
		t.Errorf("OnDowngrade called %d times, want exactly 1", n)
	}
}

func TestStreamLicenseWatcher_OnDowngrade_FiresAtMostOnce(t *testing.T) {
	t.Parallel()
	var callCount atomic.Int32
	called := make(chan struct{}, 1)
	w := newDowngradeTestWatcher(t, "test_drain_once", StreamLicenseWatcherConfig{
		DowngradeGracePeriod: 20 * time.Millisecond,
		OnDowngrade: func(from, to license.Edition) {
			callCount.Add(1)
			select {
			case called <- struct{}{}:
			default:
			}
		},
	})

	// 10 goroutines call startGracePeriod concurrently — sync.Once in the
	// timer callback must ensure OnDowngrade fires exactly once regardless.
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			w.startGracePeriod(license.Enterprise, license.Pro)
		})
	}
	wg.Wait()

	select {
	case <-called:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("OnDowngrade was not called within timeout")
	}

	// Drain the grace period so any leaked timers can fire, then assert count.
	time.Sleep(100 * time.Millisecond)
	if n := callCount.Load(); n != 1 {
		t.Errorf("OnDowngrade called %d times, want exactly 1", n)
	}
}

func TestStreamLicenseWatcher_NilOnDowngrade_NoPanic(t *testing.T) {
	t.Parallel()
	w := newDowngradeTestWatcher(t, "test_drain_nil", StreamLicenseWatcherConfig{
		DowngradeGracePeriod: 20 * time.Millisecond,
		OnDowngrade:          nil,
	})

	// Must not panic — nil OnDowngrade logs a Warn and returns.
	w.startGracePeriod(license.Enterprise, license.Pro)
	time.Sleep(100 * time.Millisecond) // let timer fire
}

func TestStreamLicenseWatcher_CancelGracePeriod_NoopWhenNoTimer(t *testing.T) {
	t.Parallel()
	w := newDowngradeTestWatcher(t, "test_drain_cancel_noop", StreamLicenseWatcherConfig{})

	// Must not panic or block.
	w.cancelGracePeriod()

	w.timerMu.Lock()
	timerNil := w.timer == nil
	w.timerMu.Unlock()
	if !timerNil {
		t.Error("timer should remain nil after cancelGracePeriod with no timer running")
	}
}

func TestStreamLicenseWatcher_ZeroGracePeriod_UsesDefault(t *testing.T) {
	t.Parallel()
	var called atomic.Bool
	w := newDowngradeTestWatcher(t, "test_drain_zero", StreamLicenseWatcherConfig{
		DowngradeGracePeriod: 0, // production default — must resolve to DefaultDowngradeGracePeriod (14 days)
		OnDowngrade: func(from, to license.Edition) {
			called.Store(true)
		},
	})

	w.startGracePeriod(license.Enterprise, license.Pro)

	// Verify timer was created.
	w.timerMu.Lock()
	timerSet := w.timer != nil
	w.timerMu.Unlock()
	if !timerSet {
		t.Fatal("timer should be set after startGracePeriod with zero DowngradeGracePeriod")
	}

	// With DefaultDowngradeGracePeriod (14 days), callback must NOT fire within 100ms.
	time.Sleep(100 * time.Millisecond)
	if called.Load() {
		t.Error("OnDowngrade fired immediately — zero DowngradeGracePeriod was not resolved to DefaultDowngradeGracePeriod")
	}

	// Explicitly cancel to avoid the 14-day timer leaking after the test.
	w.cancelGracePeriod()
}

func TestStreamLicenseWatcher_PostExpiryRenewal_Noop(t *testing.T) {
	t.Parallel()
	var callCount atomic.Int32
	w := newDowngradeTestWatcher(t, "test_drain_postexpiry", StreamLicenseWatcherConfig{
		DowngradeGracePeriod: 20 * time.Millisecond,
		OnDowngrade: func(from, to license.Edition) {
			callCount.Add(1)
		},
	})

	// Start grace period and let it fire.
	w.startGracePeriod(license.Enterprise, license.Pro)
	time.Sleep(100 * time.Millisecond) // wait for timer to fire

	if n := callCount.Load(); n != 1 {
		t.Fatalf("expected OnDowngrade to fire once before renewal, got %d", n)
	}

	// Simulate post-expiry renewal — cancelGracePeriod must not panic.
	w.cancelGracePeriod()

	// Verify OnDowngrade was not called a second time.
	if n := callCount.Load(); n != 1 {
		t.Errorf("OnDowngrade called %d times after post-expiry renewal, want 1", n)
	}

	w.timerMu.Lock()
	timerNil := w.timer == nil
	w.timerMu.Unlock()
	if !timerNil {
		t.Error("timer should be nil after post-expiry cancelGracePeriod")
	}
}

func TestNewStreamLicenseWatcher_Validation(t *testing.T) {
	t.Parallel()

	logger := zerolog.Nop()
	mgr, _ := license.NewManager("", logger)

	tests := []struct {
		name    string
		cfg     StreamLicenseWatcherConfig
		wantErr string
	}{
		{
			name:    "missing GRPCAddr",
			cfg:     StreamLicenseWatcherConfig{Manager: mgr, Logger: logger},
			wantErr: "GRPCAddr is required",
		},
		{
			name:    "missing Manager",
			cfg:     StreamLicenseWatcherConfig{GRPCAddr: "localhost:9090", Logger: logger},
			wantErr: "Manager is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w, err := NewStreamLicenseWatcher(tt.cfg)
			if err == nil {
				_ = w.Close()
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if got := err.Error(); !strings.Contains(got, tt.wantErr) {
				t.Errorf("error = %q, want substring %q", got, tt.wantErr)
			}
		})
	}
}

func TestNewStreamLicenseWatcher_Defaults(t *testing.T) {
	t.Parallel()

	logger := zerolog.Nop()
	mgr, _ := license.NewManager("", logger)

	// Use an address that won't connect (lazy dial) — we just test config defaults
	w, err := NewStreamLicenseWatcher(StreamLicenseWatcherConfig{
		GRPCAddr: "localhost:19999",
		Manager:  mgr,
		Logger:   logger,
		// Leave ReconnectDelay, ReconnectMaxDelay, MetricPrefix as zero
	})
	if err != nil {
		t.Fatalf("NewStreamLicenseWatcher() error = %v", err)
	}
	defer func() { _ = w.Close() }()

	if w.config.ReconnectDelay != 5*time.Second {
		t.Errorf("ReconnectDelay = %v, want 5s", w.config.ReconnectDelay)
	}
	if w.config.ReconnectMaxDelay != 2*time.Minute {
		t.Errorf("ReconnectMaxDelay = %v, want 2m", w.config.ReconnectMaxDelay)
	}
	if w.config.MetricPrefix != "unknown" {
		t.Errorf("MetricPrefix = %q, want %q", w.config.MetricPrefix, "unknown")
	}
}

func TestStreamLicenseWatcher_CloseStopsGoroutine(t *testing.T) {
	t.Parallel()

	logger := zerolog.Nop()
	mgr, _ := license.NewManager("", logger)

	w, err := NewStreamLicenseWatcher(StreamLicenseWatcherConfig{
		GRPCAddr:     "localhost:19999",
		Manager:      mgr,
		MetricPrefix: "test_close",
		Logger:       logger,
	})
	if err != nil {
		t.Fatalf("NewStreamLicenseWatcher() error = %v", err)
	}

	// Close should not hang — it cancels the context and waits for the goroutine
	done := make(chan struct{})
	go func() {
		_ = w.Close()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("Close() did not return within 5 seconds — goroutine leak")
	}
}

func TestStreamLicenseWatcher_InitialStateDisconnected(t *testing.T) {
	t.Parallel()

	logger := zerolog.Nop()
	mgr, _ := license.NewManager("", logger)

	w, err := NewStreamLicenseWatcher(StreamLicenseWatcherConfig{
		GRPCAddr:     "localhost:19999",
		Manager:      mgr,
		MetricPrefix: "test_state",
		Logger:       logger,
	})
	if err != nil {
		t.Fatalf("NewStreamLicenseWatcher() error = %v", err)
	}
	defer func() { _ = w.Close() }()

	// Initial state should be disconnected (0) since no server is running
	if got := w.State(); got != StreamStateDisconnected {
		t.Errorf("initial State() = %d, want %d (disconnected)", got, StreamStateDisconnected)
	}
}

// newExpiryTestManager creates a Manager with a real signed key for expiry watcher tests.
// The key has the given edition and expiry time. SetPublicKeyForTesting is called on the
// test-generated public key; the manager is fully functional for claims inspection.
// Tests using this helper MUST NOT call t.Parallel() — SetPublicKeyForTesting mutates
// package-level state in the license package.
func newExpiryTestManager(t *testing.T, edition license.Edition, exp int64) (*license.Manager, error) {
	t.Helper()
	priv, pub := license.GenerateTestKeyPair()
	license.SetPublicKeyForTesting(pub)
	claims := license.Claims{
		Edition: edition,
		Org:     "ExpiryTestOrg",
		Exp:     exp,
		Iat:     exp - 3600,
	}
	key := license.SignTestLicense(claims, priv)
	return license.NewManager(key, zerolog.Nop())
}

//nolint:paralleltest // uses newExpiryTestManager which calls SetPublicKeyForTesting (package-level state)
func TestStreamLicenseWatcher_ExpiryWatcher_BootstrapSetsCancel(t *testing.T) {
	// Non-expired claims at construction → expiryCancel must be non-nil.
	mgr, err := newExpiryTestManager(t, license.Enterprise, time.Now().Add(time.Hour).Unix())
	if err != nil {
		t.Fatalf("newExpiryTestManager() error = %v", err)
	}

	w := newDowngradeTestWatcher(t, "test_expiry_bootstrap", StreamLicenseWatcherConfig{
		Manager: mgr,
	})

	w.timerMu.Lock()
	cancelSet := w.expiryCancel != nil
	w.timerMu.Unlock()

	if !cancelSet {
		t.Error("expiryCancel should be non-nil after construction with non-expired claims")
	}
}

//nolint:paralleltest // calls SetPublicKeyForTesting directly (package-level state)
func TestStreamLicenseWatcher_ExpiryWatcher_AlreadyExpiredSkipped(t *testing.T) {
	// Expired claims at construction → bootstrap must skip, expiryCancel remains nil.
	// NewManager with expired key degrades to Community (Edition() == Community).
	// We still need Claims() to be non-nil — expired claims are preserved.
	priv, pub := license.GenerateTestKeyPair()
	license.SetPublicKeyForTesting(pub)
	expiredClaims := license.Claims{
		Edition: license.Enterprise,
		Org:     "ExpiredOrg",
		Exp:     time.Now().Add(-time.Hour).Unix(),
		Iat:     time.Now().Add(-2 * time.Hour).Unix(),
	}
	key := license.SignTestLicense(expiredClaims, priv)
	mgr, err := license.NewManager(key, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	w := newDowngradeTestWatcher(t, "test_expiry_skip", StreamLicenseWatcherConfig{
		Manager: mgr,
	})

	w.timerMu.Lock()
	cancelSet := w.expiryCancel != nil
	w.timerMu.Unlock()

	if cancelSet {
		t.Error("expiryCancel should be nil when claims are already expired at construction")
	}
}

//nolint:paralleltest // uses newExpiryTestManager which calls SetPublicKeyForTesting (package-level state)
func TestStreamLicenseWatcher_ExpiryWatcher_FiresStartsGracePeriod(t *testing.T) {
	// Short deadline → expiry goroutine fires → OnDowngrade called within timeout.
	mgr, err := newExpiryTestManager(t, license.Enterprise, time.Now().Add(50*time.Millisecond).Unix())
	if err != nil {
		t.Fatalf("newExpiryTestManager() error = %v", err)
	}

	called := make(chan struct{ from, to license.Edition }, 1)
	w := newDowngradeTestWatcher(t, "test_expiry_fires", StreamLicenseWatcherConfig{
		Manager:              mgr,
		DowngradeGracePeriod: 20 * time.Millisecond,
		OnDowngrade: func(from, to license.Edition) {
			called <- struct{ from, to license.Edition }{from, to}
		},
	})
	_ = w // bootstrap already scheduled via NewStreamLicenseWatcher

	select {
	case result := <-called:
		if result.to != license.Community {
			t.Errorf("OnDowngrade to = %v, want Community", result.to)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnDowngrade was not called within timeout after expiry")
	}
}

func TestStreamLicenseWatcher_ExpiryWatcher_RescheduleOnRenewal(t *testing.T) {
	t.Parallel()
	// Calling scheduleExpiryWatcher twice → second cancels first goroutine.
	// OnDowngrade must fire at most once.
	var callCount atomic.Int32
	w := newDowngradeTestWatcher(t, "test_expiry_resched", StreamLicenseWatcherConfig{
		DowngradeGracePeriod: 30 * time.Millisecond,
		OnDowngrade: func(from, to license.Edition) {
			callCount.Add(1)
		},
	})

	// First schedule with a 2-second future expiry (Unix second precision — must be >= 1s ahead)
	w.scheduleExpiryWatcher(time.Now().Unix() + 2)

	// Second schedule immediately cancels the first goroutine and launches a new one with a long expiry
	w.scheduleExpiryWatcher(time.Now().Add(time.Hour).Unix())

	// Wait long enough for the first goroutine's original deadline to have fired (if not canceled)
	time.Sleep(500 * time.Millisecond)

	// OnDowngrade must not have fired — first goroutine was canceled before its deadline
	if n := callCount.Load(); n != 0 {
		t.Errorf("OnDowngrade called %d times after reschedule, want 0 (first goroutine should have been canceled)", n)
	}
}

func TestStreamLicenseWatcher_ExpiryWatcher_ZeroExpCancelsOnly(t *testing.T) {
	t.Parallel()
	// scheduleExpiryWatcher(0) → cancel prior goroutine, no new goroutine launched.
	var callCount atomic.Int32
	w := newDowngradeTestWatcher(t, "test_expiry_zero", StreamLicenseWatcherConfig{
		DowngradeGracePeriod: 20 * time.Millisecond,
		OnDowngrade: func(from, to license.Edition) {
			callCount.Add(1)
		},
	})

	// Start an expiry goroutine with a 2-second future deadline (Unix second precision)
	w.scheduleExpiryWatcher(time.Now().Unix() + 2)

	w.timerMu.Lock()
	hadCancel := w.expiryCancel != nil
	w.timerMu.Unlock()

	if !hadCancel {
		t.Fatal("expiryCancel should be set before zero reschedule")
	}

	// scheduleExpiryWatcher(0) should cancel the goroutine and set expiryCancel to nil
	w.scheduleExpiryWatcher(0)

	w.timerMu.Lock()
	cancelNil := w.expiryCancel == nil
	w.timerMu.Unlock()

	if !cancelNil {
		t.Error("expiryCancel should be nil after scheduleExpiryWatcher(0)")
	}

	// No OnDowngrade should fire — goroutine was canceled
	time.Sleep(200 * time.Millisecond)
	if n := callCount.Load(); n != 0 {
		t.Errorf("OnDowngrade called %d times after zero exp reschedule, want 0", n)
	}
}

func TestStreamLicenseWatcher_CancelGracePeriodLocked_NilTimer(t *testing.T) {
	t.Parallel()
	// cancelGracePeriodLocked with nil timer must not panic.
	w := newDowngradeTestWatcher(t, "test_locked_nil", StreamLicenseWatcherConfig{})

	w.timerMu.Lock()
	defer w.timerMu.Unlock()
	// Must not panic — nil guard in cancelGracePeriodLocked
	w.cancelGracePeriodLocked()
}

func TestStreamLicenseWatcher_CloseStopsExpiryWatcher(t *testing.T) {
	t.Parallel()
	// Close() must return without deadlock even when an expiry goroutine is running.
	w := newDowngradeTestWatcher(t, "test_close_expiry", StreamLicenseWatcherConfig{
		DowngradeGracePeriod: time.Hour, // long grace period so the goroutine stays blocked
	})

	// Launch an expiry goroutine with a far-future deadline
	w.scheduleExpiryWatcher(time.Now().Add(time.Hour).Unix())

	done := make(chan struct{})
	go func() {
		_ = w.Close()
		close(done)
	}()

	select {
	case <-done:
		// success — Close() drained the goroutine via wg.Wait()
	case <-time.After(5 * time.Second):
		t.Fatal("Close() did not return within 5 seconds — expiry goroutine leak")
	}
}
