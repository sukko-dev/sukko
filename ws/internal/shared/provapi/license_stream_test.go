package provapi

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"

	provisioningv1 "github.com/sukko-dev/sukko/gen/proto/sukko/provisioning/v1"
	"github.com/sukko-dev/sukko/internal/shared/license"
)

// mockLicenseServer is a minimal in-process WatchLicense server for driving the
// StreamLicenseWatcher end-to-end: it sends the configured keys in order on stream
// open, closes `sent`, then holds the stream until the client disconnects.
type mockLicenseServer struct {
	provisioningv1.UnimplementedProvisioningInternalServiceServer
	keys []string
	sent chan struct{}
}

func (s *mockLicenseServer) WatchLicense(_ *provisioningv1.WatchLicenseRequest, stream grpc.ServerStreamingServer[provisioningv1.WatchLicenseResponse]) error {
	for _, k := range s.keys {
		if err := stream.Send(&provisioningv1.WatchLicenseResponse{LicenseKey: k}); err != nil {
			return err
		}
	}
	close(s.sent)
	<-stream.Context().Done()
	return stream.Context().Err()
}

// startMockLicenseServer starts the mock on an ephemeral loopback port and returns
// its address. The returned `sent` channel closes once all keys have been sent.
func startMockLicenseServer(t *testing.T, keys ...string) (addr string, sent chan struct{}) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	sent = make(chan struct{})
	srv := grpc.NewServer()
	provisioningv1.RegisterProvisioningInternalServiceServer(srv, &mockLicenseServer{keys: keys, sent: sent})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String(), sent
}

// mustWatcher builds a watcher pointed at addr with fast reconnect for tests.
func mustWatcher(t *testing.T, addr string, mgr *license.Manager, metricPrefix string) *StreamLicenseWatcher {
	t.Helper()
	w, err := NewStreamLicenseWatcher(StreamLicenseWatcherConfig{
		GRPCAddr:          addr,
		Manager:           mgr,
		MetricPrefix:      metricPrefix,
		Logger:            zerolog.Nop(),
		ReconnectDelay:    50 * time.Millisecond,
		ReconnectMaxDelay: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewStreamLicenseWatcher() error = %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func waitForEdition(t *testing.T, mgr *license.Manager, want license.Edition, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if mgr.Edition() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("edition did not reach %s within %v (got %s)", want, timeout, mgr.Edition())
}

// TestStreamLicenseWatcher_Upgrade_AppliedLive proves the watcher actually calls
// ReloadNoDowngrade on a stream update: a Pro key streamed to a Community-booted
// client raises the live edition to Pro (ADR-0038 — upgrades apply live).
//
//nolint:paralleltest // SetPublicKeyForTesting mutates package-level license state
func TestStreamLicenseWatcher_Upgrade_AppliedLive(t *testing.T) {
	priv, pub := license.GenerateTestKeyPair()
	license.SetPublicKeyForTesting(pub)
	exp := time.Now().Add(365 * 24 * time.Hour).Unix()
	pro := license.SignTestLicense(license.Claims{Edition: license.Pro, Org: "Acme", Exp: exp, Iat: 100}, priv)

	mgr := license.NewTestManager(license.Community)
	addr, _ := startMockLicenseServer(t, pro)
	mustWatcher(t, addr, mgr, "test_stream_upgrade")

	waitForEdition(t, mgr, license.Pro, 3*time.Second)
}

// TestStreamLicenseWatcher_Downgrade_NotAppliedLive proves the watcher defers a
// downgrade that arrives as a LIVE UPDATE (not the boot snapshot): the stream sends a
// benign Enterprise snapshot first (consuming the one-shot boot resolution), then a Pro
// key as a live update, which must NOT lower the running edition (ADR-0038 Model C).
// Discriminating guard — reverting the live-update path to the unconditional Reload
// flips the edition to Pro and fails here.
//
//nolint:paralleltest // SetPublicKeyForTesting mutates package-level license state
func TestStreamLicenseWatcher_Downgrade_NotAppliedLive(t *testing.T) {
	priv, pub := license.GenerateTestKeyPair()
	license.SetPublicKeyForTesting(pub)
	exp := time.Now().Add(365 * 24 * time.Hour).Unix()
	entSnapshot := license.SignTestLicense(license.Claims{Edition: license.Enterprise, Org: "Acme", Exp: exp, Iat: 100}, priv)
	proUpdate := license.SignTestLicense(license.Claims{Edition: license.Pro, Org: "Acme", Exp: exp, Iat: 200}, priv)

	mgr := license.NewTestManager(license.Enterprise)
	// Ordered: snapshot (boot resolution, Enterprise — no edition change) then the Pro
	// downgrade as a live update.
	addr, sent := startMockLicenseServer(t, entSnapshot, proUpdate)
	mustWatcher(t, addr, mgr, "test_stream_downgrade")

	// Both keys sent (ordered); on loopback the client receives and processes them
	// synchronously. The short settle bounds "both processed"; a buggy live-downgrade
	// of the second key would flip the edition within that window.
	select {
	case <-sent:
	case <-time.After(3 * time.Second):
		t.Fatal("mock server did not send the keys within 3s")
	}
	time.Sleep(300 * time.Millisecond)

	if got := mgr.Edition(); got != license.Enterprise {
		t.Fatalf("Edition() = %s, want Enterprise (live-update downgrade must not apply)", got)
	}
}

// TestStreamLicenseWatcher_FirstSnapshotConverges proves the boot-resolution rule: the
// FIRST stream key is applied unconditionally, so a service that booted from its env key
// at a HIGHER edition converges DOWN to the authoritative (lower) edition the stream
// reports — this is how "a downgrade takes effect at the next restart" actually reaches
// server/gateway/push, which boot from SUKKO_LICENSE_KEY and only then learn the DB key
// (ADR-0038). Under the buggy "ReloadNoDowngrade on every message" the snapshot would be
// refused and the edition would stay Enterprise, failing this test.
//
//nolint:paralleltest // SetPublicKeyForTesting mutates package-level license state
func TestStreamLicenseWatcher_FirstSnapshotConverges(t *testing.T) {
	priv, pub := license.GenerateTestKeyPair()
	license.SetPublicKeyForTesting(pub)
	exp := time.Now().Add(365 * 24 * time.Hour).Unix()
	community := license.SignTestLicense(license.Claims{Edition: license.Community, Org: "Acme", Exp: exp, Iat: 50}, priv)

	mgr := license.NewTestManager(license.Enterprise) // booted higher than the authoritative DB edition
	addr, _ := startMockLicenseServer(t, community)
	mustWatcher(t, addr, mgr, "test_stream_converge")

	waitForEdition(t, mgr, license.Community, 3*time.Second)
}

func TestNewStreamLicenseWatcher_Validation(t *testing.T) {
	t.Parallel()

	logger := zerolog.Nop()
	mgr := license.NewTestManager(license.Community)

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
	mgr := license.NewTestManager(license.Community)

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
	mgr := license.NewTestManager(license.Community)

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
	mgr := license.NewTestManager(license.Community)

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
