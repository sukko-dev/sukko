package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/logging"
)

const (
	// pushReceiverShutdownTimeout bounds graceful shutdown of the receiver's
	// HTTP server during test cleanup.
	pushReceiverShutdownTimeout = 2 * time.Second

	// maxPushReceiverBody caps how much of a delivered push body is read —
	// WebPush payloads are ≤4KB by spec; anything larger is a misbehaving peer.
	maxPushReceiverBody = 64 * 1024
)

// pushReceivedRequest records one delivery POST the mock receiver accepted.
type pushReceivedRequest struct {
	Path            string
	BodyLen         int
	ContentEncoding string // WebPush payload encryption scheme (e.g. "aes128gcm")
}

// pushReceiver is a minimal mock WebPush endpoint (RFC 8030 receiver side):
// it records every POST and responds 201 Created, letting the push suite
// assert that the push service actually DELIVERS — not merely accepts —
// notifications. Test-support only; never part of a deployed service.
type pushReceiver struct {
	server *http.Server
	addr   net.Addr
	logger zerolog.Logger

	mu       sync.Mutex
	requests []pushReceivedRequest

	wg sync.WaitGroup
}

// startPushReceiver listens on the given port on all interfaces and serves
// the mock endpoint until Close. Port 0 picks an ephemeral port (tests). The
// port default (8095) lives in the tester config (TESTER_PUSH_RECEIVER_PORT).
func startPushReceiver(ctx context.Context, port int, logger zerolog.Logger) (*pushReceiver, error) {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("push receiver listen on :%d: %w", port, err)
	}

	r := &pushReceiver{
		addr:   ln.Addr(),
		logger: logger.With().Str("component", "push_receiver").Logger(),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", r.handleDelivery)
	r.server = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second, // §IX: bound header reads
	}

	r.wg.Go(func() {
		defer logging.RecoverPanic(r.logger, "push_receiver_serve", nil)
		if serveErr := r.server.Serve(ln); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			r.logger.Warn().Err(serveErr).Msg("push receiver serve ended with error")
		}
	})

	return r, nil
}

// handleDelivery records a WebPush delivery and responds 201 Created (RFC 8030).
func (r *pushReceiver) handleDelivery(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(req.Body, maxPushReceiverBody))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	r.mu.Lock()
	r.requests = append(r.requests, pushReceivedRequest{
		Path:            req.URL.Path,
		BodyLen:         len(body),
		ContentEncoding: req.Header.Get("Content-Encoding"),
	})
	r.mu.Unlock()

	r.logger.Debug().
		Str("path", req.URL.Path).
		Int("body_len", len(body)).
		Msg("push delivery received")

	w.WriteHeader(http.StatusCreated)
}

// Requests returns a snapshot of recorded deliveries.
func (r *pushReceiver) Requests() []pushReceivedRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]pushReceivedRequest, len(r.requests))
	copy(out, r.requests)
	return out
}

// Port returns the port the receiver is listening on (useful with port 0).
func (r *pushReceiver) Port() int {
	if tcp, ok := r.addr.(*net.TCPAddr); ok {
		return tcp.Port
	}
	return 0
}

// Close shuts the receiver down gracefully and waits for the serve goroutine.
// Safe to call once; test-scoped lifecycle (started and closed within a check).
func (r *pushReceiver) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), pushReceiverShutdownTimeout)
	defer cancel()
	err := r.server.Shutdown(ctx)
	r.wg.Wait()
	if err != nil {
		return fmt.Errorf("push receiver shutdown: %w", err)
	}
	return nil
}
