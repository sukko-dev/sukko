package sse

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// Event represents a parsed SSE event.
type Event struct {
	ID   string // from "id:" line
	Type string // from "event:" line (default: "message")
	Data string // from "data:" line(s), joined with newlines for multi-line
}

// Client is a minimal SSE client using net/http + bufio.Scanner.
type Client struct {
	resp      *http.Response
	scanner   *bufio.Scanner
	closeOnce sync.Once
	logger    zerolog.Logger
}

// ConnectConfig configures an SSE connection.
type ConnectConfig struct {
	GatewayURL string   // base URL, e.g. "http://localhost:3000"
	Channels   []string // channels to subscribe to
	Token      string   // JWT via ?token= query param
	APIKey     string   // X-API-Key header
	Logger     zerolog.Logger
}

// Connect opens an SSE connection to GET {GatewayURL}/sse?channels=...&token=...
// Returns (client, 0, nil) on success, (nil, statusCode, err) on HTTP error.
// The status code allows suites to assert 401/403 without parsing error bodies.
// Callers that also need the error body (to assert the `code` field) use ConnectRaw.
func Connect(ctx context.Context, cfg ConnectConfig) (*Client, int, error) {
	client, statusCode, _, err := ConnectRaw(ctx, cfg)
	return client, statusCode, err
}

// ConnectRaw is Connect but returns the response body on the non-200 path so callers
// can assert the error `code` (e.g. distinguishing a feature-gate EDITION_LIMIT from a
// setup FORBIDDEN). On success the streaming body is owned by *Client, so the returned
// body is nil.
func ConnectRaw(ctx context.Context, cfg ConnectConfig) (client *Client, statusCode int, errorBody []byte, err error) {
	url := cfg.GatewayURL + "/sse?channels=" + strings.Join(cfg.Channels, ",")
	if cfg.Token != "" {
		url += "&token=" + cfg.Token
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("sse connect: create request: %w", err)
	}

	if cfg.APIKey != "" {
		req.Header.Set("X-API-Key", cfg.APIKey)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("sse connect: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // best-effort: error body for code assertion
		_ = resp.Body.Close()
		return nil, resp.StatusCode, body, fmt.Errorf("sse connect: HTTP %d", resp.StatusCode)
	}

	return &Client{
		resp:    resp,
		scanner: bufio.NewScanner(resp.Body),
		logger:  cfg.Logger,
	}, 0, nil, nil
}

// ReadEvent blocks until the next SSE event. Skips keepalive comments.
// Returns io.EOF on stream end.
//
// Timeout mechanism: spawns an internal goroutine (plain go func(), not wg.Go —
// fire-and-forget cancellation helper) with defer logging.RecoverPanic() that
// monitors ctx.Done() and closes resp.Body to unblock the scanner. After ctx
// cancellation, the client is no longer usable (body closed).
func (c *Client) ReadEvent(ctx context.Context) (*Event, error) {
	// Spawn cancellation goroutine that closes the body when ctx expires.
	done := make(chan struct{})
	defer close(done)

	go func() {
		defer logging.RecoverPanic(c.logger, "sse-read-event-cancel", nil)
		select {
		case <-ctx.Done():
			_ = c.Close() // best-effort: unblock scanner on context cancel
		case <-done:
		}
	}()

	var event Event
	hasData := false

	for c.scanner.Scan() {
		line := c.scanner.Text()

		switch {
		case line == "":
			// Empty line = dispatch event
			if hasData {
				if event.Type == "" {
					event.Type = "message"
				}
				return &event, nil
			}

		case strings.HasPrefix(line, ":"):
			// Comment (keepalive) — skip

		case strings.HasPrefix(line, "id:"):
			event.ID = strings.TrimSpace(strings.TrimPrefix(line, "id:"))

		case strings.HasPrefix(line, "event:"):
			event.Type = strings.TrimSpace(strings.TrimPrefix(line, "event:"))

		case strings.HasPrefix(line, "data:"):
			value := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if hasData {
				event.Data += "\n" + value
			} else {
				event.Data = value
				hasData = true
			}
		}
	}

	if err := c.scanner.Err(); err != nil {
		return nil, fmt.Errorf("sse read: %w", err)
	}
	return nil, io.EOF
}

// Close closes the SSE connection. Safe to call multiple times (sync.Once guarded).
func (c *Client) Close() error {
	var err error
	c.closeOnce.Do(func() {
		if closeErr := c.resp.Body.Close(); closeErr != nil {
			err = fmt.Errorf("sse close: %w", closeErr)
		}
	})
	return err
}
