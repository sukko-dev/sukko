// Package revocation provides an in-memory store for token revocation entries.
// Entries are auto-pruned after their expiration time.
package revocation

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/provapi"
)

// pruneInterval is the interval between automatic prune cycles.
const pruneInterval = 60 * time.Second

// Entry represents a single token revocation.
type Entry struct {
	TenantID  string
	Type      string // provapi.RevocationTypeUser or provapi.RevocationTypeToken
	Sub       string // non-empty for user revocation
	JTI       string // non-empty for token revocation
	RevokedAt int64  // Unix timestamp
	ExpiresAt int64  // Auto-prune time
}

// key returns the map key for this entry.
func (e *Entry) key() string {
	if e.Type == provapi.RevocationTypeToken {
		return "jti:" + e.JTI
	}
	return "sub:" + e.TenantID + ":" + e.Sub
}

// Store is a thread-safe in-memory revocation store with auto-pruning.
type Store struct {
	mu      sync.RWMutex
	entries map[string]*Entry
	logger  zerolog.Logger

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New creates a revocation store and starts the background prune goroutine.
func New(logger zerolog.Logger) *Store {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Store{
		entries: make(map[string]*Entry),
		logger:  logger.With().Str("component", "revocation_store").Logger(),
		cancel:  cancel,
	}

	s.wg.Go(func() {
		defer logging.RecoverPanic(s.logger, "revocation_store_prune", nil)
		s.pruneLoop(ctx)
	})

	return s
}

// Close stops the prune goroutine and waits for it to exit.
func (s *Store) Close() {
	s.cancel()
	s.wg.Wait()
}

// Revoke adds or updates a revocation entry. Idempotent — duplicate
// revocations for the same key update the entry.
func (s *Store) Revoke(entry Entry) error {
	if entry.TenantID == "" {
		return errors.New("revocation: tenant_id is required")
	}
	if entry.Type != provapi.RevocationTypeUser && entry.Type != provapi.RevocationTypeToken {
		return fmt.Errorf("revocation: type must be 'user' or 'token', got %q", entry.Type)
	}
	if entry.Type == provapi.RevocationTypeUser && entry.Sub == "" {
		return errors.New("revocation: sub is required for user revocation")
	}
	if entry.Type == provapi.RevocationTypeToken && entry.JTI == "" {
		return errors.New("revocation: jti is required for token revocation")
	}

	s.mu.Lock()
	s.entries[entry.key()] = &entry
	s.mu.Unlock()

	s.logger.Info().
		Str("type", entry.Type).
		Str(logging.LogKeyTenantSlug, entry.TenantID).
		Str("sub", entry.Sub).
		Str("jti", entry.JTI).
		Int64("expires_at", entry.ExpiresAt).
		Msg("token revocation stored")

	return nil
}

// Snapshot returns all non-expired entries. Used for gRPC stream snapshots.
func (s *Store) Snapshot() []*Entry {
	now := time.Now().Unix()
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*Entry, 0, len(s.entries))
	for _, e := range s.entries {
		if e.ExpiresAt > now {
			result = append(result, e)
		}
	}
	return result
}

// Prune removes all expired entries. Called periodically by the prune goroutine
// and can be called manually for testing.
func (s *Store) Prune() int {
	now := time.Now().Unix()
	s.mu.Lock()
	defer s.mu.Unlock()

	pruned := 0
	for key, e := range s.entries {
		if e.ExpiresAt <= now {
			delete(s.entries, key)
			pruned++
		}
	}

	if pruned > 0 {
		s.logger.Debug().Int("pruned", pruned).Int("remaining", len(s.entries)).
			Msg("revocation entries pruned")
	}
	return pruned
}

// Len returns the current number of entries (including expired but not yet pruned).
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}

func (s *Store) pruneLoop(ctx context.Context) {
	ticker := time.NewTicker(pruneInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.Prune()
		}
	}
}
