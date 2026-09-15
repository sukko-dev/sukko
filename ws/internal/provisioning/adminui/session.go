package adminui

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// Sentinel errors for session operations.
var (
	ErrInvalidCredential = errors.New("adminui: invalid credential")
	ErrSessionNotFound   = errors.New("adminui: session not found")
	ErrSessionExpired    = errors.New("adminui: session expired")
	ErrTooManySessions   = errors.New("adminui: too many concurrent sessions")
)

// AdminAuthProvider authenticates operators and manages session lifetimes.
type AdminAuthProvider interface {
	Login(ctx context.Context, credential string) (sessionID string, err error)
	ValidateSession(ctx context.Context, sessionID string) error
	Logout(ctx context.Context, sessionID string)
}

// sessionStore is an in-memory map of session IDs to expiry times.
// All operations use a sync.Mutex — every operation modifies state, so RWMutex
// provides no benefit here (§VII: "RWMutex MUST be used for read-heavy data").
type sessionStore struct {
	mu       sync.Mutex
	sessions map[string]time.Time
}

// create generates a cryptographically random 32-byte session ID, stores it with
// its expiry time, and returns the base64url-encoded ID.
func (s *sessionStore) create(ttl time.Duration) string {
	id := make([]byte, 32)
	// crypto/rand.Reader failure is unrecoverable — system entropy is unavailable.
	if _, err := io.ReadFull(rand.Reader, id); err != nil {
		panic(fmt.Sprintf("adminui: crypto/rand.Reader failed: %v", err))
	}
	key := base64.RawURLEncoding.EncodeToString(id)
	s.mu.Lock()
	s.sessions[key] = time.Now().Add(ttl)
	s.mu.Unlock()
	return key
}

// validate checks that sessionID is known and unexpired.
// Expired entries are deleted atomically under the write lock (lazy expiry, no goroutine).
func (s *sessionStore) validate(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	exp, ok := s.sessions[id]
	if !ok {
		return ErrSessionNotFound
	}
	if time.Now().After(exp) {
		delete(s.sessions, id)
		return ErrSessionExpired
	}
	return nil
}

// delete removes a session. Used by Logout.
func (s *sessionStore) delete(id string) {
	s.mu.Lock()
	delete(s.sessions, id)
	s.mu.Unlock()
}

// Prune removes all expired sessions from the store.
// Called at login to bound memory growth — returns ErrTooManySessions when
// len(sessions) > maxAdminSessions after pruning.
// Exported so tests can exercise the pruning path directly.
func (s *sessionStore) Prune() error {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, exp := range s.sessions {
		if now.After(exp) {
			delete(s.sessions, id)
		}
	}
	if len(s.sessions) >= maxAdminSessions {
		return ErrTooManySessions
	}
	return nil
}

// TokenAuthProvider implements AdminAuthProvider using a static ADMIN_TOKEN.
// It is the only auth implementation in v1; OIDC SSO is reserved for a future release.
type TokenAuthProvider struct {
	token string
	store *sessionStore
	ttl   time.Duration
}

// NewTokenAuthProvider creates a TokenAuthProvider.
func NewTokenAuthProvider(token string, ttl time.Duration) *TokenAuthProvider {
	return &TokenAuthProvider{
		token: token,
		store: &sessionStore{sessions: make(map[string]time.Time)},
		ttl:   ttl,
	}
}

// Login validates the submitted credential with constant-time comparison (§IX),
// prunes expired sessions, and creates a new session on success.
func (p *TokenAuthProvider) Login(_ context.Context, credential string) (string, error) {
	// Constant-time compare prevents timing oracle attacks (§IX).
	if subtle.ConstantTimeCompare([]byte(credential), []byte(p.token)) != 1 {
		return "", ErrInvalidCredential
	}
	if err := p.store.Prune(); err != nil {
		return "", err
	}
	return p.store.create(p.ttl), nil
}

// ValidateSession checks that the session ID is known and unexpired.
func (p *TokenAuthProvider) ValidateSession(_ context.Context, sessionID string) error {
	return p.store.validate(sessionID)
}

// Logout deletes the session.
func (p *TokenAuthProvider) Logout(_ context.Context, sessionID string) {
	p.store.delete(sessionID)
}
