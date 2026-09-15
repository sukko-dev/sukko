package adminui

import "time"

// NewTestTokenAuthProvider creates a TokenAuthProvider for testing.
func NewTestTokenAuthProvider(token string, ttl time.Duration) *TokenAuthProvider {
	return NewTokenAuthProvider(token, ttl)
}

// NewTestSessionStore creates a sessionStore for testing.
func NewTestSessionStore() *sessionStore {
	return &sessionStore{sessions: make(map[string]time.Time)}
}

// CreateSession creates a session in the store for testing.
func (s *sessionStore) CreateSession(ttl time.Duration) string {
	return s.create(ttl)
}

// SessionCount returns the number of sessions in the store for testing.
func (s *sessionStore) SessionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sessions)
}

// PruneExported wraps Prune for tests.
func (s *sessionStore) PruneExported() error {
	return s.Prune()
}
