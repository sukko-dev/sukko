package adminui

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestSessionStore_Validate_Valid(t *testing.T) {
	t.Parallel()
	store := NewTestSessionStore()
	id := store.CreateSession(time.Hour)
	if err := store.validate(id); err != nil {
		t.Errorf("expected no error for valid session, got: %v", err)
	}
}

func TestSessionStore_Validate_Expired(t *testing.T) {
	t.Parallel()
	store := NewTestSessionStore()
	id := store.CreateSession(-time.Second) // already expired
	err := store.validate(id)
	if !errors.Is(err, ErrSessionExpired) {
		t.Errorf("expected ErrSessionExpired, got: %v", err)
	}
	// Lazy deletion: session must have been removed.
	if store.SessionCount() != 0 {
		t.Error("expected expired session to be lazily deleted")
	}
}

func TestSessionStore_Validate_NotFound(t *testing.T) {
	t.Parallel()
	store := NewTestSessionStore()
	err := store.validate("nonexistent-id")
	if !errors.Is(err, ErrSessionNotFound) {
		t.Errorf("expected ErrSessionNotFound, got: %v", err)
	}
}

func TestSessionStore_Prune_RemovesExpired(t *testing.T) {
	t.Parallel()
	store := NewTestSessionStore()
	_ = store.CreateSession(-time.Second) // expired
	_ = store.CreateSession(time.Hour)    // valid
	if err := store.PruneExported(); err != nil {
		t.Fatalf("unexpected Prune error: %v", err)
	}
	if store.SessionCount() != 1 {
		t.Errorf("expected 1 session after prune, got %d", store.SessionCount())
	}
}

func TestSessionStore_Prune_KeepsValid(t *testing.T) {
	t.Parallel()
	store := NewTestSessionStore()
	id := store.CreateSession(time.Hour)
	if err := store.PruneExported(); err != nil {
		t.Fatalf("unexpected Prune error: %v", err)
	}
	// Valid session must still validate.
	if err := store.validate(id); err != nil {
		t.Errorf("valid session removed by Prune: %v", err)
	}
}

func TestSessionStore_Prune_TooManySessions(t *testing.T) {
	t.Parallel()
	store := NewTestSessionStore()
	for range maxAdminSessions {
		_ = store.CreateSession(time.Hour)
	}
	err := store.PruneExported()
	if !errors.Is(err, ErrTooManySessions) {
		t.Errorf("expected ErrTooManySessions, got: %v", err)
	}
}

func TestSessionStore_ConcurrentAccess(t *testing.T) {
	t.Parallel()
	store := NewTestSessionStore()
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			id := store.CreateSession(time.Hour)
			_ = store.validate(id)
			store.delete(id)
		})
	}
	wg.Wait()
}

func TestTokenAuthProvider_Login_Success(t *testing.T) {
	t.Parallel()
	token := "a-token-that-is-exactly-32-chars!"
	provider := NewTestTokenAuthProvider(token, time.Hour)
	id, err := provider.Login(context.Background(), token)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if id == "" {
		t.Error("expected non-empty session ID")
	}
}

func TestTokenAuthProvider_Login_InvalidToken(t *testing.T) {
	t.Parallel()
	provider := NewTestTokenAuthProvider("correct-token-32-chars-long-here", time.Hour)
	_, err := provider.Login(context.Background(), "wrong")
	if !errors.Is(err, ErrInvalidCredential) {
		t.Errorf("expected ErrInvalidCredential, got: %v", err)
	}
}

func TestTokenAuthProvider_ValidateSession_Valid(t *testing.T) {
	t.Parallel()
	token := "a-token-that-is-exactly-32-chars!"
	provider := NewTestTokenAuthProvider(token, time.Hour)
	id, err := provider.Login(context.Background(), token)
	if err != nil {
		t.Fatalf("login failed: %v", err)
	}
	if err := provider.ValidateSession(context.Background(), id); err != nil {
		t.Errorf("expected valid session, got: %v", err)
	}
}

func TestTokenAuthProvider_Logout(t *testing.T) {
	t.Parallel()
	token := "a-token-that-is-exactly-32-chars!"
	provider := NewTestTokenAuthProvider(token, time.Hour)
	id, err := provider.Login(context.Background(), token)
	if err != nil {
		t.Fatalf("login failed: %v", err)
	}
	provider.Logout(context.Background(), id)
	err = provider.ValidateSession(context.Background(), id)
	if !errors.Is(err, ErrSessionNotFound) {
		t.Errorf("expected ErrSessionNotFound after logout, got: %v", err)
	}
}
