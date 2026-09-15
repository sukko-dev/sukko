package auth

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sukko-dev/sukko/internal/provisioning"
	sharedauth "github.com/sukko-dev/sukko/internal/shared/auth"
)

func TestGraceTenantResolver_ResolveTenantUUID(t *testing.T) {
	t.Parallel()

	const uuidA = "11111111-1111-1111-1111-111111111111"

	tests := []struct {
		name     string
		lookup   TenantUUIDLookup
		wantUUID string
		wantErr  error
	}{
		{
			name:     "resolves slug to uuid",
			lookup:   func(context.Context, string, time.Duration) (string, error) { return uuidA, nil },
			wantUUID: uuidA,
		},
		{
			name: "unknown tenant -> not resolvable (not transient)",
			lookup: func(context.Context, string, time.Duration) (string, error) {
				return "", provisioning.ErrTenantNotFound
			},
			wantErr: sharedauth.ErrTenantNotResolvable,
		},
		{
			name:    "transient error is surfaced verbatim (fail closed, distinguishable)",
			lookup:  func(context.Context, string, time.Duration) (string, error) { return "", errTransient },
			wantErr: errTransient,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := NewGraceTenantResolver(tc.lookup, time.Hour)
			got, err := r.ResolveTenantUUID(context.Background(), "tenant-a")
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil || got != tc.wantUUID {
				t.Fatalf("got (%q, %v), want (%q, nil)", got, err, tc.wantUUID)
			}
		})
	}
}

var errTransient = errors.New("db unavailable")
