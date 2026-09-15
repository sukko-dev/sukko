package provider

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/rs/zerolog"
)

// TODO: integration tests with mock FCM server

func TestFCMProvider_Name(t *testing.T) {
	t.Parallel()

	p, err := NewFCMProvider(zerolog.Nop(), func(string) (json.RawMessage, error) {
		return nil, nil
	})
	if err != nil {
		t.Fatalf("NewFCMProvider: %v", err)
	}
	if got := p.Name(); got != "fcm" {
		t.Fatalf("Name() = %q, want %q", got, "fcm")
	}
}

func TestNewFCMProvider_NilLookup(t *testing.T) {
	t.Parallel()

	_, err := NewFCMProvider(zerolog.Nop(), nil)
	if err == nil {
		t.Fatal("expected error for nil credential lookup, got nil")
	}
}

func TestFCMProvider_SendBatch_Empty(t *testing.T) {
	t.Parallel()

	p, err := NewFCMProvider(zerolog.Nop(), func(string) (json.RawMessage, error) {
		return nil, nil
	})
	if err != nil {
		t.Fatalf("NewFCMProvider: %v", err)
	}

	if err := p.SendBatch(context.Background(), nil); err != nil {
		t.Fatalf("SendBatch(nil) returned unexpected error: %v", err)
	}
	if err := p.SendBatch(context.Background(), []PushJob{}); err != nil {
		t.Fatalf("SendBatch([]) returned unexpected error: %v", err)
	}
}

func TestFCMProvider_Send_InvalidCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		creds string
	}{
		{
			name:  "invalid_json",
			creds: `{not valid json}`,
		},
		{
			name:  "empty_object",
			creds: `{}`,
		},
		{
			name:  "malformed_service_account",
			creds: `{"type": "service_account", "project_id": "test"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p, err := NewFCMProvider(zerolog.Nop(), func(string) (json.RawMessage, error) {
				return json.RawMessage(tt.creds), nil
			})
			if err != nil {
				t.Fatalf("NewFCMProvider: %v", err)
			}

			job := PushJob{
				TenantID:  "tenant-1",
				Principal: "user-1",
				Platform:  "android",
				Token:     "fake-fcm-token",
				Title:     "Test",
				Body:      "Hello",
			}

			err = p.Send(context.Background(), job)
			if err == nil {
				t.Fatal("expected error for invalid credentials, got nil")
			}
		})
	}
}

func TestFCMProvider_Close(t *testing.T) {
	t.Parallel()

	p, err := NewFCMProvider(zerolog.Nop(), func(string) (json.RawMessage, error) {
		return nil, nil
	})
	if err != nil {
		t.Fatalf("NewFCMProvider: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close() returned unexpected error: %v", err)
	}
}
