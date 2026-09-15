package provider

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
)

func TestDryRunProvider_Name(t *testing.T) {
	t.Parallel()

	p := NewDryRunProvider(zerolog.Nop())
	if got := p.Name(); got != "dryrun" {
		t.Fatalf("Name() = %q, want %q", got, "dryrun")
	}
}

func TestDryRunProvider_Send(t *testing.T) {
	t.Parallel()

	p := NewDryRunProvider(zerolog.Nop())

	job := PushJob{
		TenantID:  "tenant-1",
		Principal: "user-1",
		Platform:  "web",
		Title:     "Test",
		Body:      "Hello",
		Endpoint:  "https://example.com/push/123",
		Channel:   "prices",
	}

	if err := p.Send(context.Background(), job); err != nil {
		t.Fatalf("Send() returned unexpected error: %v", err)
	}
}

func TestDryRunProvider_SendBatch(t *testing.T) {
	t.Parallel()

	p := NewDryRunProvider(zerolog.Nop())

	jobs := []PushJob{
		{TenantID: "t1", Principal: "u1", Platform: "web", Title: "A", Body: "1", Endpoint: "https://example.com/1"},
		{TenantID: "t1", Principal: "u2", Platform: "android", Title: "B", Body: "2", Token: "fcm-token-abc"},
		{TenantID: "t2", Principal: "u3", Platform: "ios", Title: "C", Body: "3", Token: "apns-token-xyz"},
	}

	if err := p.SendBatch(context.Background(), jobs); err != nil {
		t.Fatalf("SendBatch() returned unexpected error: %v", err)
	}
}

func TestDryRunProvider_SendBatch_Empty(t *testing.T) {
	t.Parallel()

	p := NewDryRunProvider(zerolog.Nop())

	if err := p.SendBatch(context.Background(), nil); err != nil {
		t.Fatalf("SendBatch(nil) returned unexpected error: %v", err)
	}
	if err := p.SendBatch(context.Background(), []PushJob{}); err != nil {
		t.Fatalf("SendBatch([]) returned unexpected error: %v", err)
	}
}

func TestDryRunProvider_Close(t *testing.T) {
	t.Parallel()

	p := NewDryRunProvider(zerolog.Nop())

	if err := p.Close(); err != nil {
		t.Fatalf("Close() returned unexpected error: %v", err)
	}
}
