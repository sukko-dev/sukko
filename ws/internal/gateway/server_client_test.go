package gateway

import (
	"testing"

	"github.com/rs/zerolog"
)

func TestNewServerClient_InvalidAddr(t *testing.T) {
	t.Parallel()

	// grpc.NewClient with an invalid address still succeeds (lazy connection)
	// but we can test Close on a valid client
	client, err := NewServerClient("localhost:0", zerolog.Nop())
	if err != nil {
		t.Fatalf("NewServerClient error: %v", err)
	}
	if client.Client() == nil {
		t.Error("Client() should not be nil")
	}
	if err := client.Close(); err != nil {
		t.Errorf("Close error: %v", err)
	}
}

func TestServerClient_Close_NilConn(t *testing.T) {
	t.Parallel()

	client := &ServerClient{conn: nil}
	if err := client.Close(); err != nil {
		t.Errorf("Close on nil conn should return nil, got: %v", err)
	}
}
