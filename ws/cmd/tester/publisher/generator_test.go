package publisher

import (
	"encoding/json"
	"testing"
)

func TestNewGenerator(t *testing.T) {
	t.Parallel()

	g := NewGenerator()
	if g == nil {
		t.Fatal("expected non-nil generator")
	}
}

func TestGenerator_Next(t *testing.T) {
	t.Parallel()

	g := NewGenerator()

	data, err := g.Next("test.channel")
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if data == nil {
		t.Fatal("expected non-nil data")
	}

	var msg TestMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if msg.Sequence != 1 {
		t.Errorf("Sequence = %d, want 1", msg.Sequence)
	}
	if msg.Timestamp == 0 {
		t.Error("expected non-zero timestamp")
	}
	if msg.Payload != "test-message-1" {
		t.Errorf("Payload = %q, want %q", msg.Payload, "test-message-1")
	}
}

func TestGenerator_SequenceIncrement(t *testing.T) {
	t.Parallel()

	g := NewGenerator()

	for i := range 5 {
		data, err := g.Next("ch")
		if err != nil {
			t.Fatalf("Next[%d]: %v", i, err)
		}
		var msg TestMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			t.Fatalf("unmarshal[%d]: %v", i, err)
		}
		want := int64(i + 1)
		if msg.Sequence != want {
			t.Errorf("iteration %d: Sequence = %d, want %d", i, msg.Sequence, want)
		}
	}
}

func TestGenerator_Reset(t *testing.T) {
	t.Parallel()

	g := NewGenerator()

	// Generate a few messages
	for range 3 {
		if _, err := g.Next("ch"); err != nil {
			t.Fatalf("Next: %v", err)
		}
	}

	g.Reset()

	data, err := g.Next("ch")
	if err != nil {
		t.Fatalf("Next after reset: %v", err)
	}
	var msg TestMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if msg.Sequence != 1 {
		t.Errorf("Sequence after reset = %d, want 1", msg.Sequence)
	}
}

func TestGenerator_ConcurrentNext(t *testing.T) {
	t.Parallel()

	g := NewGenerator()
	const goroutines = 10
	const perGoroutine = 100

	done := make(chan struct{})
	for range goroutines {
		go func() {
			for range perGoroutine {
				if _, err := g.Next("ch"); err != nil {
					t.Errorf("Next: %v", err)
				}
			}
			done <- struct{}{}
		}()
	}

	for range goroutines {
		<-done
	}

	// Next sequence should be goroutines * perGoroutine + 1
	data, _ := g.Next("ch")
	var msg TestMessage
	_ = json.Unmarshal(data, &msg)
	want := int64(goroutines*perGoroutine + 1)
	if msg.Sequence != want {
		t.Errorf("final Sequence = %d, want %d", msg.Sequence, want)
	}
}
