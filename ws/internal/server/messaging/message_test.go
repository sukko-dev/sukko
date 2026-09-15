package messaging

import (
	"encoding/json"
	"sync"
	"testing"
)

// =============================================================================
// SequenceGenerator Tests
// =============================================================================

func TestSequenceGenerator_Next(t *testing.T) {
	t.Parallel()
	sg := NewSequenceGenerator()

	// First sequence should be 1
	seq := sg.Next()
	if seq != 1 {
		t.Errorf("First sequence should be 1, got %d", seq)
	}

	// Second sequence should be 2
	seq = sg.Next()
	if seq != 2 {
		t.Errorf("Second sequence should be 2, got %d", seq)
	}

	// Third sequence should be 3
	seq = sg.Next()
	if seq != 3 {
		t.Errorf("Third sequence should be 3, got %d", seq)
	}
}

func TestSequenceGenerator_Monotonic(t *testing.T) {
	t.Parallel()
	sg := NewSequenceGenerator()
	const count = 1000

	var prev int64
	for i := range count {
		seq := sg.Next()
		if seq <= prev {
			t.Errorf("Sequence %d not greater than previous %d at iteration %d", seq, prev, i)
		}
		prev = seq
	}
}

func TestSequenceGenerator_Current(t *testing.T) {
	t.Parallel()
	sg := NewSequenceGenerator()

	// Initial current should be 0
	if sg.Current() != 0 {
		t.Errorf("Initial current should be 0, got %d", sg.Current())
	}

	// After Next(), current should match
	sg.Next()
	if sg.Current() != 1 {
		t.Errorf("Current after one Next() should be 1, got %d", sg.Current())
	}

	// Current doesn't increment
	curr1 := sg.Current()
	curr2 := sg.Current()
	if curr1 != curr2 {
		t.Error("Current should not change between calls")
	}
}

func TestSequenceGenerator_Reset(t *testing.T) {
	t.Parallel()
	sg := NewSequenceGenerator()

	// Generate some sequences
	sg.Next()
	sg.Next()
	sg.Next()

	if sg.Current() != 3 {
		t.Errorf("Current should be 3 before reset, got %d", sg.Current())
	}

	// Reset
	sg.Reset()

	if sg.Current() != 0 {
		t.Errorf("Current should be 0 after reset, got %d", sg.Current())
	}

	// Next after reset should be 1
	seq := sg.Next()
	if seq != 1 {
		t.Errorf("First sequence after reset should be 1, got %d", seq)
	}
}

func TestSequenceGenerator_Concurrent(t *testing.T) {
	t.Parallel()
	sg := NewSequenceGenerator()
	const numGoroutines = 100
	const numOps = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	results := make(chan int64, numGoroutines*numOps)

	for range numGoroutines {
		go func() {
			defer wg.Done()
			for range numOps {
				results <- sg.Next()
			}
		}()
	}

	wg.Wait()
	close(results)

	// Verify all sequences are unique
	seen := make(map[int64]bool)
	for seq := range results {
		if seen[seq] {
			t.Errorf("Duplicate sequence number: %d", seq)
		}
		seen[seq] = true
	}

	// Verify correct total count
	expected := numGoroutines * numOps
	if len(seen) != expected {
		t.Errorf("Expected %d unique sequences, got %d", expected, len(seen))
	}

	// Verify final counter matches
	if sg.Current() != int64(expected) {
		t.Errorf("Final counter should be %d, got %d", expected, sg.Current())
	}
}

// =============================================================================
// MessageEnvelope Tests
// =============================================================================

func TestMessageEnvelope_Serialize(t *testing.T) {
	t.Parallel()
	data := json.RawMessage(`{"tokenId":"BTC","price":"100.50"}`)
	envelope := &MessageEnvelope{
		Seq:       1,
		Timestamp: 1234567890,
		Channel:   "BTC.trade",
		Priority:  PriorityHigh,
		Data:      data,
	}

	result, err := envelope.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Deserialize and verify
	var decoded map[string]any
	if err := json.Unmarshal(result, &decoded); err != nil {
		t.Fatalf("Failed to decode serialized envelope: %v", err)
	}

	if decoded["seq"].(float64) != 1 {
		t.Errorf("seq: got %v, want 1", decoded["seq"])
	}
	if decoded["ts"].(float64) != 1234567890 {
		t.Errorf("ts: got %v, want 1234567890", decoded["ts"])
	}
	if decoded["channel"].(string) != "BTC.trade" {
		t.Errorf("channel: got %v, want BTC.trade", decoded["channel"])
	}
	if decoded["data"] == nil {
		t.Error("data should not be nil")
	}
}

func TestMessageEnvelope_Priority(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		priority MessagePriority
	}{
		{"CRITICAL", PriorityCritical},
		{"HIGH", PriorityHigh},
		{"NORMAL", PriorityNormal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			envelope := &MessageEnvelope{
				Seq:      1,
				Channel:  "test.channel",
				Priority: tt.priority,
				Data:     json.RawMessage(`{}`),
			}

			result, err := envelope.Serialize()
			if err != nil {
				t.Fatalf("Serialize failed: %v", err)
			}

			var decoded map[string]any
			if err := json.Unmarshal(result, &decoded); err != nil {
				t.Fatalf("Failed to decode: %v", err)
			}

			// Priority should never be in JSON (hidden with json:"-")
			if decoded["priority"] != nil {
				t.Errorf("priority should not be present in JSON, got: %v", decoded["priority"])
			}
		})
	}
}

func TestMessageEnvelope_DataPreserved(t *testing.T) {
	t.Parallel()
	originalData := `{"nested":{"key":"value"},"array":[1,2,3]}`
	envelope := &MessageEnvelope{
		Seq:     1,
		Channel: "test.channel",
		Data:    json.RawMessage(originalData),
	}

	result, err := envelope.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Verify the data is preserved exactly (no re-encoding artifacts)
	var decoded MessageEnvelope
	if err := json.Unmarshal(result, &decoded); err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	var decodedData map[string]any
	if err := json.Unmarshal(decoded.Data, &decodedData); err != nil {
		t.Fatalf("Failed to decode data: %v", err)
	}

	// Verify nested structure preserved
	nested, ok := decodedData["nested"].(map[string]any)
	if !ok {
		t.Fatal("nested should be a map")
	}
	if nested["key"].(string) != "value" {
		t.Error("nested key should be 'value'")
	}

	// Verify array preserved
	arr, ok := decodedData["array"].([]any)
	if !ok {
		t.Fatal("array should be an array")
	}
	if len(arr) != 3 {
		t.Error("array should have 3 elements")
	}
}

func TestWrapMessage(t *testing.T) {
	t.Parallel()
	sg := NewSequenceGenerator()
	data := []byte(`{"tokenId":"BTC","price":"100.50"}`)

	envelope, err := WrapMessage(data, "BTC.trade", PriorityHigh, sg)
	if err != nil {
		t.Fatalf("WrapMessage failed: %v", err)
	}

	if envelope.Seq != 1 {
		t.Errorf("Seq should be 1, got %d", envelope.Seq)
	}
	if envelope.Channel != "BTC.trade" {
		t.Errorf("Channel: got %s, want BTC.trade", envelope.Channel)
	}
	if envelope.Priority != PriorityHigh {
		t.Errorf("Priority: got %d, want %d", envelope.Priority, PriorityHigh)
	}
	if envelope.Timestamp == 0 {
		t.Error("Timestamp should be set")
	}
	if string(envelope.Data) != string(data) {
		t.Error("Data should match input")
	}
}

func TestWrapMessage_SequenceIncrement(t *testing.T) {
	t.Parallel()
	sg := NewSequenceGenerator()

	env1, _ := WrapMessage([]byte(`{}`), "test.channel", PriorityNormal, sg)
	env2, _ := WrapMessage([]byte(`{}`), "test.channel", PriorityNormal, sg)
	env3, _ := WrapMessage([]byte(`{}`), "test.channel", PriorityNormal, sg)

	if env1.Seq != 1 || env2.Seq != 2 || env3.Seq != 3 {
		t.Errorf("Sequences should be 1, 2, 3; got %d, %d, %d",
			env1.Seq, env2.Seq, env3.Seq)
	}
}

func TestMessagePriority_Values(t *testing.T) {
	t.Parallel()
	// Verify priority constants have expected values
	if PriorityCritical != 0 {
		t.Errorf("PriorityCritical should be 0, got %d", PriorityCritical)
	}
	if PriorityHigh != 1 {
		t.Errorf("PriorityHigh should be 1, got %d", PriorityHigh)
	}
	if PriorityNormal != 2 {
		t.Errorf("PriorityNormal should be 2, got %d", PriorityNormal)
	}
}
