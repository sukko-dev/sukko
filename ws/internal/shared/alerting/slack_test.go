package alerting

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewSlackAlerterWithConfig(t *testing.T) {
	t.Parallel()
	alerter := NewSlackAlerterWithConfig(SlackConfig{
		WebhookURL:  "https://hooks.slack.com/services/test",
		Channel:     "#alerts",
		Username:    "AlertBot",
		ServiceName: "ws-server",
		Environment: "production",
		Timeout:     10 * time.Second,
	})

	if alerter == nil {
		t.Fatal("NewSlackAlerterWithConfig should return non-nil")
	}
	if alerter.serviceName != "ws-server" {
		t.Errorf("serviceName mismatch: %s", alerter.serviceName)
	}
	if alerter.environment != "production" {
		t.Errorf("environment mismatch: %s", alerter.environment)
	}
}

func TestSlackAlerter_SkipsEmptyWebhook(t *testing.T) {
	t.Parallel()
	var requestMade atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requestMade.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Create alerter with empty webhook URL
	alerter := NewSlackAlerterWithConfig(SlackConfig{
		Channel:  "#alerts",
		Username: "AlertBot",
		Timeout:  5 * time.Second,
	})

	alerter.Alert(ERROR, "Test", nil)

	// Give time for any potential HTTP request
	time.Sleep(50 * time.Millisecond)

	// Verify NO request was made (empty webhook should skip)
	if requestMade.Load() != 0 {
		t.Error("SlackAlerter with empty webhook should not make HTTP requests")
	}
}

func TestSlackAlerter_SendsToWebhook(t *testing.T) {
	t.Parallel()
	var receivedPayload map[string]any
	var receivedContentType string
	var requestCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		receivedContentType = r.Header.Get("Content-Type")

		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &receivedPayload)

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	alerter := NewSlackAlerterWithConfig(SlackConfig{
		WebhookURL: server.URL,
		Channel:    "#test-channel",
		Username:   "TestBot",
		Timeout:    5 * time.Second,
	})

	alerter.Alert(ERROR, "Test alert message", map[string]any{
		"server":      "ws-1",
		"connections": 5000,
	})

	// Give time for HTTP request
	time.Sleep(50 * time.Millisecond)

	if requestCount.Load() != 1 {
		t.Errorf("Expected 1 request, got %d", requestCount.Load())
	}

	if receivedContentType != "application/json" {
		t.Errorf("Content-Type: got %s, want application/json", receivedContentType)
	}

	// Verify payload structure
	if receivedPayload["username"] != "TestBot" {
		t.Errorf("username: got %v, want TestBot", receivedPayload["username"])
	}
	if receivedPayload["channel"] != "#test-channel" {
		t.Errorf("channel: got %v, want #test-channel", receivedPayload["channel"])
	}
	if !strings.Contains(receivedPayload["text"].(string), "ERROR Alert") {
		t.Errorf("text should contain ERROR Alert: %v", receivedPayload["text"])
	}
}

func TestSlackAlerter_IncludesServiceContext(t *testing.T) {
	t.Parallel()
	var receivedPayload map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &receivedPayload)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	alerter := NewSlackAlerterWithConfig(SlackConfig{
		WebhookURL:  server.URL,
		Channel:     "#test",
		Username:    "Bot",
		ServiceName: "ws-gateway",
		Environment: "staging",
		Timeout:     5 * time.Second,
	})

	alerter.Alert(WARNING, "Test message", nil)

	time.Sleep(50 * time.Millisecond)

	// Check attachments contain service context fields
	attachments, ok := receivedPayload["attachments"].([]any)
	if !ok || len(attachments) == 0 {
		t.Fatal("Should have attachments")
	}

	attachment := attachments[0].(map[string]any)
	fields, ok := attachment["fields"].([]any)
	if !ok {
		t.Fatal("Attachment should have fields")
	}

	// Find service and environment fields
	var hasService, hasEnv bool
	for _, f := range fields {
		field := f.(map[string]any)
		if field["title"] == "Service" && field["value"] == "ws-gateway" {
			hasService = true
		}
		if field["title"] == "Environment" && field["value"] == "staging" {
			hasEnv = true
		}
	}

	if !hasService {
		t.Error("Should include Service field")
	}
	if !hasEnv {
		t.Error("Should include Environment field")
	}
}

func TestSlackAlerter_getColor(t *testing.T) {
	t.Parallel()
	alerter := &SlackAlerter{}

	tests := []struct {
		level    Level
		expected string
	}{
		{CRITICAL, "danger"},
		{ERROR, "danger"},
		{WARNING, "warning"},
		{INFO, "good"},
		{DEBUG, "good"},
	}

	for _, tt := range tests {
		t.Run(string(tt.level), func(t *testing.T) {
			t.Parallel()
			color := alerter.getColor(tt.level)
			if color != tt.expected {
				t.Errorf("getColor(%s): got %s, want %s", tt.level, color, tt.expected)
			}
		})
	}
}

func TestSlackAlerter_getEmoji(t *testing.T) {
	t.Parallel()
	alerter := &SlackAlerter{}

	tests := []struct {
		level    Level
		expected string
	}{
		{CRITICAL, ":rotating_light:"},
		{ERROR, ":x:"},
		{WARNING, ":warning:"},
		{INFO, ":information_source:"},
		{DEBUG, ":white_check_mark:"},
	}

	for _, tt := range tests {
		t.Run(string(tt.level), func(t *testing.T) {
			t.Parallel()
			emoji := alerter.getEmoji(tt.level)
			if emoji != tt.expected {
				t.Errorf("getEmoji(%s): got %s, want %s", tt.level, emoji, tt.expected)
			}
		})
	}
}

func TestSlackAlerter_ImplementsInterface(t *testing.T) {
	t.Parallel()
	var alerter Alerter = NewSlackAlerterWithConfig(SlackConfig{
		Timeout: 5 * time.Second,
	})

	// Should not panic even with empty config
	alerter.Alert(ERROR, "Test", nil)
}
