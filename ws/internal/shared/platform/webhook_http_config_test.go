package platform

import "testing"

func TestWebhookHTTPConfig_Validate(t *testing.T) {
	t.Parallel()
	// Validate() is always nil — the bool field has no invalid state.
	// The cross-field check (AllowHTTP && Environment != "local") lives in each
	// embedding config's top-level Validate().
	cfg := WebhookHTTPConfig{WebhookAllowHTTP: true}
	if err := cfg.Validate(); err != nil {
		t.Errorf("Validate() should always return nil, got %v", err)
	}
	cfg.WebhookAllowHTTP = false
	if err := cfg.Validate(); err != nil {
		t.Errorf("Validate() should always return nil, got %v", err)
	}
}
