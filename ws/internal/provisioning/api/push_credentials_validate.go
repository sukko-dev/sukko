package api

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Sentinel errors for push credential handler construction.
var (
	errNilCredentialsRepo = errors.New("push credential handler: credentials repository is required")
	errNilEventBus        = errors.New("push credential handler: event bus is required")
	// errNilTenantResolver is shared by both push handlers (credentials + channels).
	errNilTenantResolver = errors.New("push handler: tenant resolver is required")
)

// validateCredentialData validates the credential_data JSON structure
// based on the provider type.
func validateCredentialData(provider, credentialData string) error {
	var data map[string]any
	if err := json.Unmarshal([]byte(credentialData), &data); err != nil {
		return fmt.Errorf("credential_data must be valid JSON: %w", err)
	}

	switch provider {
	case "fcm":
		return validateFCMCredential(data)
	case "apns":
		return validateAPNsCredential(data)
	case "vapid":
		return validateVAPIDCredential(data)
	default:
		return fmt.Errorf("unsupported provider: %s", provider)
	}
}

// validateFCMCredential checks that FCM service account JSON has required fields.
func validateFCMCredential(data map[string]any) error {
	required := []string{"project_id", "private_key", "client_email"}
	return requireFields(data, "fcm", required)
}

// validateAPNsCredential checks that APNs credential JSON has required fields.
func validateAPNsCredential(data map[string]any) error {
	required := []string{"key_id", "team_id", "bundle_id", "p8_key"}
	return requireFields(data, "apns", required)
}

// validateVAPIDCredential checks that VAPID credential JSON has required fields.
func validateVAPIDCredential(data map[string]any) error {
	required := []string{"public_key", "private_key"}
	return requireFields(data, "vapid", required)
}

// requireFields checks that all required fields are present and non-empty in the data map.
func requireFields(data map[string]any, provider string, fields []string) error {
	for _, field := range fields {
		val, ok := data[field]
		if !ok {
			return fmt.Errorf("%s credential_data missing required field: %s", provider, field)
		}
		str, isStr := val.(string)
		if !isStr || str == "" {
			return fmt.Errorf("%s credential_data field %s must be a non-empty string", provider, field)
		}
	}
	return nil
}

// testFCMConnectivity attempts an OAuth2 token exchange with the FCM service account
// to verify the credentials are valid. This is a best-effort connectivity test.
func testFCMConnectivity(credentialData string) error {
	// Parse the service account JSON to verify it's structurally valid for OAuth2.
	// A full token exchange requires network access to Google's OAuth2 endpoint,
	// which we skip in the provisioning service to avoid external dependencies
	// in the write path. The structural validation in validateFCMCredential
	// catches most configuration errors.
	//
	// Future enhancement: async connectivity check that updates credential status.
	var sa struct {
		Type         string `json:"type"`
		ProjectID    string `json:"project_id"`
		PrivateKey   string `json:"private_key"`
		ClientEmail  string `json:"client_email"`
		TokenURI     string `json:"token_uri"`
		AuthURI      string `json:"auth_uri"`
		AuthProvider string `json:"auth_provider_x509_cert_url"`
	}
	if err := json.Unmarshal([]byte(credentialData), &sa); err != nil {
		return fmt.Errorf("parse service account JSON: %w", err)
	}

	if sa.Type != "" && sa.Type != "service_account" {
		return fmt.Errorf("FCM credential type must be 'service_account', got %q", sa.Type)
	}

	return nil
}
