package gateway

import (
	"encoding/json"
	"testing"
)

func TestAuthData_UnmarshalJSON(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		input     string
		wantToken string
		wantErr   bool
	}{
		{
			name:      "valid_token",
			input:     `{"token":"eyJhbGciOiJFUzI1NiJ9.test.sig"}`,
			wantToken: "eyJhbGciOiJFUzI1NiJ9.test.sig",
		},
		{
			name:      "empty_token",
			input:     `{"token":""}`,
			wantToken: "",
		},
		{
			name:      "missing_token_field",
			input:     `{}`,
			wantToken: "",
		},
		{
			name:    "invalid_json",
			input:   `{invalid`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var data AuthData
			err := json.Unmarshal([]byte(tt.input), &data)
			if tt.wantErr {
				if err == nil {
					t.Error("Expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("Unmarshal failed: %v", err)
			}
			if data.Token != tt.wantToken {
				t.Errorf("Token = %q, want %q", data.Token, tt.wantToken)
			}
		})
	}
}

func TestAuthAckResponse_MarshalJSON(t *testing.T) {
	t.Parallel()
	resp := AuthAckResponse{
		Type: RespTypeAuthAck,
		Data: AuthAckData{Exp: 1704067200},
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Parse check failed: %v", err)
	}

	if parsed["type"] != RespTypeAuthAck {
		t.Errorf("type = %v, want %q", parsed["type"], RespTypeAuthAck)
	}

	dataField, ok := parsed["data"].(map[string]any)
	if !ok {
		t.Fatal("data field missing or wrong type")
	}
	if exp, ok := dataField["exp"].(float64); !ok || int64(exp) != 1704067200 {
		t.Errorf("exp = %v, want 1704067200", dataField["exp"])
	}
}

func TestAuthErrorResponse_MarshalJSON(t *testing.T) {
	t.Parallel()
	resp := AuthErrorResponse{
		Type: RespTypeAuthError,
		Data: AuthErrorData{
			Code:    AuthErrTokenExpired,
			Message: AuthErrorMessages[AuthErrTokenExpired],
		},
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Parse check failed: %v", err)
	}

	if parsed["type"] != RespTypeAuthError {
		t.Errorf("type = %v, want %q", parsed["type"], RespTypeAuthError)
	}

	dataField, ok := parsed["data"].(map[string]any)
	if !ok {
		t.Fatal("data field missing or wrong type")
	}
	if dataField["code"] != AuthErrTokenExpired {
		t.Errorf("code = %v, want %q", dataField["code"], AuthErrTokenExpired)
	}
}

func TestAuthErrorCodes_AllValid(t *testing.T) {
	t.Parallel()
	codes := []string{
		AuthErrInvalidToken,
		AuthErrTokenExpired,
		AuthErrTenantMismatch,
		AuthErrRateLimited,
		AuthErrNotAvailable,
	}

	for _, code := range codes {
		if code == "" {
			t.Errorf("Empty error code found")
		}
		msg, exists := AuthErrorMessages[code]
		if !exists {
			t.Errorf("No message for error code %q", code)
		}
		if msg == "" {
			t.Errorf("Empty message for error code %q", code)
		}
	}
}

func TestAuthData_JSONRoundTrip(t *testing.T) {
	t.Parallel()
	original := AuthData{Token: "eyJhbGciOiJFUzI1NiJ9.payload.signature"}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var decoded AuthData
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if decoded.Token != original.Token {
		t.Errorf("Token = %q, want %q", decoded.Token, original.Token)
	}
}
