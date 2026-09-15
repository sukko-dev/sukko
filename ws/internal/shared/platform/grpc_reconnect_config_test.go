package platform

import (
	"strings"
	"testing"
	"time"
)

func TestGRPCReconnectConfig_Validate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		delay     time.Duration
		maxDelay  time.Duration
		wantErr   bool
		errSubstr string
	}{
		{
			name:     "valid defaults",
			delay:    1 * time.Second,
			maxDelay: 30 * time.Second,
			wantErr:  false,
		},
		{
			name:     "at minimum boundary",
			delay:    100 * time.Millisecond,
			maxDelay: 100 * time.Millisecond,
			wantErr:  false,
		},
		{
			name:     "at maximum boundary",
			delay:    5 * time.Minute,
			maxDelay: 5 * time.Minute,
			wantErr:  false,
		},
		{
			name:      "delay below minimum",
			delay:     50 * time.Millisecond,
			maxDelay:  30 * time.Second,
			wantErr:   true,
			errSubstr: "PROVISIONING_GRPC_RECONNECT_DELAY must be >= 100ms",
		},
		{
			name:      "delay above maximum (5m ceiling)",
			delay:     6 * time.Minute,
			maxDelay:  6 * time.Minute,
			wantErr:   true,
			errSubstr: "PROVISIONING_GRPC_RECONNECT_DELAY must be <= 5m",
		},
		{
			name:      "maxDelay above ceiling",
			delay:     1 * time.Second,
			maxDelay:  6 * time.Minute,
			wantErr:   true,
			errSubstr: "PROVISIONING_GRPC_RECONNECT_MAX_DELAY must be <= 5m",
		},
		{
			name:      "maxDelay less than delay",
			delay:     5 * time.Second,
			maxDelay:  1 * time.Second,
			wantErr:   true,
			errSubstr: "PROVISIONING_GRPC_RECONNECT_MAX_DELAY",
		},
		{
			name:     "equal delay and maxDelay (valid hysteresis)",
			delay:    5 * time.Second,
			maxDelay: 5 * time.Second,
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := GRPCReconnectConfig{
				GRPCReconnectDelay:    tt.delay,
				GRPCReconnectMaxDelay: tt.maxDelay,
			}
			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && tt.errSubstr != "" && err != nil {
				if !strings.Contains(err.Error(), tt.errSubstr) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errSubstr)
				}
			}
		})
	}
}
