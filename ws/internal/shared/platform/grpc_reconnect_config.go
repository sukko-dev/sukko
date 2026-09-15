package platform

import (
	"fmt"
	"time"
)

// GRPCReconnectConfig holds reconnection backoff parameters for gRPC clients
// that connect to the provisioning service. Env var names carry the
// PROVISIONING_ prefix — embed only in configs that connect to provisioning.
type GRPCReconnectConfig struct {
	GRPCReconnectDelay    time.Duration `env:"PROVISIONING_GRPC_RECONNECT_DELAY"     envDefault:"1s"`  // Initial backoff delay between provisioning gRPC stream reconnect attempts.
	GRPCReconnectMaxDelay time.Duration `env:"PROVISIONING_GRPC_RECONNECT_MAX_DELAY" envDefault:"30s"` // Maximum backoff delay cap for provisioning gRPC stream reconnect attempts.
}

const grpcReconnectDelayFloor = 100 * time.Millisecond
const grpcReconnectDelayCeiling = 5 * time.Minute

// Validate checks reconnect backoff bounds.
// Lower bound: delay >= 100ms (prevents tight reconnect loops).
// Upper bound: both <= 5m (prevents configs that never reconnect in practice).
// Hysteresis: maxDelay >= delay (§I: lower < upper).
func (c GRPCReconnectConfig) Validate() error {
	if c.GRPCReconnectDelay < grpcReconnectDelayFloor {
		return fmt.Errorf("PROVISIONING_GRPC_RECONNECT_DELAY must be >= %s, got %s",
			grpcReconnectDelayFloor, c.GRPCReconnectDelay)
	}
	if c.GRPCReconnectDelay > grpcReconnectDelayCeiling {
		return fmt.Errorf("PROVISIONING_GRPC_RECONNECT_DELAY must be <= %s, got %s",
			grpcReconnectDelayCeiling, c.GRPCReconnectDelay)
	}
	if c.GRPCReconnectMaxDelay < c.GRPCReconnectDelay {
		return fmt.Errorf("PROVISIONING_GRPC_RECONNECT_MAX_DELAY (%s) must be >= PROVISIONING_GRPC_RECONNECT_DELAY (%s)",
			c.GRPCReconnectMaxDelay, c.GRPCReconnectDelay)
	}
	if c.GRPCReconnectMaxDelay > grpcReconnectDelayCeiling {
		return fmt.Errorf("PROVISIONING_GRPC_RECONNECT_MAX_DELAY must be <= %s, got %s",
			grpcReconnectDelayCeiling, c.GRPCReconnectMaxDelay)
	}
	return nil
}
