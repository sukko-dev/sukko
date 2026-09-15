package gateway

import (
	"context"
	"sync"
)

// Connection represents an active authenticated connection that can be
// force-disconnected on token revocation. Implemented by Proxy (WebSocket)
// and sseConnection (SSE).
type Connection interface {
	// ForceClose terminates the connection. For WebSocket: sends close frame
	// with the given code and reason. For SSE: cancels the handler context.
	ForceClose(code int, reason string)

	// ConnectionClaims returns the JWT claims used to authenticate this connection.
	ConnectionClaims() (sub, jti string, iat int64)

	// Transport returns the transport type: "ws" or "sse".
	Transport() string
}

// ConnectionRegistry tracks active connections indexed by jti and sub
// for force-disconnect on token revocation. Uses sync.RWMutex — register/
// unregister are on the connection lifecycle (cold path), find operations
// are on revocation events (also cold path, rare). NOT on the JWT validation
// hot path — revocation checks use atomic.Value in StreamRevocationRegistry.
type ConnectionRegistry struct {
	mu        sync.RWMutex
	bySubject map[string]map[Connection]struct{} // "tenant:sub" → set of connections
	byJTI     map[string]Connection              // jti → connection
}

// NewConnectionRegistry creates an empty connection registry.
func NewConnectionRegistry() *ConnectionRegistry {
	return &ConnectionRegistry{
		bySubject: make(map[string]map[Connection]struct{}),
		byJTI:     make(map[string]Connection),
	}
}

// Register adds a connection to the registry indexed by both jti and sub.
func (r *ConnectionRegistry) Register(conn Connection, tenantID, sub, jti string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if jti != "" {
		r.byJTI[jti] = conn
	}

	if sub != "" {
		key := tenantID + ":" + sub
		if r.bySubject[key] == nil {
			r.bySubject[key] = make(map[Connection]struct{})
		}
		r.bySubject[key][conn] = struct{}{}
	}
}

// Unregister removes a connection from the registry.
func (r *ConnectionRegistry) Unregister(conn Connection, tenantID, sub, jti string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if jti != "" {
		delete(r.byJTI, jti)
	}

	if sub != "" {
		key := tenantID + ":" + sub
		if conns, ok := r.bySubject[key]; ok {
			delete(conns, conn)
			if len(conns) == 0 {
				delete(r.bySubject, key)
			}
		}
	}
}

// FindByJTI returns the connection with the given jti, or nil.
func (r *ConnectionRegistry) FindByJTI(jti string) Connection {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.byJTI[jti]
}

// FindBySub returns all connections for the given tenant+sub.
func (r *ConnectionRegistry) FindBySub(tenantID, sub string) []Connection {
	r.mu.RLock()
	defer r.mu.RUnlock()

	key := tenantID + ":" + sub
	conns, ok := r.bySubject[key]
	if !ok {
		return nil
	}

	result := make([]Connection, 0, len(conns))
	for conn := range conns {
		result = append(result, conn)
	}
	return result
}

// sseConnection wraps an SSE handler context for force-disconnect.
type sseConnection struct {
	cancel context.CancelFunc
	sub    string
	jti    string
	iat    int64
}

// ForceClose cancels the SSE handler context, dropping the stream.
func (c *sseConnection) ForceClose(_ int, _ string) {
	c.cancel()
}

// ConnectionClaims returns the JWT claims from the SSE connection.
func (c *sseConnection) ConnectionClaims() (sub, jti string, iat int64) {
	return c.sub, c.jti, c.iat
}

// Transport returns TransportSSE.
func (c *sseConnection) Transport() string { return TransportSSE }
