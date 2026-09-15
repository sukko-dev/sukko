package types

// RoutingRule maps a channel glob pattern to one or more Kafka topic suffixes.
// Used across provisioning, ws-server, and shared/provapi without importing
// service-specific packages (Constitution X).
type RoutingRule struct {
	Pattern  string
	Topics   []string
	Priority int
}
