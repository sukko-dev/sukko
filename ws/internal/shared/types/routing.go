package types

// RoutingRule maps a channel glob pattern to Kafka topic suffixes with two
// explicit, mutually exclusive roles (§XV): IngressTopic is the single topic
// whose records the platform consumes and delivers to subscribers; EgressTopics
// receive additional copies for external consumers and are NEVER consumed.
// Used across provisioning, ws-server, and shared/provapi without importing
// service-specific packages (Constitution X).
type RoutingRule struct {
	Pattern      string
	IngressTopic string
	EgressTopics []string
	Priority     int
}
