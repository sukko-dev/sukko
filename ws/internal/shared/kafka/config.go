package kafka

// =============================================================================
// Topic Namespace Configuration
// =============================================================================
//
// Kafka topic names use the format {namespace}.{tenant}.{suffix} (BuildTopicName, topic.go).
// The {namespace} segment comes from a single explicit config value, KAFKA_TOPIC_NAMESPACE:
//   - It is the sole source of truth — there is NO inference from ENVIRONMENT.
//   - It is required wherever a service builds or parses namespaced topics, validated against
//     VALID_NAMESPACES at startup (platform.KafkaNamespaceConfig), and normalized once at load.
//
// ENVIRONMENT is a separate concern: it identifies the deployment (logging, metrics labels,
// consumer-group naming, history stream/lock keys) and has NO role in topic naming.
//
// There is no environment-name guard anywhere: the VALID_NAMESPACES allowlist is the only
// value-based enforcement boundary. Valid namespaces are configured via VALID_NAMESPACES
// (default: local, dev, stag, prod).
//
// =============================================================================

// EventType represents different event types
type EventType string

// EventType constants for trading platform events.
const (
	EventTradeExecuted EventType = "TRADE_EXECUTED" // Trade executed event
	EventBuyCompleted  EventType = "BUY_COMPLETED"  // Buy completed event
	EventSellCompleted EventType = "SELL_COMPLETED" // Sell completed event

	EventLiquidityAdded      EventType = "LIQUIDITY_ADDED"      // Liquidity added event
	EventLiquidityRemoved    EventType = "LIQUIDITY_REMOVED"    // Liquidity removed event
	EventLiquidityRebalanced EventType = "LIQUIDITY_REBALANCED" // Liquidity rebalanced event

	EventMetadataUpdated   EventType = "METADATA_UPDATED"    // Token metadata updated event
	EventTokenNameChanged  EventType = "TOKEN_NAME_CHANGED"  // Token name changed event
	EventTokenFlagsChanged EventType = "TOKEN_FLAGS_CHANGED" // Token flags changed event

	EventTwitterVerified    EventType = "TWITTER_VERIFIED"     // Twitter verification event
	EventSocialLinksUpdated EventType = "SOCIAL_LINKS_UPDATED" // Social links updated event

	EventCommentPosted   EventType = "COMMENT_POSTED"   // Comment posted event
	EventCommentPinned   EventType = "COMMENT_PINNED"   // Comment pinned event
	EventCommentUpvoted  EventType = "COMMENT_UPVOTED"  // Comment upvoted event
	EventFavoriteToggled EventType = "FAVORITE_TOGGLED" // Favorite toggled event

	EventTokenCreated EventType = "TOKEN_CREATED" // Token created event
	EventTokenListed  EventType = "TOKEN_LISTED"  // Token listed event

	EventPriceDeltaUpdated     EventType = "PRICE_DELTA_UPDATED"    // Price delta updated event
	EventHolderCountUpdated    EventType = "HOLDER_COUNT_UPDATED"   // Holder count updated event
	EventAnalyticsRecalculated EventType = "ANALYTICS_RECALCULATED" // Analytics recalculated event
	EventTrendingUpdated       EventType = "TRENDING_UPDATED"       // Trending updated event

	EventBalanceUpdated    EventType = "BALANCE_UPDATED"    // Balance updated event
	EventTransferCompleted EventType = "TRANSFER_COMPLETED" // Transfer completed event
)
