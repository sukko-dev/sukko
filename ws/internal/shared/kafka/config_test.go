package kafka

import (
	"testing"
)

// =============================================================================
// EventType Constants Tests
// =============================================================================

func TestEventTypeConstants(t *testing.T) {
	t.Parallel()
	// Trading events
	if EventTradeExecuted != "TRADE_EXECUTED" {
		t.Errorf("EventTradeExecuted = %q, want TRADE_EXECUTED", EventTradeExecuted)
	}
	if EventBuyCompleted != "BUY_COMPLETED" {
		t.Errorf("EventBuyCompleted = %q, want BUY_COMPLETED", EventBuyCompleted)
	}
	if EventSellCompleted != "SELL_COMPLETED" {
		t.Errorf("EventSellCompleted = %q, want SELL_COMPLETED", EventSellCompleted)
	}

	// Liquidity events
	if EventLiquidityAdded != "LIQUIDITY_ADDED" {
		t.Errorf("EventLiquidityAdded = %q, want LIQUIDITY_ADDED", EventLiquidityAdded)
	}

	// Creation events
	if EventTokenCreated != "TOKEN_CREATED" {
		t.Errorf("EventTokenCreated = %q, want TOKEN_CREATED", EventTokenCreated)
	}
}
