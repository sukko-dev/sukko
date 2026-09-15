package broadcast

// subscriberEntry holds a subscriber channel.
// Used by valkeyBus to track active subscribers.
// Unsubscribe matching uses channel pointer equality (entry.ch == ch).
type subscriberEntry struct {
	ch chan *Message
}
