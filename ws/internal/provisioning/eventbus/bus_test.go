package eventbus

import (
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func newTestBus() *Bus {
	logger := zerolog.Nop()
	return New(logger)
}

func TestBus_PublishSubscribe(t *testing.T) {
	t.Parallel()

	bus := newTestBus()
	id, ch := bus.Subscribe()
	defer bus.Unsubscribe(id)

	bus.Publish(Event{Type: KeysChanged})

	select {
	case event := <-ch:
		if event.Type != KeysChanged {
			t.Errorf("expected %s, got %s", KeysChanged, event.Type)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestBus_MultipleEventTypes(t *testing.T) {
	t.Parallel()

	bus := newTestBus()
	id, ch := bus.Subscribe()
	defer bus.Unsubscribe(id)

	types := []EventType{KeysChanged, TenantConfigChanged, TopicsChanged}
	for _, et := range types {
		bus.Publish(Event{Type: et})
	}

	for _, expected := range types {
		select {
		case event := <-ch:
			if event.Type != expected {
				t.Errorf("expected %s, got %s", expected, event.Type)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for event %s", expected)
		}
	}
}

func TestBus_MultipleSubscribers(t *testing.T) {
	t.Parallel()

	bus := newTestBus()

	const numSubscribers = 5
	ids := make([]int, numSubscribers)
	chs := make([]<-chan Event, numSubscribers)

	for i := range numSubscribers {
		ids[i], chs[i] = bus.Subscribe()
	}
	defer func() {
		for _, id := range ids {
			bus.Unsubscribe(id)
		}
	}()

	bus.Publish(Event{Type: TopicsChanged})

	for i, ch := range chs {
		select {
		case event := <-ch:
			if event.Type != TopicsChanged {
				t.Errorf("subscriber %d: expected %s, got %s", i, TopicsChanged, event.Type)
			}
		case <-time.After(time.Second):
			t.Fatalf("subscriber %d: timed out waiting for event", i)
		}
	}
}

func TestBus_NonBlockingFanOut_SlowSubscriber(t *testing.T) {
	t.Parallel()

	bus := newTestBus()

	// Fast subscriber
	fastID, fastCh := bus.Subscribe()
	defer bus.Unsubscribe(fastID)

	// Slow subscriber — never reads from channel
	slowID, _ := bus.Subscribe()
	defer bus.Unsubscribe(slowID)

	// Publish more events than subscriber buffer size to trigger drops
	for range subscriberBufferSize + 5 {
		bus.Publish(Event{Type: KeysChanged})
	}

	// Fast subscriber should still receive events (first subscriberBufferSize)
	received := 0
drain:
	for range subscriberBufferSize {
		select {
		case <-fastCh:
			received++
		case <-time.After(time.Second):
			break drain
		}
	}

	if received == 0 {
		t.Error("fast subscriber should have received events despite slow subscriber")
	}
}

func TestBus_UnsubscribeStopsDelivery(t *testing.T) {
	t.Parallel()

	bus := newTestBus()
	id, ch := bus.Subscribe()

	// Unsubscribe — channel should be closed
	bus.Unsubscribe(id)

	// Channel should be closed (reads return zero value, ok=false)
	_, ok := <-ch
	if ok {
		t.Error("channel should be closed after unsubscribe")
	}

	// Publishing after unsubscribe should not panic
	bus.Publish(Event{Type: KeysChanged})
}

func TestBus_UnsubscribeIdempotent(t *testing.T) {
	t.Parallel()

	bus := newTestBus()
	id, _ := bus.Subscribe()

	// Double unsubscribe should not panic
	bus.Unsubscribe(id)
	bus.Unsubscribe(id)
}

func TestBus_SubscriberCount(t *testing.T) {
	t.Parallel()

	bus := newTestBus()

	if count := bus.SubscriberCount(); count != 0 {
		t.Errorf("expected 0 subscribers, got %d", count)
	}

	id1, _ := bus.Subscribe()
	id2, _ := bus.Subscribe()

	if count := bus.SubscriberCount(); count != 2 {
		t.Errorf("expected 2 subscribers, got %d", count)
	}

	bus.Unsubscribe(id1)
	if count := bus.SubscriberCount(); count != 1 {
		t.Errorf("expected 1 subscriber, got %d", count)
	}

	bus.Unsubscribe(id2)
	if count := bus.SubscriberCount(); count != 0 {
		t.Errorf("expected 0 subscribers, got %d", count)
	}
}

func TestBus_ConcurrentPublishSubscribe(t *testing.T) {
	t.Parallel()

	bus := newTestBus()
	const numGoroutines = 10
	const eventsPerGoroutine = 100

	var wg sync.WaitGroup

	// Concurrent subscribers
	for range numGoroutines {
		wg.Go(func() {
			id, ch := bus.Subscribe()
			defer bus.Unsubscribe(id)

			// Drain a few events then leave
			for range 5 {
				select {
				case <-ch:
				case <-time.After(100 * time.Millisecond):
					return
				}
			}
		})
	}

	// Concurrent publishers
	for range numGoroutines {
		wg.Go(func() {
			for range eventsPerGoroutine {
				bus.Publish(Event{Type: KeysChanged})
			}
		})
	}

	wg.Wait()
}

func TestBus_PublishToEmptyBus(t *testing.T) {
	t.Parallel()

	bus := newTestBus()
	// Publishing to an empty bus should not panic
	bus.Publish(Event{Type: KeysChanged})
	bus.Publish(Event{Type: TenantConfigChanged})
	bus.Publish(Event{Type: TopicsChanged})
}
