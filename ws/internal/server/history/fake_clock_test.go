package history_test

import (
	"sync"
	"testing"
	"time"

	"github.com/sukko-dev/sukko/internal/server/history"
)

// fakeClock is a deterministic history.Clock for writer tests (ADR-0029). Timers
// and tickers fire only when the test Advances the clock; BlockUntil lets the
// test wait until the writer has parked the expected number of timers before
// advancing, closing the advance-before-waiter race that would silently drop a
// tick and create a new flake.
type fakeClock struct {
	mu      sync.Mutex
	cond    *sync.Cond
	now     time.Time
	waiters []*fakeWaiter
}

type fakeWaiter struct {
	until  time.Time
	period time.Duration // 0 = one-shot (After); >0 = ticker (re-arms)
	ch     chan time.Time
}

func newFakeClock() *fakeClock {
	c := &fakeClock{now: time.Unix(0, 0)}
	c.cond = sync.NewCond(&c.mu)
	return c
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	w := &fakeWaiter{until: c.now.Add(d), ch: make(chan time.Time, 1)}
	c.waiters = append(c.waiters, w)
	c.cond.Broadcast()
	return w.ch
}

func (c *fakeClock) NewTicker(d time.Duration) history.Ticker {
	c.mu.Lock()
	defer c.mu.Unlock()
	w := &fakeWaiter{until: c.now.Add(d), period: d, ch: make(chan time.Time, 1)}
	c.waiters = append(c.waiters, w)
	c.cond.Broadcast()
	return &fakeTicker{c: c, w: w}
}

// BlockUntil blocks until at least n timers/tickers are registered.
func (c *fakeClock) BlockUntil(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for len(c.waiters) < n {
		c.cond.Wait()
	}
}

// Advance moves the clock forward by d, firing every waiter now due. Sends are
// non-blocking on a size-1 channel, mirroring time.After/time.Ticker: a ticker
// whose receiver is slow drops intermediate ticks rather than blocking.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	kept := c.waiters[:0]
	for _, w := range c.waiters {
		for !w.until.After(c.now) {
			select {
			case w.ch <- w.until:
			default:
			}
			if w.period == 0 {
				break
			}
			w.until = w.until.Add(w.period)
		}
		if w.period == 0 && !w.until.After(c.now) {
			continue // one-shot fired — drop it
		}
		kept = append(kept, w)
	}
	c.waiters = kept
}

type fakeTicker struct {
	c *fakeClock
	w *fakeWaiter
}

func (t *fakeTicker) Chan() <-chan time.Time { return t.w.ch }
func (t *fakeTicker) Stop() {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	for i, w := range t.c.waiters {
		if w == t.w {
			t.c.waiters = append(t.c.waiters[:i], t.c.waiters[i+1:]...)
			break
		}
	}
}

// --- unit tests for the fake itself (de-risk the correctness hazard) ---

func TestFakeClock_AfterFiresOnAdvance(t *testing.T) {
	t.Parallel()
	c := newFakeClock()
	ch := c.After(100 * time.Millisecond)
	select {
	case <-ch:
		t.Fatal("After fired before Advance")
	default:
	}
	c.Advance(99 * time.Millisecond)
	select {
	case <-ch:
		t.Fatal("After fired early (before its deadline)")
	default:
	}
	c.Advance(1 * time.Millisecond)
	select {
	case <-ch:
	default:
		t.Fatal("After did not fire at its deadline")
	}
}

func TestFakeClock_TickerFiresEachPeriod(t *testing.T) {
	t.Parallel()
	c := newFakeClock()
	tk := c.NewTicker(10 * time.Millisecond)
	c.Advance(10 * time.Millisecond)
	select {
	case <-tk.Chan():
	default:
		t.Fatal("ticker did not fire after one period")
	}
	c.Advance(10 * time.Millisecond)
	select {
	case <-tk.Chan():
	default:
		t.Fatal("ticker did not fire after second period")
	}
	tk.Stop()
	c.Advance(10 * time.Millisecond)
	select {
	case <-tk.Chan():
		t.Fatal("ticker fired after Stop")
	default:
	}
}

func TestFakeClock_BlockUntilWaitsForWaiter(t *testing.T) {
	t.Parallel()
	c := newFakeClock()
	done := make(chan struct{})
	go func() {
		c.BlockUntil(1)
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("BlockUntil returned before any waiter registered")
	case <-time.After(20 * time.Millisecond):
	}
	_ = c.After(time.Second) // registers a waiter
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("BlockUntil did not return after a waiter registered")
	}
}

// blockUntilOneShots blocks until at least n one-shot (After) waiters are parked
// — the restart-backoff timer — ignoring the heartbeat ticker (period > 0).
func (c *fakeClock) blockUntilOneShots(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.countOneShotsLocked() < n {
		c.cond.Wait()
	}
}

func (c *fakeClock) countOneShotsLocked() int {
	k := 0
	for _, w := range c.waiters {
		if w.period == 0 {
			k++
		}
	}
	return k
}

// oneShotDelays returns the remaining delay of each parked one-shot (After)
// waiter — the writer's restart-backoff delay, inspectable deterministically.
func (c *fakeClock) oneShotDelays() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []time.Duration
	for _, w := range c.waiters {
		if w.period == 0 {
			out = append(out, w.until.Sub(c.now))
		}
	}
	return out
}
