package history

import "time"

// Clock is the history writer's seam over wall-clock time (ADR-0029). The
// supervised restart backoff and heartbeat loop read time through this interface
// so tests can drive them deterministically with a fake clock; production uses
// realClock, thin wrappers over the time package, so behavior is unchanged.
type Clock interface {
	// After returns a channel that receives once after d elapses.
	After(d time.Duration) <-chan time.Time
	// NewTicker returns a ticker firing every d.
	NewTicker(d time.Duration) Ticker
}

// Ticker abstracts *time.Ticker so a fake clock can fire ticks on demand. The
// channel is exposed as a method (not a field) so the fake can own it.
type Ticker interface {
	// Chan is the tick channel.
	Chan() <-chan time.Time
	// Stop halts the ticker.
	Stop()
}

// realClock is the production Clock: direct time-package calls.
type realClock struct{}

func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (realClock) NewTicker(d time.Duration) Ticker       { return realTicker{t: time.NewTicker(d)} }

// realTicker wraps *time.Ticker to satisfy Ticker.
type realTicker struct{ t *time.Ticker }

func (r realTicker) Chan() <-chan time.Time { return r.t.C }
func (r realTicker) Stop()                  { r.t.Stop() }
