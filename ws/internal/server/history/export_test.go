package history

// SetClockForTest injects a deterministic clock and jitter source into the
// writer before Run is launched (ADR-0029). Test-only: this file has the
// _test.go suffix and is never compiled into the production binary. randN may be
// nil to leave the jitter source unchanged.
func SetClockForTest(w *Writer, c Clock, randN func(n int64) int64) {
	w.clock = c
	if randN != nil {
		w.randInt64N = randN
	}
}
