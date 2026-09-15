package runner

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

// Fast bounds for tests — the production values (pushAvailableTimeout/Interval) are only
// wired at the call site, so the helper's retry contract is tested independently of them.
const (
	testProbeTimeout  = 200 * time.Millisecond
	testProbeInterval = 10 * time.Millisecond
)

func TestWaitForPushAvailable(t *testing.T) {
	t.Parallel()

	errProbe := errors.New("probe failed")

	tests := []struct {
		name string
		// sequence of statuses the fake probe returns; the last entry repeats forever.
		statuses   []int
		wantStatus int
		wantErr    bool
		// wantMaxProbes asserts terminal statuses short-circuit (no retries burned).
		wantMaxProbes int64
	}{
		{name: "immediate 200 pass", statuses: []int{http.StatusOK}, wantStatus: http.StatusOK, wantErr: false, wantMaxProbes: 1},
		{name: "immediate 403 edition skip", statuses: []int{http.StatusForbidden}, wantStatus: http.StatusForbidden, wantErr: true, wantMaxProbes: 1},
		{name: "immediate 503 not deployed skip", statuses: []int{http.StatusServiceUnavailable}, wantStatus: http.StatusServiceUnavailable, wantErr: true, wantMaxProbes: 1},
		{name: "401 propagation race converges to 200", statuses: []int{http.StatusUnauthorized, http.StatusUnauthorized, http.StatusOK}, wantStatus: http.StatusOK, wantErr: false},
		{name: "500 transient converges to 200", statuses: []int{http.StatusInternalServerError, http.StatusOK}, wantStatus: http.StatusOK, wantErr: false},
		{name: "transport error converges to 200", statuses: []int{0, http.StatusOK}, wantStatus: http.StatusOK, wantErr: false},
		{name: "persistent 401 fails with last status after timeout", statuses: []int{http.StatusUnauthorized}, wantStatus: http.StatusUnauthorized, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var calls atomic.Int64
			probe := func(_ context.Context) (int, error) {
				n := calls.Add(1)
				idx := min(int(n)-1, len(tt.statuses)-1)
				status := tt.statuses[idx]
				if status == http.StatusOK {
					return status, nil
				}
				return status, errProbe
			}

			status, err := waitForPushAvailable(context.Background(), testProbeTimeout, testProbeInterval, probe)

			if status != tt.wantStatus {
				t.Errorf("status = %d, want %d", status, tt.wantStatus)
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("err = %v, wantErr = %v", err, tt.wantErr)
			}
			if tt.wantMaxProbes > 0 && calls.Load() > tt.wantMaxProbes {
				t.Errorf("probe called %d times, want at most %d (terminal status must not retry)", calls.Load(), tt.wantMaxProbes)
			}
		})
	}
}

func TestWaitForPushAvailable_CtxCancelReturnsLastObservation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	probe := func(_ context.Context) (int, error) {
		cancel() // cancel after the first (non-terminal) probe — the retry loop must exit promptly
		return http.StatusUnauthorized, errors.New("key not yet propagated")
	}

	start := time.Now()
	status, err := waitForPushAvailable(ctx, 5*time.Second, time.Millisecond, probe)

	if status != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", status, http.StatusUnauthorized)
	}
	if err == nil {
		t.Error("err = nil, want last probe error")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("cancel took %v to unblock, want prompt exit", elapsed)
	}
}
