package runner

import (
	"errors"
	"testing"
)

// classifyIngestFailure drives ingestTracked's recovery choice. The distinction
// under test is load-bearing for the recovery suites: republishing after a mere
// delivery timeout would drop an ORPHAN duplicate into the replay window
// (breaking exact messages_replayed counts), so only a publish-side failure may
// trigger a republish; a slow delivery must extend the wait instead.
func TestClassifyIngestFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		res  DeliveryResult
		want ingestRecovery
	}{
		{
			name: "delivered — nothing to recover",
			res:  DeliveryResult{Delivered: true},
			want: ingestDone,
		},
		{
			name: "publish failed — record never reached the log, republish is safe",
			res:  DeliveryResult{Delivered: false, PublishErr: errors.New("broker down")},
			want: ingestRepublish,
		},
		{
			name: "publish acked but delivery timed out — record IS in the log, only re-wait",
			res:  DeliveryResult{Delivered: false, Missing: []string{"user-a"}},
			want: ingestExtendWait,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := classifyIngestFailure(tt.res); got != tt.want {
				t.Errorf("classifyIngestFailure() = %v, want %v", got, tt.want)
			}
		})
	}
}
