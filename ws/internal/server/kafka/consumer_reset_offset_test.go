package kafka

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// consumeResetOffset selects the reset policy for never-committed topics.
// > 0 must be AfterMilli(that) so partitions created after the timestamp are
// consumed from offset 0 (fresh-topic race fix, ADR-0010); 0 must stay AtEnd.
func TestConsumeResetOffset(t *testing.T) {
	t.Parallel()

	const boot int64 = 1_725_000_000_000

	// kgo.Offset is comparable and DOES distinguish AfterMilli from At/AtEnd
	// (the afterMilli flag participates in ==), whereas MarshalJSON does NOT
	// encode that flag — so identity comparison is required here: a JSON
	// assertion would let the At(boot) mutant through, and At(boot) is an
	// out-of-range exact offset that clamps to log end, i.e. the #255 bug.
	if got := consumeResetOffset(boot); got != kgo.NewOffset().AfterMilli(boot) {
		t.Errorf("consumeResetOffset(%d) = %v, want AfterMilli(%d)", boot, got, boot)
	}
	if got := consumeResetOffset(0); got != kgo.NewOffset().AtEnd() {
		t.Errorf("consumeResetOffset(0) = %v, want AtEnd", got)
	}
	// Guard the review-caught failure mode: At(boot) must NOT satisfy the AfterMilli check.
	if kgo.NewOffset().At(boot) == kgo.NewOffset().AfterMilli(boot) {
		t.Fatal("At(ts) and AfterMilli(ts) compare equal — this test cannot discriminate the fix from its bug-equivalent mutant")
	}
}
