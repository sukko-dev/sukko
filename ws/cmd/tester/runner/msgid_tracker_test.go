package runner

import "testing"

// The tracker must retain the delivered envelope's mid per msg_id so the
// rest-publish suite can assert ack↔delivery mid equality (ADR-0008).
func TestMsgIDTracker_MidCapture(t *testing.T) {
	t.Parallel()

	tr := newMsgIDTracker()
	tr.Add("m1", "cafe-1-2")
	tr.Add("m2", "") // pre-mid server or fan-out publish

	if !tr.Has("m1") || !tr.Has("m2") {
		t.Fatal("presence tracking broken")
	}
	if got := tr.MidOf("m1"); got != "cafe-1-2" {
		t.Errorf("MidOf(m1) = %q, want cafe-1-2", got)
	}
	if got := tr.MidOf("m2"); got != "" {
		t.Errorf("MidOf(m2) = %q, want empty", got)
	}
	if got := tr.MidOf("absent"); got != "" {
		t.Errorf("MidOf(absent) = %q, want empty", got)
	}
	tr.Clear()
	if tr.Has("m1") {
		t.Error("Clear did not clear")
	}
}
