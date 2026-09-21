package runner

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
)

func TestParseTopicsPage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		body      string
		wantItems int
		wantTotal int
		wantFirst string
	}{
		{"valid", `{"items":[{"suffix":"default"},{"suffix":"a"}],"total":2,"limit":50,"offset":0}`, 2, 2, "default"},
		{"empty", `{"items":[],"total":0}`, 0, 0, ""},
		{"malformed → zero values", `not json`, 0, 0, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := parseTopicsPage([]byte(tc.body))
			if len(p.Items) != tc.wantItems || p.Total != tc.wantTotal {
				t.Fatalf("items=%d total=%d, want items=%d total=%d", len(p.Items), p.Total, tc.wantItems, tc.wantTotal)
			}
			if tc.wantFirst != "" && (len(p.Items) == 0 || p.Items[0].Suffix != tc.wantFirst) {
				t.Errorf("first suffix mismatch, want %q", tc.wantFirst)
			}
		})
	}
}

func TestTopicsContain(t *testing.T) {
	t.Parallel()
	p := parseTopicsPage([]byte(`{"items":[{"suffix":"default"},{"suffix":"analytics"}]}`))
	if !topicsContain(p, "analytics") {
		t.Error("expected analytics present")
	}
	if topicsContain(p, "missing") {
		t.Error("did not expect missing present")
	}
}

func TestNonDefaultTopicCount(t *testing.T) {
	t.Parallel()
	client, closeFn := newTestProvClient(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"items":[{"suffix":"default"},{"suffix":"a"},{"suffix":"b"}],"total":3,"limit":200,"offset":0}`))
	}))
	defer closeFn()
	n, err := nonDefaultTopicCount(context.Background(), client, "t")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 2 {
		t.Errorf("count = %d, want 2 (default excluded)", n)
	}
}

// TestCheckTopicsQuotaWall_Community asserts the Pro-gated quota probe propagates the
// 403 EDITION_LIMIT so the shared provRoutingCheck wrapper records a skip (never a pass).
func TestCheckTopicsQuotaWall_Community(t *testing.T) {
	t.Parallel()
	client, closeFn := newTestProvClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/quotas") {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"code":"EDITION_LIMIT","message":"feature not available"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer closeFn()
	err := checkTopicsQuotaWall(context.Background(), client, "t")
	if err == nil || extractErrorCode(err.Error()) != errCodeEditionLimit {
		t.Fatalf("expected EDITION_LIMIT error (→ skip), got %v", err)
	}
}

// TestCheckTopicsQuotaWall_Enforced drives the Pro happy path: read quota, set it to count+1,
// first create succeeds, the next is rejected 400 TOO_MANY_TOPICS.
func TestCheckTopicsQuotaWall_Enforced(t *testing.T) {
	t.Parallel()
	var creates atomic.Int32
	client, closeFn := newTestProvClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/quotas"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"max_topics":50}`))
		case r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/quotas"):
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/topics"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"items":[{"suffix":"default"}],"total":1,"limit":200,"offset":0}`))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/topics"):
			if creates.Add(1) == 1 {
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(`{"topic":{"suffix":"e2e-quota-fill"}}`))
			} else {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"code":"TOO_MANY_TOPICS","message":"quota exceeded"}`))
			}
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer closeFn()
	if err := checkTopicsQuotaWall(context.Background(), client, "t"); err != nil {
		t.Fatalf("expected quota wall to pass, got %v", err)
	}
}

// TestTopicsCheckNamesInE2E is the drift guard: every ungated topics check must be
// present-and-pass in all three provisioning cells' REQUIRE_PASS, and the Pro-gated quota
// check must be REQUIRE_PASS on the Pro cells while ALLOWED_SKIPS on community-direct. A typo
// or a forgotten cell would let a topics check silently vacuous-pass.
func TestTopicsCheckNamesInE2E(t *testing.T) {
	t.Parallel()

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed — cannot locate this test file")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", "..", ".."))
	raw, err := os.ReadFile(filepath.Join(repoRoot, "taskfiles", "e2e.yml"))
	if err != nil {
		t.Fatalf("read e2e.yml: %v", err)
	}
	content := string(raw)

	cells := []string{"cell:community-direct", "cell:pro-kafka", "cell:pro-direct"}
	for _, cell := range cells {
		requirePass := extractCellRequirePass(t, content, cell)
		for _, name := range TopicsAlwaysPassCheckNames {
			if !strings.Contains(requirePass, "provisioning:"+name) {
				t.Errorf("cell %s REQUIRE_PASS is missing %q (drift)", cell, "provisioning:"+name)
			}
		}
	}

	// Quota check: REQUIRE_PASS on Pro cells; ALLOWED_SKIPS on community-direct.
	quotaTok := "provisioning:" + TopicsQuotaCheckName
	for _, cell := range []string{"cell:pro-kafka", "cell:pro-direct"} {
		if rp := extractCellRequirePass(t, content, cell); !strings.Contains(rp, quotaTok) {
			t.Errorf("cell %s REQUIRE_PASS is missing %q", cell, quotaTok)
		}
	}
	if sk := extractCellAllowedSkips(t, content, "cell:community-direct"); !strings.Contains(sk, quotaTok) {
		t.Errorf("cell community-direct ALLOWED_SKIPS is missing %q (quota is Pro-gated → skips on Community)", quotaTok)
	}

	if len(TopicsAlwaysPassCheckNames) != 9 {
		t.Errorf("expected 9 always-pass topic check names, got %d", len(TopicsAlwaysPassCheckNames))
	}
	for _, name := range append(append([]string{}, TopicsAlwaysPassCheckNames...), TopicsQuotaCheckName) {
		if strings.Contains(name, ";") {
			t.Errorf("check name %q contains the REQUIRE_PASS separator ';'", name)
		}
	}
}

// extractCellAllowedSkips returns the ALLOWED_SKIPS line for the named cell block.
func extractCellAllowedSkips(t *testing.T, content, cell string) string {
	t.Helper()
	cellIdx := strings.Index(content, cell+":")
	if cellIdx < 0 {
		t.Fatalf("cell %q not found in e2e.yml", cell)
	}
	rest := content[cellIdx:]
	skIdx := strings.Index(rest, "ALLOWED_SKIPS:")
	if skIdx < 0 {
		t.Fatalf("cell %q has no ALLOWED_SKIPS line", cell)
	}
	line := rest[skIdx:]
	if nl := strings.IndexByte(line, '\n'); nl >= 0 {
		line = line[:nl]
	}
	return line
}
