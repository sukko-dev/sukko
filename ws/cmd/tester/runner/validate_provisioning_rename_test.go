package runner

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

// tenantSlugPattern mirrors internal/provisioning/types.go ValidateSlug: 3–63 chars, lowercase
// alphanumeric + hyphen, must start with a letter. buildRenameSlug outputs MUST match it.
var tenantSlugPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{2,62}$`)

func TestAllowedPatternsContain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		body    string
		pattern string
		want    bool
	}{
		{
			name:    "present",
			body:    `{"allowed_patterns":["room.vip","general.*"]}`,
			pattern: "room.vip",
			want:    true,
		},
		{
			name:    "absent (falls to default+public, no group match)",
			body:    `{"allowed_patterns":["general.*","dm.{principal}"]}`,
			pattern: "room.vip",
			want:    false,
		},
		{
			name:    "empty allowed_patterns",
			body:    `{"allowed_patterns":[]}`,
			pattern: "room.vip",
			want:    false,
		},
		{
			name:    "malformed body → false (assertion fails loudly at caller)",
			body:    `{not json`,
			pattern: "room.vip",
			want:    false,
		},
		{
			name:    "missing field → false",
			body:    `{"other":"x"}`,
			pattern: "room.vip",
			want:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := allowedPatternsContain([]byte(tc.body), tc.pattern); got != tc.want {
				t.Errorf("allowedPatternsContain(%q, %q) = %v, want %v", tc.body, tc.pattern, got, tc.want)
			}
		})
	}
}

func TestParseAllowedPatterns(t *testing.T) {
	t.Parallel()

	patterns, err := parseAllowedPatterns([]byte(`{"allowed_patterns":["a","b"]}`))
	if err != nil {
		t.Fatalf("parseAllowedPatterns: %v", err)
	}
	if len(patterns) != 2 || patterns[0] != "a" || patterns[1] != "b" {
		t.Errorf("patterns = %v, want [a b]", patterns)
	}

	if _, err := parseAllowedPatterns([]byte(`{not json`)); err == nil {
		t.Error("expected error for malformed body")
	}

	empty, err := parseAllowedPatterns([]byte(`{"allowed_patterns":[]}`))
	if err != nil {
		t.Fatalf("parseAllowedPatterns empty: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("empty patterns = %v, want []", empty)
	}
}

func TestBuildRenameSlug(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		runID string
	}{
		{name: "typical run id", runID: "abc123def456"},
		{name: "uppercase + underscores sanitized", runID: "Run_ID_With_UPPER"},
		{name: "digit-leading run id (prefix keeps letter-first)", runID: "1abc"},
		{name: "very long run id (must be capped to 63)", runID: strings.Repeat("x", 200)},
		{name: "empty run id", runID: ""},
		{name: "special chars", runID: "a.b/c:d"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			slug := buildRenameSlug(tc.runID)
			if len(slug) > maxSlugLen {
				t.Errorf("slug %q length %d exceeds maxSlugLen %d", slug, len(slug), maxSlugLen)
			}
			if !tenantSlugPattern.MatchString(slug) {
				t.Errorf("slug %q does not match ValidateSlug pattern %s", slug, tenantSlugPattern)
			}
			if !strings.HasPrefix(slug, "renamed-") {
				t.Errorf("slug %q must start with letter-first prefix renamed-", slug)
			}
		})
	}

	// Uniqueness per invocation (random suffix) — two builds with the same runID must differ.
	a := buildRenameSlug("same-run")
	b := buildRenameSlug("same-run")
	if a == b {
		t.Errorf("buildRenameSlug produced identical slugs %q, %q — must be unique per run", a, b)
	}
}

func TestSanitizeSlugComponent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   string
		want string
	}{
		{in: "abc123", want: "abc123"},
		{in: "ABC", want: "abc"},
		{in: "a_b", want: "a-b"},
		{in: "a.b/c", want: "a-b-c"},
		{in: "already-hyphen", want: "already-hyphen"},
	}
	for _, tc := range tests {
		if got := sanitizeSlugComponent(tc.in); got != tc.want {
			t.Errorf("sanitizeSlugComponent(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestRenameTestAccessCheckNamesInE2E is the drift guard: every emitted check name
// (the exported RenameTestAccessCheckNames set) MUST appear as provisioning:<name> in the
// REQUIRE_PASS of all three provisioning cells (community-direct, pro-kafka, pro-direct) in
// taskfiles/e2e.yml. A rename/typo in a Go constant that is not mirrored into e2e.yml — or a cell
// that drops a name — is a test failure here, not a silent vacuous gate (the #205 R11 lesson).
func TestRenameTestAccessCheckNamesInE2E(t *testing.T) {
	t.Parallel()

	// Locate taskfiles/e2e.yml relative to this test file (repo-root/taskfiles/e2e.yml).
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed — cannot locate this test file")
	}
	// thisFile = <repo>/ws/cmd/tester/runner/validate_provisioning_rename_test.go
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", "..", ".."))
	e2ePath := filepath.Join(repoRoot, "taskfiles", "e2e.yml")

	raw, err := os.ReadFile(e2ePath)
	if err != nil {
		t.Fatalf("read %s: %v", e2ePath, err)
	}
	content := string(raw)

	// Extract each cell's REQUIRE_PASS line so a name present in one cell but absent in another is caught.
	cells := []string{"cell:community-direct", "cell:pro-kafka", "cell:pro-direct"}
	for _, cell := range cells {
		requirePass := extractCellRequirePass(t, content, cell)
		for _, name := range RenameTestAccessCheckNames {
			token := "provisioning:" + name
			if !strings.Contains(requirePass, token) {
				t.Errorf("cell %s REQUIRE_PASS is missing %q — the Go check name is not gated into e2e.yml (drift)", cell, token)
			}
		}
	}

	// Sanity: exactly 10 frozen names, none containing the REQUIRE_PASS separator.
	if len(RenameTestAccessCheckNames) != 10 {
		t.Errorf("expected 10 frozen check names, got %d", len(RenameTestAccessCheckNames))
	}
	for _, name := range RenameTestAccessCheckNames {
		if strings.Contains(name, ";") {
			t.Errorf("check name %q contains the REQUIRE_PASS separator ';'", name)
		}
	}
}

// extractCellRequirePass returns the REQUIRE_PASS string for the named cell block. It finds the
// cell header, then the first REQUIRE_PASS: line at or after it, and returns that line's value.
func extractCellRequirePass(t *testing.T, content, cell string) string {
	t.Helper()
	cellIdx := strings.Index(content, cell+":")
	if cellIdx < 0 {
		t.Fatalf("cell %q not found in e2e.yml", cell)
	}
	rest := content[cellIdx:]
	rpIdx := strings.Index(rest, "REQUIRE_PASS:")
	if rpIdx < 0 {
		t.Fatalf("cell %q has no REQUIRE_PASS line", cell)
	}
	line := rest[rpIdx:]
	if nl := strings.IndexByte(line, '\n'); nl >= 0 {
		line = line[:nl]
	}
	return line
}
