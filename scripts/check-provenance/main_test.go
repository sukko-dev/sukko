package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func testConfig() Config {
	return Config{
		Domain:          "sukko.dev",
		ExtraCommitters: []string{"noreply@github.com"},
		SkipPaths:       []string{"scripts/check-provenance/", ".githooks/"},
		Markers:         parseMarkers(defaultMarkers),
	}
}

func TestEmailAllowed(t *testing.T) {
	tests := []struct {
		name   string
		email  string
		domain string
		want   bool
	}{
		{"exact domain", "red@sukko.dev", "sukko.dev", true},
		{"uppercase local and domain", "RED@SUKKO.DEV", "sukko.dev", true},
		{"lookalike suffix domain", "red@notsukko.dev", "sukko.dev", false},
		{"subdomain", "red@mail.sukko.dev", "sukko.dev", false},
		{"different domain", "dev@example.org", "sukko.dev", false},
		{"empty email", "", "sukko.dev", false},
		{"domain only no local part", "@sukko.dev", "sukko.dev", false},
		{"domain as local part", "sukko.dev@evil.com", "sukko.dev", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := emailAllowed(tt.email, tt.domain); got != tt.want {
				t.Errorf("emailAllowed(%q, %q) = %v, want %v", tt.email, tt.domain, got, tt.want)
			}
		})
	}
}

func TestParseMarkers(t *testing.T) {
	raw := "# comment line\nCo-Authored-By: Claude\n\n  \nclaude.ai/code\n"
	got := parseMarkers(raw).substrings
	want := []string{"co-authored-by: claude", "claude.ai/code"}
	if len(got) != len(want) {
		t.Fatalf("parseMarkers returned %d markers, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("marker[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestParseMarkersRegex(t *testing.T) {
	raw := "plain marker\nre:\\bFR-\\d+\\b\nre:\\bT\\d{3}\\b\n"
	m := parseMarkers(raw)
	if len(m.substrings) != 1 || m.substrings[0] != "plain marker" {
		t.Fatalf("substrings = %v, want [plain marker]", m.substrings)
	}
	if len(m.regexps) != 2 {
		t.Fatalf("got %d regexps, want 2", len(m.regexps))
	}
	tests := []struct {
		text string
		want bool
	}{
		{"implements FR-029 fully", true},
		{"fr-029 lowercase", true}, // regex compiled case-insensitive like substrings
		{"FRAME-1 is not a spec id", false},
		{"task T042 done", true},
		{"T42 too short", false},
		{"CONNECT042 embedded", false},
	}
	cfg := Config{Domain: "sukko.dev", Markers: m}
	for _, tt := range tests {
		c := Commit{Hash: "aaa", AuthorEmail: "red@sukko.dev", CommitterEmail: "red@sukko.dev", Message: tt.text}
		got := len(checkCommit(c, cfg)) > 0
		if got != tt.want {
			t.Errorf("message %q: flagged=%v, want %v", tt.text, got, tt.want)
		}
	}
}

func TestParseMarkersBadRegexFailsClosed(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("invalid re: line must panic (fail closed at startup), got nil")
		}
	}()
	parseMarkers("re:[unclosed\n")
}

func TestDefaultMarkersParse(t *testing.T) {
	markers := parseMarkers(defaultMarkers)
	if len(markers.substrings) == 0 {
		t.Fatal("embedded ai-markers.txt produced no markers")
	}
	for _, m := range markers.substrings {
		if m != strings.ToLower(m) {
			t.Errorf("marker %q is not lowercased", m)
		}
	}
	// The four retired-pipeline ID families must be ARMED — a re-commented
	// or typo'd re: line silently disables enforcement.
	if got := len(markers.regexps); got != 4 {
		t.Fatalf("embedded regex markers = %d, want 4 (retired-ID families)", got)
	}
}

// TestEmbeddedRegexesFire pins that the ARMED embedded patterns actually
// catch each retired-ID family, including the letter-suffixed variants that
// a bare \b…\d\b pattern is structurally blind to.
func TestEmbeddedRegexesFire(t *testing.T) {
	cfg := Config{
		Domain:          "sukko.dev",
		ExtraCommitters: []string{"noreply@github.com"},
		SkipPaths:       splitList(defaultSkipPaths),
		Markers:         parseMarkers(defaultMarkers),
	}
	tests := []struct {
		name string
		text string
		want bool
	}{
		{"FR plain", "implements FR-029 fully", true},
		{"FR suffixed", "jti mandatory (FR-001a)", true},
		{"T plain", "task T042 done", true},
		{"T suffixed", "guard case T013b", true},
		{"SC plain", "covers SC-021 mapping", true},
		{"SC suffixed", "close reason SC-001b", true},
		{"NFR plain", "violates NFR-002", true},
		{"NFR suffixed", "budget NFR-002b holds", true},
		{"lowercase does NOT fire (base64/go.sum safety)", "h1:qt123abc=", false},
		{"clean prose", "the revocation preflight check", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Commit{Hash: "aaa", AuthorEmail: "red@sukko.dev", CommitterEmail: "red@sukko.dev",
				Message: "feat: x", Added: []AddedLine{{Path: "ws/some/file.go", Text: tt.text}}}
			got := len(checkCommit(c, cfg)) > 0
			if got != tt.want {
				t.Errorf("text %q: flagged=%v, want %v", tt.text, got, tt.want)
			}
		})
	}
	// scripts/check-publish/ is skiplisted — the publish gate's own deny-list
	// legitimately contains the marker strings it denies.
	c := Commit{Hash: "aaa", AuthorEmail: "red@sukko.dev", CommitterEmail: "red@sukko.dev",
		Message: "feat: x", Added: []AddedLine{{Path: "scripts/check-publish/publish-markers.txt", Text: "Co-Authored-By: Claude"}}}
	if f := checkCommit(c, cfg); len(f) != 0 {
		t.Errorf("scripts/check-publish/ path must be skiplisted, got findings: %v", f)
	}
}

func TestCheckCommitIdentity(t *testing.T) {
	tests := []struct {
		name      string
		commit    Commit
		wantKinds []string
	}{
		{
			name:      "clean commit",
			commit:    Commit{Hash: "aaa", AuthorEmail: "red@sukko.dev", CommitterEmail: "red@sukko.dev", Message: "feat: x"},
			wantKinds: nil,
		},
		{
			name:      "bad author good committer",
			commit:    Commit{Hash: "aaa", AuthorEmail: "dev@example.org", CommitterEmail: "red@sukko.dev", Message: "feat: x"},
			wantKinds: []string{"author-email"},
		},
		{
			name:      "good author bad committer",
			commit:    Commit{Hash: "aaa", AuthorEmail: "red@sukko.dev", CommitterEmail: "dev@example.org", Message: "feat: x"},
			wantKinds: []string{"committer-email"},
		},
		{
			name:      "both bad",
			commit:    Commit{Hash: "aaa", AuthorEmail: "a@b.c", CommitterEmail: "a@b.c", Message: "feat: x"},
			wantKinds: []string{"author-email", "committer-email"},
		},
		{
			name:      "github merge machinery committer allowed",
			commit:    Commit{Hash: "aaa", AuthorEmail: "red@sukko.dev", CommitterEmail: "noreply@github.com", Message: "Merge pull request #1", IsMerge: true},
			wantKinds: nil,
		},
		{
			name:      "github committer on squash (non-merge) also allowed",
			commit:    Commit{Hash: "aaa", AuthorEmail: "red@sukko.dev", CommitterEmail: "noreply@github.com", Message: "feat: squashed (#1)"},
			wantKinds: nil,
		},
		{
			name:      "github committer does not excuse bad author",
			commit:    Commit{Hash: "aaa", AuthorEmail: "dev@example.org", CommitterEmail: "noreply@github.com", Message: "Merge pull request #1", IsMerge: true},
			wantKinds: []string{"author-email"},
		},
		{
			name:      "lookalike author domain rejected",
			commit:    Commit{Hash: "aaa", AuthorEmail: "red@notsukko.dev", CommitterEmail: "red@sukko.dev", Message: "feat: x"},
			wantKinds: []string{"author-email"},
		},
	}
	cfg := testConfig()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findings := checkCommit(tt.commit, cfg)
			assertKinds(t, findings, tt.wantKinds)
		})
	}
}

func TestCheckCommitMarkers(t *testing.T) {
	tests := []struct {
		name      string
		commit    Commit
		wantKinds []string
	}{
		{
			name: "marker in message",
			commit: Commit{
				Hash: "aaa", AuthorEmail: "red@sukko.dev", CommitterEmail: "red@sukko.dev",
				Message: "feat: add thing\n\nCo-Authored-By: Claude <noreply@anthropic.com>",
			},
			wantKinds: []string{"message-marker"}, // first match wins; one violation rejects
		},
		{
			name: "marker in message case-insensitive",
			commit: Commit{
				Hash: "aaa", AuthorEmail: "red@sukko.dev", CommitterEmail: "red@sukko.dev",
				Message: "feat: x\n\nco-authored-by: claude <x@y.z>",
			},
			wantKinds: []string{"message-marker"},
		},
		{
			name: "robot emoji in message",
			commit: Commit{
				Hash: "aaa", AuthorEmail: "red@sukko.dev", CommitterEmail: "red@sukko.dev",
				Message: "feat: x\n\n🤖 Generated",
			},
			wantKinds: []string{"message-marker"},
		},
		{
			name: "marker in added line",
			commit: Commit{
				Hash: "aaa", AuthorEmail: "red@sukko.dev", CommitterEmail: "red@sukko.dev",
				Message: "docs: update",
				Added:   []AddedLine{{Path: "README.md", Text: "built with claude.ai/code"}},
			},
			wantKinds: []string{"content-marker"},
		},
		{
			name: "marker in skiplisted path ignored",
			commit: Commit{
				Hash: "aaa", AuthorEmail: "red@sukko.dev", CommitterEmail: "red@sukko.dev",
				Message: "chore: extend hook",
				Added:   []AddedLine{{Path: ".githooks/pre-commit", Text: `grep "Co-Authored-By: Claude"`}},
			},
			wantKinds: nil,
		},
		{
			name: "skiplist matches prefix not substring",
			commit: Commit{
				Hash: "aaa", AuthorEmail: "red@sukko.dev", CommitterEmail: "red@sukko.dev",
				Message: "docs: x",
				Added:   []AddedLine{{Path: "docs/.githooks/note.md", Text: "Co-Authored-By: Claude"}},
			},
			wantKinds: []string{"content-marker"},
		},
		{
			name: "clean added lines",
			commit: Commit{
				Hash: "aaa", AuthorEmail: "red@sukko.dev", CommitterEmail: "red@sukko.dev",
				Message: "feat: x",
				Added:   []AddedLine{{Path: "ws/main.go", Text: "func main() {}"}},
			},
			wantKinds: nil,
		},
	}
	cfg := testConfig()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findings := checkCommit(tt.commit, cfg)
			assertKinds(t, findings, tt.wantKinds)
		})
	}
}

func assertKinds(t *testing.T, findings []Finding, wantKinds []string) {
	t.Helper()
	if len(findings) != len(wantKinds) {
		t.Fatalf("got %d findings %v, want kinds %v", len(findings), findings, wantKinds)
	}
	got := map[string]int{}
	for _, f := range findings {
		got[f.Kind]++
	}
	want := map[string]int{}
	for _, k := range wantKinds {
		want[k]++
	}
	for k, n := range want {
		if got[k] != n {
			t.Errorf("kind %q: got %d, want %d (findings: %v)", k, got[k], n, findings)
		}
	}
}

// --- integration: real git repo ---

func gitEnv(authorEmail, committerEmail string) []string {
	return append(os.Environ(),
		"GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL="+authorEmail,
		"GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL="+committerEmail,
	)
}

func runGit(t *testing.T, dir string, env []string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = env
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

func commitFile(t *testing.T, dir string, env []string, path, content, msg string) {
	t.Helper()
	full := filepath.Join(dir, path)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit(t, dir, env, "add", ".")
	runGit(t, dir, env, "commit", "-m", msg)
}

func TestLoadCommitsIntegration(t *testing.T) {
	dir := t.TempDir()
	good := gitEnv("red@sukko.dev", "red@sukko.dev")
	bad := gitEnv("dev@example.org", "dev@example.org")

	runGit(t, dir, good, "init", "-q", "-b", "main")
	commitFile(t, dir, good, "base.txt", "base\n", "chore: base")
	runGit(t, dir, good, "tag", "base")

	commitFile(t, dir, good, "a.txt", "clean content\n", "feat: clean")
	commitFile(t, dir, bad, "b.txt", "more\n", "feat: wrong identity")
	commitFile(t, dir, good, "c.txt", "made with claude.ai/code\n", "feat: tainted content")
	// A commit that REMOVES the tainted line must produce no content finding.
	commitFile(t, dir, good, "c.txt", "clean now\n", "fix: remove marker")

	commits, err := loadCommits(dir, "base..HEAD")
	if err != nil {
		t.Fatalf("loadCommits: %v", err)
	}
	if len(commits) != 4 {
		t.Fatalf("got %d commits, want 4", len(commits))
	}

	cfg := testConfig()
	var all []Finding
	for _, c := range commits {
		all = append(all, checkCommit(c, cfg)...)
	}
	kinds := map[string]int{}
	for _, f := range all {
		kinds[f.Kind]++
	}
	// wrong identity commit: author + committer; tainted commit: one content marker.
	if kinds["author-email"] != 1 || kinds["committer-email"] != 1 || kinds["content-marker"] != 1 {
		t.Errorf("unexpected findings: %v", all)
	}
	if len(all) != 3 {
		t.Errorf("got %d findings, want 3: %v", len(all), all)
	}
}

// Regression tests for prefix-based diff parsing failing open: header lines
// (`+++ b/...`) must be discriminated from content by hunk state, not prefix —
// an added content line starting with "++" renders as "+++...".
func TestLoadCommitsUnicodePath(t *testing.T) {
	dir := t.TempDir()
	good := gitEnv("red@sukko.dev", "red@sukko.dev")
	runGit(t, dir, good, "init", "-q", "-b", "main")
	commitFile(t, dir, good, "base.txt", "base\n", "chore: base")
	runGit(t, dir, good, "tag", "base")
	// core.quotePath default renders this as `+++ "b/\346..."` — must not
	// cause the file's added lines to escape the scan.
	commitFile(t, dir, good, "文档.md", "built with claude.ai/code\n", "docs: unicode path")

	assertContentMarkers(t, dir, "base..HEAD", 1)
}

func TestLoadCommitsPlusPlusContentLine(t *testing.T) {
	dir := t.TempDir()
	good := gitEnv("red@sukko.dev", "red@sukko.dev")
	runGit(t, dir, good, "init", "-q", "-b", "main")
	commitFile(t, dir, good, "base.txt", "base\n", "chore: base")
	runGit(t, dir, good, "tag", "base")
	// First added line "++x" renders as "+++x" — must not reset path state
	// and hide the marker on the next line.
	commitFile(t, dir, good, "notes.txt", "++x\nbuilt with claude.ai/code\n", "docs: plus plus line")

	assertContentMarkers(t, dir, "base..HEAD", 1)
}

func TestLoadCommitsSkiplistSpoof(t *testing.T) {
	dir := t.TempDir()
	good := gitEnv("red@sukko.dev", "red@sukko.dev")
	runGit(t, dir, good, "init", "-q", "-b", "main")
	commitFile(t, dir, good, "base.txt", "base\n", "chore: base")
	runGit(t, dir, good, "tag", "base")
	// Content line "++ b/scripts/check-provenance/x" renders as
	// "+++ b/scripts/check-provenance/x" — must not steer the path into the
	// skiplist and exempt the rest of the file.
	commitFile(t, dir, good, "notes.txt",
		"++ b/scripts/check-provenance/x\nbuilt with claude.ai/code\n", "docs: spoof attempt")

	assertContentMarkers(t, dir, "base..HEAD", 1)
}

func assertContentMarkers(t *testing.T, dir, revRange string, want int) {
	t.Helper()
	commits, err := loadCommits(dir, revRange)
	if err != nil {
		t.Fatalf("loadCommits: %v", err)
	}
	cfg := testConfig()
	got := 0
	for _, c := range commits {
		for _, f := range checkCommit(c, cfg) {
			if f.Kind == "content-marker" {
				got++
			} else {
				t.Errorf("unexpected finding: %v", f)
			}
		}
	}
	if got != want {
		t.Errorf("got %d content-marker findings, want %d", got, want)
	}
}

// git C-quotes paths containing '"', '\' or control chars REGARDLESS of
// core.quotePath — those headers render as `+++ "b/..."` and must still be
// scanned (scan-by-default: unrecognized header shapes must never drop lines).
func TestLoadCommitsQuoteInPath(t *testing.T) {
	dir := t.TempDir()
	good := gitEnv("red@sukko.dev", "red@sukko.dev")
	runGit(t, dir, good, "init", "-q", "-b", "main")
	commitFile(t, dir, good, "base.txt", "base\n", "chore: base")
	runGit(t, dir, good, "tag", "base")
	commitFile(t, dir, good, `qu"ote.txt`, "built with claude.ai/code\n", "docs: quoted path")

	assertContentMarkers(t, dir, "base..HEAD", 1)
}

func TestLoadCommitsControlCharInPath(t *testing.T) {
	dir := t.TempDir()
	good := gitEnv("red@sukko.dev", "red@sukko.dev")
	runGit(t, dir, good, "init", "-q", "-b", "main")
	commitFile(t, dir, good, "base.txt", "base\n", "chore: base")
	runGit(t, dir, good, "tag", "base")
	commitFile(t, dir, good, "ctrl\tname.txt", "built with claude.ai/code\n", "docs: control char path")

	assertContentMarkers(t, dir, "base..HEAD", 1)
}

func TestLoadCommitsRecordForgeryResistant(t *testing.T) {
	dir := t.TempDir()
	bad := gitEnv("evil@example.com", "evil@example.com")
	runGit(t, dir, bad, "init", "-q", "-b", "main")
	// Message embeds control chars shaped like a spare well-formed record
	// (old \x1e record sep + \x1f field seps). Records are NUL-terminated by
	// `git log -z`, and NUL cannot appear in a message — so this must stay ONE
	// commit with its real (bad) identity, not split into a phantom clean one.
	forged := "feat: x\x1eface\x1fa@sukko.dev\x1fa@sukko.dev\x1fp1 p2\x1fclean"
	commitFile(t, dir, bad, "a.txt", "x\n", forged)

	commits, err := loadCommits(dir, "HEAD~0") // full history: the one commit
	if err != nil {
		t.Fatalf("loadCommits: %v", err)
	}
	if len(commits) != 1 {
		t.Fatalf("got %d commits, want 1 (record boundary forged?)", len(commits))
	}
	cfg := testConfig()
	findings := checkCommit(commits[0], cfg)
	kinds := map[string]bool{}
	for _, f := range findings {
		kinds[f.Kind] = true
	}
	if !kinds["author-email"] || !kinds["committer-email"] {
		t.Errorf("forged message hid identity violations; findings: %v", findings)
	}
}

func TestLoadCommitsEmptyRange(t *testing.T) {
	dir := t.TempDir()
	good := gitEnv("red@sukko.dev", "red@sukko.dev")
	runGit(t, dir, good, "init", "-q", "-b", "main")
	commitFile(t, dir, good, "a.txt", "x\n", "chore: a")

	commits, err := loadCommits(dir, "HEAD..HEAD")
	if err != nil {
		t.Fatalf("loadCommits on empty range: %v", err)
	}
	if len(commits) != 0 {
		t.Errorf("got %d commits, want 0", len(commits))
	}
}

func TestLoadCommitsMerge(t *testing.T) {
	dir := t.TempDir()
	good := gitEnv("red@sukko.dev", "red@sukko.dev")
	ghMerge := gitEnv("red@sukko.dev", "noreply@github.com")

	runGit(t, dir, good, "init", "-q", "-b", "main")
	commitFile(t, dir, good, "base.txt", "base\n", "chore: base")
	runGit(t, dir, good, "tag", "base")
	runGit(t, dir, good, "checkout", "-q", "-b", "feature")
	commitFile(t, dir, good, "f.txt", "feature\n", "feat: branch work")
	runGit(t, dir, good, "checkout", "-q", "main")
	commitFile(t, dir, good, "m.txt", "main\n", "chore: mainline")
	runGit(t, dir, ghMerge, "merge", "--no-ff", "-m", "Merge pull request #1", "feature")

	commits, err := loadCommits(dir, "base..HEAD")
	if err != nil {
		t.Fatalf("loadCommits: %v", err)
	}
	var merge *Commit
	for i := range commits {
		if commits[i].IsMerge {
			merge = &commits[i]
		}
	}
	if merge == nil {
		t.Fatal("no merge commit found in range")
	}
	if len(merge.Added) != 0 {
		t.Errorf("merge commit content scanned; Added = %v, want empty", merge.Added)
	}
	cfg := testConfig()
	if f := checkCommit(*merge, cfg); len(f) != 0 {
		t.Errorf("clean GitHub-style merge produced findings: %v", f)
	}
}
