package main

import "testing"

func TestMatchForbidden(t *testing.T) {
	rules := parseForbidden(defaultForbidden)

	tests := []struct {
		name string
		path string
		want bool
	}{
		// Directory rules match at any depth — a nested assistant config is the
		// same leak as a top-level one.
		{"assistant dir at root", ".claude/settings.json", true},
		{"assistant dir nested", "ws/internal/.claude/agents/x.md", true},
		{"other vendor dir", ".cursor/rules/go.md", true},
		{"continue dir", ".continue/config.json", true},

		// File rules match the basename at any depth.
		{"instruction file at root", "CLAUDE.md", true},
		{"instruction file nested", "ws/CLAUDE.md", true},
		{"agents file", "AGENTS.md", true},
		{"gemini file", "docs/GEMINI.md", true},
		{"copilot instructions", ".github/copilot-instructions.md", true},

		// Prefix rules (trailing *) match a basename prefix.
		{"aider prefix", ".aider.conf.yml", true},
		{"aider bare", ".aider", true},

		// Must NOT fire on ordinary project files — a false positive here blocks
		// legitimate work, so these are the cases that keep the rule honest.
		{"ordinary go file", "ws/internal/server/shard.go", false},
		{"docs page", "docs/adr/0014-routing-rules-move-to-community.md", false},
		{"readme", "README.md", false},
		{"substring of a rule is not a match", "ws/internal/agents.go", false},
		{"claude-like name is not the rule", "docs/claude-compat.md", false},
		{"github dir itself is fine", ".github/workflows/ci.yml", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchForbidden(tt.path, rules)
			if (got != "") != tt.want {
				t.Errorf("matchForbidden(%q) = %q, want match=%v", tt.path, got, tt.want)
			}
		})
	}
}

func TestParseForbiddenRejectsEmpty(t *testing.T) {
	if rules := parseForbidden("# only comments\n\n"); len(rules) != 0 {
		t.Errorf("got %d rules from a comment-only file, want 0", len(rules))
	}
}

// The embedded rule file must actually carry rules — an empty embed would make
// the whole check silently pass, which is the one failure mode that matters.
func TestDefaultForbiddenIsPopulated(t *testing.T) {
	if rules := parseForbidden(defaultForbidden); len(rules) < 5 {
		t.Errorf("embedded forbidden-paths.txt yielded %d rules, want >= 5", len(rules))
	}
}

func TestCheckPathsReportsEveryViolation(t *testing.T) {
	rules := parseForbidden(defaultForbidden)
	paths := []string{
		"README.md",
		"CLAUDE.md",
		"ws/go.mod",
		".claude/agents/x.md",
	}
	findings := checkPaths(paths, rules)
	if len(findings) != 2 {
		t.Fatalf("got %d findings, want 2: %v", len(findings), findings)
	}
	for _, f := range findings {
		if f.Kind != "forbidden-path" {
			t.Errorf("finding kind = %q, want forbidden-path", f.Kind)
		}
	}
}
