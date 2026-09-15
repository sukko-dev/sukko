package main

import (
	_ "embed"
	"fmt"
	"path"
	"strings"
)

//go:embed forbidden-paths.txt
var defaultForbidden string

// forbiddenRule is one parsed rule from forbidden-paths.txt. The three shapes
// are mutually exclusive, so a rule is described by its value plus which shape
// matched — see forbidden-paths.txt for the user-facing definition.
type forbiddenRule struct {
	raw      string // original line, reported verbatim so the error names the rule
	value    string // lowercased, with the "/" or "*" marker stripped
	isDir    bool
	isPrefix bool
}

// parseForbidden turns the rule file contents into rules. Comments and blank
// lines are dropped. Rules are lowercased once here so matching never lowers
// per-candidate-per-rule.
func parseForbidden(raw string) []forbiddenRule {
	var rules []forbiddenRule
	for line := range strings.Lines(raw) {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		r := forbiddenRule{raw: line}
		switch {
		case strings.HasSuffix(line, "/"):
			r.isDir = true
			r.value = strings.ToLower(strings.TrimSuffix(line, "/"))
		case strings.HasSuffix(line, "*"):
			r.isPrefix = true
			r.value = strings.ToLower(strings.TrimSuffix(line, "*"))
		default:
			r.value = strings.ToLower(line)
		}
		rules = append(rules, r)
	}
	return rules
}

// matchForbidden returns the raw rule that p violates, or "" when p is allowed.
//
// Directory rules are checked against every path segment rather than only the
// leading one: a config directory nested inside a subtree leaks exactly as much
// as one at the root, and nesting is the likelier accident (a tool run from a
// subdirectory). File rules are checked against the basename for the same
// reason.
func matchForbidden(p string, rules []forbiddenRule) string {
	p = strings.ToLower(path.Clean(strings.ReplaceAll(p, "\\", "/")))
	base := path.Base(p)
	segments := strings.Split(p, "/")

	for _, r := range rules {
		switch {
		case r.isDir:
			for _, seg := range segments[:max(len(segments)-1, 0)] {
				if seg == r.value {
					return r.raw
				}
			}
		case r.isPrefix:
			if strings.HasPrefix(base, r.value) {
				return r.raw
			}
		default:
			if base == r.value {
				return r.raw
			}
		}
	}
	return ""
}

// checkPaths reports one finding per tracked path that violates a rule. Every
// violation is reported rather than stopping at the first, so a single run
// tells the committer everything to remove.
func checkPaths(paths []string, rules []forbiddenRule) []Finding {
	var findings []Finding
	for _, p := range paths {
		if rule := matchForbidden(p, rules); rule != "" {
			findings = append(findings, Finding{"tree", "forbidden-path",
				fmt.Sprintf("%s is tracked but matches forbidden rule %q", p, rule)})
		}
	}
	return findings
}

// loadTrackedPaths lists every file tracked at the working tree of dir.
//
// The check is deliberately on the tracked tree rather than on the commit range:
// a forbidden path that arrived through a merge, a rebase, or a range this run
// does not cover is still tracked, and still a leak. The tree is the property
// worth asserting.
func loadTrackedPaths(dir string) ([]string, error) {
	out, err := gitOutput(dir, "-c", "core.quotePath=false", "ls-files", "-z")
	if err != nil {
		return nil, fmt.Errorf("git ls-files: %w", err)
	}
	var paths []string
	for _, p := range strings.Split(out, "\x00") {
		if p != "" {
			paths = append(paths, p)
		}
	}
	return paths, nil
}
