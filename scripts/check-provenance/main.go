// Command check-provenance enforces commit provenance over a git revision
// range: every commit's author and committer email must belong to the company
// domain, and neither commit messages nor added diff lines may contain
// AI-attribution markers (see ai-markers.txt). Over the tracked tree: no file
// may match a forbidden path rule (see forbidden-paths.txt).
//
// Exemptions:
//   - Committer emails in --extra-committers (default noreply@github.com) are
//     allowed on any commit — GitHub's merge/squash machinery sets itself as
//     committer while preserving authorship, and authorship is what we enforce.
//   - Merge commits are exempt from the content scan; identity and message are
//     still checked. Accepted risk: an evil merge (conflict resolution
//     introducing lines that exist on no parent) is not content-scanned — the
//     threat model here is accident prevention, and every non-merge path to the
//     same content is scanned.
//   - Paths under --skip prefixes are exempt from the content scan (the checker
//     itself and the git hooks legitimately contain marker strings).
//
// Usage:
//
//	check-provenance --range origin/main..HEAD [--domain sukko.dev]
//	    [--extra-committers a@b,c@d] [--skip path1/,path2/] [--repo dir]
package main

import (
	"bufio"
	"bytes"
	_ "embed"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
)

//go:embed ai-markers.txt
var defaultMarkers string

const (
	defaultDomain          = "sukko.dev"
	defaultExtraCommitters = "noreply@github.com"
	// ws/go.sum is machine-generated base64 where uppercase T\d{3} shapes can
	// occur by chance in hashes on dependency bumps.
	defaultSkipPaths = "scripts/check-provenance/,scripts/check-publish/,.githooks/,ws/go.sum"

	// git log field separator (ASCII US). Records are NUL-terminated via
	// `git log -z` — NUL cannot appear in a commit message (git rejects it),
	// so record boundaries cannot be forged by a crafted message. A stray
	// fieldSep inside a message lands wholly in the final SplitN field.
	fieldSep = "\x1f"
)

// Commit is one commit's provenance-relevant data.
type Commit struct {
	Hash           string
	AuthorEmail    string
	CommitterEmail string
	Message        string
	IsMerge        bool
	Added          []AddedLine
}

// AddedLine is a single "+" line from a commit's diff.
type AddedLine struct {
	Path string
	Text string
}

// markerSet holds the compiled deny-list: plain lowercased substrings and
// regexes (from `re:`-prefixed lines, compiled case-insensitive).
type markerSet struct {
	substrings []string
	regexps    []*regexp.Regexp
}

// match returns the first marker that text violates, or "" if clean.
// Takes RAW text: substrings match case-insensitively via lowering here;
// regexps see the original text so patterns own their case semantics
// (e.g. `(?-i:...)` for uppercase-only retired-ID families).
func (m markerSet) match(text string) string {
	lower := strings.ToLower(text)
	for _, s := range m.substrings {
		if strings.Contains(lower, s) {
			return s
		}
	}
	for _, r := range m.regexps {
		if r.MatchString(text) {
			return r.String()
		}
	}
	return ""
}

// Config holds the enforcement policy.
type Config struct {
	Domain          string
	ExtraCommitters []string
	SkipPaths       []string
	Markers         markerSet
}

// Finding is one policy violation.
type Finding struct {
	Hash   string
	Kind   string // author-email | committer-email | message-marker | content-marker
	Detail string
}

func (f Finding) String() string {
	return fmt.Sprintf("%s: %s: %s", f.Hash, f.Kind, f.Detail)
}

// emailAllowed reports whether email's domain part is exactly domain
// (case-insensitive, anchored — "red@notsukko.dev" must not match "sukko.dev").
func emailAllowed(email, domain string) bool {
	e := strings.ToLower(email)
	suffix := "@" + strings.ToLower(domain)
	return strings.HasSuffix(e, suffix) && len(e) > len(suffix)
}

// parseMarkers turns the marker file contents into a markerSet: plain lines
// become lowercased substring matches; `re:`-prefixed lines compile to
// case-insensitive regexes. Comments and blank lines are dropped. An invalid
// regex panics — the deny-list is compiled in and must fail closed at startup,
// not silently skip a pattern.
func parseMarkers(raw string) markerSet {
	var m markerSet
	for line := range strings.Lines(raw) {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if expr, ok := strings.CutPrefix(line, "re:"); ok {
			m.regexps = append(m.regexps, regexp.MustCompile("(?i)"+expr))
			continue
		}
		m.substrings = append(m.substrings, strings.ToLower(line))
	}
	return m
}

// checkCommit applies the full policy to one commit.
func checkCommit(c Commit, cfg Config) []Finding {
	var findings []Finding

	if !emailAllowed(c.AuthorEmail, cfg.Domain) {
		findings = append(findings, Finding{c.Hash, "author-email",
			fmt.Sprintf("author %q is not @%s", c.AuthorEmail, cfg.Domain)})
	}
	committerOK := emailAllowed(c.CommitterEmail, cfg.Domain)
	for _, extra := range cfg.ExtraCommitters {
		if strings.EqualFold(c.CommitterEmail, extra) {
			committerOK = true
			break
		}
	}
	if !committerOK {
		findings = append(findings, Finding{c.Hash, "committer-email",
			fmt.Sprintf("committer %q is not @%s", c.CommitterEmail, cfg.Domain)})
	}

	if m := cfg.Markers.match(c.Message); m != "" {
		findings = append(findings, Finding{c.Hash, "message-marker",
			fmt.Sprintf("commit message contains %q", m)})
	}

	for _, line := range c.Added {
		if skipPath(line.Path, cfg.SkipPaths) {
			continue
		}
		if m := cfg.Markers.match(line.Text); m != "" {
			findings = append(findings, Finding{c.Hash, "content-marker",
				fmt.Sprintf("%s: added line contains %q", line.Path, m)})
		}
	}
	return findings
}

// skipPath reports whether path falls under any skip prefix.
func skipPath(path string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(path, p) {
			return true
		}
	}
	return false
}

// loadCommits reads all commits in revRange from the repository at dir,
// including added diff lines for non-merge commits.
func loadCommits(dir, revRange string) ([]Commit, error) {
	// %x1f is git's own escape — literal control bytes in an argv argument
	// make exec fail with EINVAL. -z NUL-terminates each record.
	format := "%H%x1f%ae%x1f%ce%x1f%P%x1f%B"
	out, err := gitOutput(dir, "log", "-z", "--format="+format, revRange)
	if err != nil {
		return nil, fmt.Errorf("git log %s: %w", revRange, err)
	}

	var commits []Commit
	for _, record := range strings.Split(out, "\x00") {
		if record == "" {
			continue
		}
		fields := strings.SplitN(record, fieldSep, 5)
		if len(fields) != 5 {
			return nil, fmt.Errorf("malformed git log record: %q", record)
		}
		c := Commit{
			Hash:           fields[0],
			AuthorEmail:    fields[1],
			CommitterEmail: fields[2],
			IsMerge:        len(strings.Fields(fields[3])) > 1,
			Message:        strings.TrimRight(fields[4], "\n"),
		}
		if !c.IsMerge {
			added, err := loadAddedLines(dir, c.Hash)
			if err != nil {
				return nil, fmt.Errorf("diff for %s: %w", c.Hash, err)
			}
			c.Added = added
		}
		commits = append(commits, c)
	}
	return commits, nil
}

// loadAddedLines extracts the "+" lines (with their file paths) from one
// commit's diff against its parent.
//
// Header lines cannot be told from content by prefix alone: an added content
// line starting with "++" renders as "+++...". Headers ("+++ b/…") occur only
// BETWEEN a "diff --git " line and that file's first "@@" hunk marker, so we
// track hunk state and treat "+" lines as content only inside a hunk. Inside a
// hunk every line carries a +/-/\ marker, so bare "diff --git "/"@@" prefixes
// there are unambiguous. core.quotePath=false keeps non-ASCII paths literal
// instead of quoted-and-escaped (which would dodge the "+++ b/" match and
// silently drop the file from the scan).
func loadAddedLines(dir, hash string) ([]AddedLine, error) {
	out, err := gitOutput(dir, "-c", "core.quotePath=false",
		"show", hash, "--format=", "--unified=0", "--no-color")
	if err != nil {
		return nil, err
	}
	var added []AddedLine
	path := ""
	inHunk := false
	sc := bufio.NewScanner(strings.NewReader(out))
	sc.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "diff --git "):
			inHunk = false
			path = ""
		case !inHunk && strings.HasPrefix(line, "@@"):
			inHunk = true
		case !inHunk && line == "+++ /dev/null":
			// Deletion — no added lines will follow for it. Exact match: content
			// cannot spoof this arm (content lines only exist inside hunks).
			path = ""
		case !inHunk && strings.HasPrefix(line, "+++ b/"):
			// Trailing tab is git's whitespace protection on space-containing
			// filenames — strip it so skip prefixes and reports see the real path.
			path = strings.TrimSuffix(strings.TrimPrefix(line, "+++ b/"), "\t")
		case !inHunk && strings.HasPrefix(line, "+++ "):
			// Any other header shape — notably C-quoted paths (`+++ "b/..."`),
			// which git emits for '"', '\' and control chars REGARDLESS of
			// core.quotePath. Scan-by-default: keep the raw remainder as the
			// path. It starts with '"', so it can never match a skip prefix —
			// fail-closed by construction.
			path = strings.TrimPrefix(line, "+++ ")
		case inHunk && strings.HasPrefix(line, "@@"):
			// Next hunk of the same file.
		case inHunk && strings.HasPrefix(line, "+") && path != "":
			added = append(added, AddedLine{Path: path, Text: line[1:]})
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("scanning diff: %w", err)
	}
	return added, nil
}

func gitOutput(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

func splitList(s string) []string {
	var out []string
	for part := range strings.SplitSeq(s, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func main() {
	revRange := flag.String("range", "", "git revision range to check (e.g. origin/main..HEAD)")
	domain := flag.String("domain", defaultDomain, "required email domain for authors and committers")
	extraCommitters := flag.String("extra-committers", defaultExtraCommitters,
		"comma-separated committer emails additionally allowed (merge machinery)")
	skip := flag.String("skip", defaultSkipPaths, "comma-separated path prefixes exempt from the content scan")
	repo := flag.String("repo", ".", "repository directory")
	flag.Parse()

	if *revRange == "" {
		fmt.Fprintln(os.Stderr, "check-provenance: --range is required")
		os.Exit(2)
	}

	cfg := Config{
		Domain:          *domain,
		ExtraCommitters: splitList(*extraCommitters),
		SkipPaths:       splitList(*skip),
		Markers:         parseMarkers(defaultMarkers),
	}

	commits, err := loadCommits(*repo, *revRange)
	if err != nil {
		fmt.Fprintf(os.Stderr, "check-provenance: %v\n", err)
		os.Exit(2)
	}

	var findings []Finding
	for _, c := range commits {
		findings = append(findings, checkCommit(c, cfg)...)
	}

	// The tracked tree is checked once, independently of the range: a forbidden
	// path can arrive through a merge or a range this run does not cover, and it
	// is a violation for as long as it stays tracked.
	paths, err := loadTrackedPaths(*repo)
	if err != nil {
		fmt.Fprintf(os.Stderr, "check-provenance: %v\n", err)
		os.Exit(2)
	}
	findings = append(findings, checkPaths(paths, parseForbidden(defaultForbidden))...)

	if len(findings) > 0 {
		fmt.Fprintf(os.Stderr, "check-provenance: %d violation(s) in %d commit(s) (%s):\n",
			len(findings), len(commits), *revRange)
		for _, f := range findings {
			fmt.Fprintf(os.Stderr, "  %s\n", f)
		}
		os.Exit(1)
	}
	fmt.Printf("check-provenance: %d commit(s) clean (%s)\n", len(commits), *revRange)
}
