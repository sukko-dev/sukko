package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func parseSource(t *testing.T, filename, src string) (*token.FileSet, *ast.File) {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filename, src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	return fset, f
}

func TestCheckFile_MissingInlineComment(t *testing.T) {
	t.Parallel()
	fset, f := parseSource(t, "cfg.go", "package p\ntype Cfg struct {\n\tPort int `env:\"PORT\"`\n}")
	issues := checkFile(fset, f)
	if len(issues) != 1 {
		t.Fatalf("expected 1 issue, got %d: %v", len(issues), issues)
	}
}

func TestCheckFile_HasInlineComment(t *testing.T) {
	t.Parallel()
	fset, f := parseSource(t, "cfg.go", "package p\ntype Cfg struct {\n\tPort int `env:\"PORT\"` // HTTP port\n}")
	issues := checkFile(fset, f)
	if len(issues) != 0 {
		t.Fatalf("expected 0 issues, got %d: %v", len(issues), issues)
	}
}

func TestCheckFile_SkipsEnvDash(t *testing.T) {
	t.Parallel()
	fset, f := parseSource(t, "cfg.go", "package p\ntype Cfg struct {\n\tInternal string `env:\"-\"`\n}")
	issues := checkFile(fset, f)
	if len(issues) != 0 {
		t.Fatalf("expected 0 issues for env:\"-\", got %d: %v", len(issues), issues)
	}
}

func TestCheckFile_SkipsNoEnvTag(t *testing.T) {
	t.Parallel()
	fset, f := parseSource(t, "cfg.go", "package p\ntype Cfg struct {\n\tPort int\n}")
	issues := checkFile(fset, f)
	if len(issues) != 0 {
		t.Fatalf("expected 0 issues for no env tag, got %d: %v", len(issues), issues)
	}
}

func TestCheckFile_SkipsTestFile(t *testing.T) {
	t.Parallel()
	fset, f := parseSource(t, "cfg_test.go", "package p\ntype Cfg struct {\n\tPort int `env:\"PORT\"`\n}")
	issues := checkFile(fset, f)
	if len(issues) != 0 {
		t.Fatalf("expected 0 issues for _test.go file, got %d: %v", len(issues), issues)
	}
}

func TestCheckFile_SkipsGenFile(t *testing.T) {
	t.Parallel()
	fset, f := parseSource(t, "cfg_gen.go", "package p\ntype Cfg struct {\n\tPort int `env:\"PORT\"`\n}")
	issues := checkFile(fset, f)
	if len(issues) != 0 {
		t.Fatalf("expected 0 issues for _gen.go file, got %d: %v", len(issues), issues)
	}
}

func TestCheckFile_SkipsProtoFile(t *testing.T) {
	t.Parallel()
	fset, f := parseSource(t, "cfg.pb.go", "package p\ntype Cfg struct {\n\tPort int `env:\"PORT\"`\n}")
	issues := checkFile(fset, f)
	if len(issues) != 0 {
		t.Fatalf("expected 0 issues for .pb.go file, got %d: %v", len(issues), issues)
	}
}

func TestCheckFile_MultipleFields_MixedComments(t *testing.T) {
	t.Parallel()
	src := "package p\ntype Cfg struct {\n\tPort int `env:\"PORT\"` // HTTP port\n\tHost string `env:\"HOST\"`\n}"
	fset, f := parseSource(t, "cfg.go", src)
	issues := checkFile(fset, f)
	if len(issues) != 1 {
		t.Fatalf("expected 1 issue (Host missing, Port present), got %d: %v", len(issues), issues)
	}
}

func TestCheckFile_IssueContainsEnvNameAndFieldName(t *testing.T) {
	t.Parallel()
	fset, f := parseSource(t, "cfg.go", "package p\ntype Cfg struct {\n\tMyPort int `env:\"MY_PORT\"`\n}")
	issues := checkFile(fset, f)
	if len(issues) != 1 {
		t.Fatalf("expected 1 issue, got %d", len(issues))
	}
	issue := issues[0]
	for _, want := range []string{"MY_PORT", "MyPort", "cfg.go"} {
		if !strings.Contains(issue, want) {
			t.Errorf("issue %q missing expected substring %q", issue, want)
		}
	}
}

func TestWalkTarget_ParseErrorContinues(t *testing.T) {
	t.Parallel()
	// A parse error in one file must not abort the walk — issues from valid sibling files
	// must still be reported, and failed must be true.
	dir := t.TempDir()

	// valid.go: valid Go, but missing an inline comment → produces 1 issue
	if err := os.WriteFile(filepath.Join(dir, "valid.go"),
		[]byte("package p\ntype Cfg struct {\n\tPort int `env:\"PORT\"`\n}"), 0o644); err != nil {
		t.Fatal(err)
	}
	// aaa_invalid.go: lexically before valid.go, syntactically broken → causes parse error
	if err := os.WriteFile(filepath.Join(dir, "aaa_invalid.go"),
		[]byte("package p\n{ this is not valid go }"), 0o644); err != nil {
		t.Fatal(err)
	}

	fset := token.NewFileSet()
	var errBuf strings.Builder
	issues, failed := walkTarget(fset, dir, &errBuf)

	if !failed {
		t.Fatal("expected failed=true due to parse error in aaa_invalid.go")
	}
	if len(issues) != 1 {
		t.Fatalf("expected 1 issue from valid.go after parse error in aaa_invalid.go, got %d: %v", len(issues), issues)
	}
	if !strings.Contains(errBuf.String(), "aaa_invalid.go") {
		t.Errorf("expected parse error for aaa_invalid.go logged to errOut, got: %q", errBuf.String())
	}
}

func TestCheckFile_EmbeddedFieldWithEnvTag(t *testing.T) {
	t.Parallel()
	// Anonymous embedded fields with env tags produce <embedded> as the field name.
	src := "package p\ntype Inner struct{}\ntype Outer struct {\n\tInner `env:\"X\"`\n}"
	fset, f := parseSource(t, "cfg.go", src)
	issues := checkFile(fset, f)
	if len(issues) != 1 {
		t.Fatalf("expected 1 issue for embedded field missing inline comment, got %d: %v", len(issues), issues)
	}
	if !strings.Contains(issues[0], "<embedded>") {
		t.Errorf("issue for embedded field should contain '<embedded>', got: %q", issues[0])
	}
	if !strings.Contains(issues[0], `"X"`) {
		t.Errorf("issue for embedded field should contain the env name, got: %q", issues[0])
	}
}
