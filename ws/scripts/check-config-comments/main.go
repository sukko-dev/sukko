// check-config-comments verifies that every env-tagged struct field in the
// specified Go files/directories carries an inline comment (field.Comment).
// A missing inline comment means the docs pipeline will produce an empty
// description for that env var. Exits non-zero with a list of offending fields.
//
// Usage:
//
//	go run ./scripts/check-config-comments [path...]
//
// When no paths are given, defaults to internal/shared/platform/ and cmd/tester/.
package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

func main() {
	dirs := os.Args[1:]
	if len(dirs) == 0 {
		dirs = []string{
			"internal/shared/platform/",
			"cmd/tester/",
		}
	}

	fset := token.NewFileSet()
	var issues []string
	failed := false

	for _, target := range dirs {
		got, err := walkTarget(fset, target, os.Stderr)
		issues = append(issues, got...)
		if err {
			failed = true
		}
	}

	for _, issue := range issues {
		fmt.Fprintln(os.Stderr, issue)
	}
	if len(issues) > 0 || failed {
		os.Exit(1)
	}
}

// walkTarget walks a directory tree, parses Go files, and collects missing-inline-comment issues.
// Parse errors are logged to errOut and do not abort the walk — remaining files are still checked.
// Returns all issues found and whether any parse or filesystem error occurred.
func walkTarget(fset *token.FileSet, target string, errOut io.Writer) (issues []string, failed bool) {
	err := filepath.WalkDir(target, func(path string, d os.DirEntry, err error) error { //nolint:gosec // G703: target comes from os.Args — developer-supplied codebase paths; path traversal is not a threat for a developer pre-commit tool.
		if err != nil {
			return err // filesystem error (permission denied, not found) — stop this target
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			_, _ = fmt.Fprintf(errOut, "check-config-comments: parse %s: %v\n", path, err)
			failed = true
			return nil // continue walking; parse errors don't abort the directory
		}
		issues = append(issues, checkFile(fset, f)...)
		return nil
	})
	if err != nil {
		_, _ = fmt.Fprintf(errOut, "check-config-comments: %v\n", err)
		failed = true
	}
	return
}

func checkFile(fset *token.FileSet, f *ast.File) []string {
	filename := fset.File(f.Pos()).Name()
	// skip generated files
	if strings.HasSuffix(filename, "_gen.go") || strings.HasSuffix(filename, ".pb.go") {
		return nil
	}
	// skip test files
	if strings.HasSuffix(filename, "_test.go") {
		return nil
	}

	var issues []string
	ast.Inspect(f, func(n ast.Node) bool {
		st, ok := n.(*ast.StructType)
		if !ok {
			return true
		}
		for _, field := range st.Fields.List {
			if field.Tag == nil {
				continue
			}
			tagStr := strings.Trim(field.Tag.Value, "`")
			envName := reflect.StructTag(tagStr).Get("env")
			if envName == "" || envName == "-" {
				continue
			}
			if field.Comment != nil {
				continue
			}
			pos := fset.Position(field.Pos())
			fieldName := "<embedded>"
			if len(field.Names) > 0 {
				fieldName = field.Names[0].Name
			}
			issues = append(issues, fmt.Sprintf(
				"%s:%d: %s (env:%q) missing inline comment",
				pos.Filename, pos.Line, fieldName, envName,
			))
		}
		return true
	})
	return issues
}
