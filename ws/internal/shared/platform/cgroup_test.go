package platform

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// =============================================================================
// GetMemoryLimit Tests
// =============================================================================

// GetMemoryLimit is an integration-style test that verifies the real function
// works on the current system. The function reads from system cgroup paths
// (/sys/fs/cgroup/...) which cannot be mocked without filesystem injection.
//
// Comprehensive unit tests for the parsing logic are in parseMemoryLimit* and
// readMemoryLimitFromPath* test functions below, which use mock data and temp files.

func TestGetMemoryLimit_ReturnsValidResult(t *testing.T) {
	t.Parallel()
	// Integration test: verify GetMemoryLimit works on current system
	limit, err := GetMemoryLimit()

	// On most dev machines (non-containerized), this returns 0, nil
	// In containers, it returns the actual limit
	if err != nil {
		t.Errorf("GetMemoryLimit returned unexpected error: %v", err)
	}

	// Limit should never be negative
	if limit < 0 {
		t.Errorf("GetMemoryLimit returned negative value: %d", limit)
	}
}

// =============================================================================
// Memory Limit Parsing Helper Tests (unit testable)
// =============================================================================

// parseMemoryLimit is a testable helper that mimics GetMemoryLimit logic
func parseMemoryLimit(v2Data, v1Data string, v2Exists, v1Exists bool) (int64, error) {
	// Try cgroup v2 first
	if v2Exists && v2Data != "" {
		limitStr := strings.TrimSpace(v2Data)
		if limitStr != "max" {
			v, err := strconv.ParseInt(limitStr, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("parse cgroup v2 memory limit: %w", err)
			}
			return v, nil
		}
		// "max" means unlimited
		return 0, nil
	}

	// Fallback to cgroup v1
	if v1Exists && v1Data != "" {
		limitStr := strings.TrimSpace(v1Data)
		v, err := strconv.ParseInt(limitStr, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse cgroup v1 memory limit: %w", err)
		}
		return v, nil
	}

	// No cgroup limits found
	return 0, nil
}

func TestParseMemoryLimit_V2_WithLimit(t *testing.T) {
	t.Parallel()
	// 512MB = 536870912 bytes
	limit, err := parseMemoryLimit("536870912\n", "", true, false)

	if err != nil {
		t.Fatalf("parseMemoryLimit failed: %v", err)
	}
	if limit != 536870912 {
		t.Errorf("limit: got %d, want 536870912", limit)
	}
}

func TestParseMemoryLimit_V2_Max(t *testing.T) {
	t.Parallel()
	// "max" means unlimited
	limit, err := parseMemoryLimit("max\n", "", true, false)

	if err != nil {
		t.Fatalf("parseMemoryLimit failed: %v", err)
	}
	if limit != 0 {
		t.Errorf("limit: got %d, want 0 (unlimited)", limit)
	}
}

func TestParseMemoryLimit_V2_LargeValue(t *testing.T) {
	t.Parallel()
	// 16GB
	limit, err := parseMemoryLimit("17179869184\n", "", true, false)

	if err != nil {
		t.Fatalf("parseMemoryLimit failed: %v", err)
	}
	if limit != 17179869184 {
		t.Errorf("limit: got %d, want 17179869184", limit)
	}
}

func TestParseMemoryLimit_V1_WithLimit(t *testing.T) {
	t.Parallel()
	// 1GB = 1073741824 bytes
	limit, err := parseMemoryLimit("", "1073741824\n", false, true)

	if err != nil {
		t.Fatalf("parseMemoryLimit failed: %v", err)
	}
	if limit != 1073741824 {
		t.Errorf("limit: got %d, want 1073741824", limit)
	}
}

func TestParseMemoryLimit_V1_VeryLargeValue(t *testing.T) {
	t.Parallel()
	// This represents "unlimited" on some v1 systems
	// 9223372036854771712 is close to int64 max
	limit, err := parseMemoryLimit("", "9223372036854771712\n", false, true)

	if err != nil {
		t.Fatalf("parseMemoryLimit failed: %v", err)
	}
	if limit != 9223372036854771712 {
		t.Errorf("limit: got %d, want 9223372036854771712", limit)
	}
}

func TestParseMemoryLimit_V2_Preferred_Over_V1(t *testing.T) {
	t.Parallel()
	// When both exist, v2 should be preferred
	limit, err := parseMemoryLimit("536870912\n", "1073741824\n", true, true)

	if err != nil {
		t.Fatalf("parseMemoryLimit failed: %v", err)
	}
	// Should use v2 value
	if limit != 536870912 {
		t.Errorf("limit: got %d, want 536870912 (v2 value)", limit)
	}
}

func TestParseMemoryLimit_NoFiles(t *testing.T) {
	t.Parallel()
	limit, err := parseMemoryLimit("", "", false, false)

	if err != nil {
		t.Fatalf("parseMemoryLimit failed: %v", err)
	}
	if limit != 0 {
		t.Errorf("limit: got %d, want 0 (no cgroup)", limit)
	}
}

func TestParseMemoryLimit_InvalidV2Format(t *testing.T) {
	t.Parallel()
	_, err := parseMemoryLimit("invalid\n", "", true, false)

	if err == nil {
		t.Error("Expected error for invalid format")
	}
}

func TestParseMemoryLimit_InvalidV1Format(t *testing.T) {
	t.Parallel()
	_, err := parseMemoryLimit("", "not-a-number\n", false, true)

	if err == nil {
		t.Error("Expected error for invalid format")
	}
}

// =============================================================================
// File-based Tests (using temp files)
// =============================================================================

// readMemoryLimitFromPath reads memory limit from a specific file path
// This is a testable version of the file reading logic
func readMemoryLimitFromPath(path string, isV2 bool) (int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("read memory limit file: %w", err)
	}

	limitStr := strings.TrimSpace(string(data))

	if isV2 && limitStr == "max" {
		return 0, nil // unlimited
	}

	v, parseErr := strconv.ParseInt(limitStr, 10, 64)
	if parseErr != nil {
		return 0, fmt.Errorf("parse memory limit value: %w", parseErr)
	}
	return v, nil
}

func TestReadMemoryLimitFromPath_V2_WithLimit(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	memMaxPath := filepath.Join(tmpDir, "memory.max")

	// Write 256MB limit
	err := os.WriteFile(memMaxPath, []byte("268435456\n"), 0o600)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	limit, err := readMemoryLimitFromPath(memMaxPath, true)
	if err != nil {
		t.Fatalf("readMemoryLimitFromPath failed: %v", err)
	}

	if limit != 268435456 {
		t.Errorf("limit: got %d, want 268435456", limit)
	}
}

func TestReadMemoryLimitFromPath_V2_Max(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	memMaxPath := filepath.Join(tmpDir, "memory.max")

	err := os.WriteFile(memMaxPath, []byte("max\n"), 0o600)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	limit, err := readMemoryLimitFromPath(memMaxPath, true)
	if err != nil {
		t.Fatalf("readMemoryLimitFromPath failed: %v", err)
	}

	if limit != 0 {
		t.Errorf("limit: got %d, want 0 (unlimited)", limit)
	}
}

func TestReadMemoryLimitFromPath_V1(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	memLimitPath := filepath.Join(tmpDir, "memory.limit_in_bytes")

	// Write 2GB limit
	err := os.WriteFile(memLimitPath, []byte("2147483648\n"), 0o600)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	limit, err := readMemoryLimitFromPath(memLimitPath, false)
	if err != nil {
		t.Fatalf("readMemoryLimitFromPath failed: %v", err)
	}

	if limit != 2147483648 {
		t.Errorf("limit: got %d, want 2147483648", limit)
	}
}

func TestReadMemoryLimitFromPath_MissingFile(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	missingPath := filepath.Join(tmpDir, "nonexistent")

	_, err := readMemoryLimitFromPath(missingPath, true)
	if err == nil {
		t.Error("Expected error for missing file")
	}
}

func TestReadMemoryLimitFromPath_EmptyFile(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	emptyPath := filepath.Join(tmpDir, "empty")

	err := os.WriteFile(emptyPath, []byte(""), 0o600)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	_, err = readMemoryLimitFromPath(emptyPath, true)
	if err == nil {
		t.Error("Expected error for empty file")
	}
}

func TestReadMemoryLimitFromPath_WhitespaceOnly(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	wsPath := filepath.Join(tmpDir, "whitespace")

	err := os.WriteFile(wsPath, []byte("   \n  \t  \n"), 0o600)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	_, err = readMemoryLimitFromPath(wsPath, true)
	if err == nil {
		t.Error("Expected error for whitespace-only file")
	}
}

// =============================================================================
// Memory Size Tests
// =============================================================================

func TestMemorySizeConstants(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		sizeStr     string
		expectedMB  int64
		description string
	}{
		{
			name:        "256MB",
			sizeStr:     "268435456",
			expectedMB:  256,
			description: "256 * 1024 * 1024",
		},
		{
			name:        "512MB",
			sizeStr:     "536870912",
			expectedMB:  512,
			description: "512 * 1024 * 1024",
		},
		{
			name:        "1GB",
			sizeStr:     "1073741824",
			expectedMB:  1024,
			description: "1024 * 1024 * 1024",
		},
		{
			name:        "2GB",
			sizeStr:     "2147483648",
			expectedMB:  2048,
			description: "2048 * 1024 * 1024",
		},
		{
			name:        "4GB",
			sizeStr:     "4294967296",
			expectedMB:  4096,
			description: "4096 * 1024 * 1024",
		},
		{
			name:        "8GB",
			sizeStr:     "8589934592",
			expectedMB:  8192,
			description: "8192 * 1024 * 1024",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			bytes, err := strconv.ParseInt(tt.sizeStr, 10, 64)
			if err != nil {
				t.Fatalf("Failed to parse %s: %v", tt.sizeStr, err)
			}

			mb := bytes / (1024 * 1024)
			if mb != tt.expectedMB {
				t.Errorf("MB conversion: got %d, want %d", mb, tt.expectedMB)
			}
		})
	}
}

// =============================================================================
// Edge Case Tests
// =============================================================================

func TestParseMemoryLimit_LeadingTrailingWhitespace(t *testing.T) {
	t.Parallel()
	limit, err := parseMemoryLimit("  536870912  \n", "", true, false)

	if err != nil {
		t.Fatalf("parseMemoryLimit failed: %v", err)
	}
	if limit != 536870912 {
		t.Errorf("limit: got %d, want 536870912", limit)
	}
}

func TestParseMemoryLimit_V2_MaxWithWhitespace(t *testing.T) {
	t.Parallel()
	limit, err := parseMemoryLimit("  max  \n", "", true, false)

	if err != nil {
		t.Fatalf("parseMemoryLimit failed: %v", err)
	}
	if limit != 0 {
		t.Errorf("limit: got %d, want 0 (unlimited)", limit)
	}
}

func TestParseMemoryLimit_ZeroBytes(t *testing.T) {
	t.Parallel()
	// Zero is technically valid (though unusual)
	limit, err := parseMemoryLimit("0\n", "", true, false)

	if err != nil {
		t.Fatalf("parseMemoryLimit failed: %v", err)
	}
	if limit != 0 {
		t.Errorf("limit: got %d, want 0", limit)
	}
}

func TestParseMemoryLimit_MinimalSize(t *testing.T) {
	t.Parallel()
	// Very small limit (1 byte)
	limit, err := parseMemoryLimit("1\n", "", true, false)

	if err != nil {
		t.Fatalf("parseMemoryLimit failed: %v", err)
	}
	if limit != 1 {
		t.Errorf("limit: got %d, want 1", limit)
	}
}

// =============================================================================
// Benchmark Tests
// =============================================================================

func BenchmarkGetMemoryLimit(b *testing.B) {
	for b.Loop() {
		_, _ = GetMemoryLimit()
	}
}

func BenchmarkParseMemoryLimit_V2(b *testing.B) {
	for b.Loop() {
		_, _ = parseMemoryLimit("536870912\n", "", true, false)
	}
}

func BenchmarkParseMemoryLimit_V1(b *testing.B) {
	for b.Loop() {
		_, _ = parseMemoryLimit("", "1073741824\n", false, true)
	}
}

func BenchmarkReadMemoryLimitFromPath(b *testing.B) {
	tmpDir := b.TempDir()
	memMaxPath := filepath.Join(tmpDir, "memory.max")
	_ = os.WriteFile(memMaxPath, []byte("536870912\n"), 0o600)

	for b.Loop() {
		_, _ = readMemoryLimitFromPath(memMaxPath, true)
	}
}
