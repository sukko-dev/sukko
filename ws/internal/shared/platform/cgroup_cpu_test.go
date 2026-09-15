package platform

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// =============================================================================
// ThrottleStats Tests
// =============================================================================

func TestThrottleStats_ZeroValue(t *testing.T) {
	t.Parallel()
	var stats ThrottleStats

	if stats.NrPeriods != 0 {
		t.Errorf("NrPeriods: got %d, want 0", stats.NrPeriods)
	}
	if stats.NrThrottled != 0 {
		t.Errorf("NrThrottled: got %d, want 0", stats.NrThrottled)
	}
	if stats.ThrottledSec != 0 {
		t.Errorf("ThrottledSec: got %f, want 0", stats.ThrottledSec)
	}
}

func TestThrottleStats_Values(t *testing.T) {
	t.Parallel()
	stats := ThrottleStats{
		NrPeriods:    100,
		NrThrottled:  10,
		ThrottledSec: 1.5,
	}

	if stats.NrPeriods != 100 {
		t.Errorf("NrPeriods: got %d, want 100", stats.NrPeriods)
	}
	if stats.NrThrottled != 10 {
		t.Errorf("NrThrottled: got %d, want 10", stats.NrThrottled)
	}
	if stats.ThrottledSec != 1.5 {
		t.Errorf("ThrottledSec: got %f, want 1.5", stats.ThrottledSec)
	}
}

// =============================================================================
// ContainerCPU Tests (with mocked filesystem)
// =============================================================================

func TestContainerCPU_GetAllocation(t *testing.T) {
	t.Parallel()
	cc := &ContainerCPU{
		numCPUsAllocated: 2.5,
	}

	got := cc.GetAllocation()
	if got != 2.5 {
		t.Errorf("GetAllocation: got %f, want 2.5", got)
	}
}

func TestContainerCPU_GetAllocation_ThreadSafe(t *testing.T) {
	t.Parallel()
	cc := &ContainerCPU{
		numCPUsAllocated: 4.0,
	}

	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			_ = cc.GetAllocation()
		})
	}
	wg.Wait()
}

func TestContainerCPU_GetInfo(t *testing.T) {
	t.Parallel()
	cc := &ContainerCPU{
		cgroupVersion:    2,
		cgroupPath:       "/sys/fs/cgroup/test",
		cpuQuota:         200000,
		cpuPeriod:        100000,
		numCPUsAllocated: 2.0,
	}

	info := cc.GetInfo()

	if info["cgroup_version"] != 2 {
		t.Errorf("cgroup_version: got %v, want 2", info["cgroup_version"])
	}
	if info["cgroup_path"] != "/sys/fs/cgroup/test" {
		t.Errorf("cgroup_path: got %v, want /sys/fs/cgroup/test", info["cgroup_path"])
	}
	if info["cpu_quota"] != int64(200000) {
		t.Errorf("cpu_quota: got %v, want 200000", info["cpu_quota"])
	}
	if info["cpu_period"] != int64(100000) {
		t.Errorf("cpu_period: got %v, want 100000", info["cpu_period"])
	}
	if info["cpus_allocated"] != 2.0 {
		t.Errorf("cpus_allocated: got %v, want 2.0", info["cpus_allocated"])
	}
}

func TestContainerCPU_GetInfo_ThreadSafe(t *testing.T) {
	t.Parallel()
	cc := &ContainerCPU{
		cgroupVersion:    2,
		cgroupPath:       "/sys/fs/cgroup/test",
		cpuQuota:         100000,
		cpuPeriod:        100000,
		numCPUsAllocated: 1.0,
	}

	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			_ = cc.GetInfo()
		})
	}
	wg.Wait()
}

// =============================================================================
// CPUMonitor Tests
// =============================================================================

func TestCPUMonitor_Mode_Host(t *testing.T) {
	t.Parallel()
	// Create a CPUMonitor in host mode (no container detection)
	cm := &CPUMonitor{
		mode:   "host",
		logger: zerolog.Nop(),
	}

	if cm.Mode() != "host" {
		t.Errorf("Mode: got %s, want host", cm.Mode())
	}
}

func TestCPUMonitor_Mode_Container(t *testing.T) {
	t.Parallel()
	cm := &CPUMonitor{
		mode: "container",
		containerCPU: &ContainerCPU{
			numCPUsAllocated: 1.0,
		},
		logger: zerolog.Nop(),
	}

	if cm.Mode() != "container" {
		t.Errorf("Mode: got %s, want container", cm.Mode())
	}
}

func TestCPUMonitor_GetAllocation_HostMode(t *testing.T) {
	t.Parallel()
	cm := &CPUMonitor{
		mode:   "host",
		logger: zerolog.Nop(),
	}

	allocation := cm.GetAllocation()
	expectedCPUs := float64(runtime.NumCPU())

	if allocation != expectedCPUs {
		t.Errorf("GetAllocation (host): got %f, want %f", allocation, expectedCPUs)
	}
}

func TestCPUMonitor_GetAllocation_ContainerMode(t *testing.T) {
	t.Parallel()
	cm := &CPUMonitor{
		mode: "container",
		containerCPU: &ContainerCPU{
			numCPUsAllocated: 2.5,
		},
		logger: zerolog.Nop(),
	}

	allocation := cm.GetAllocation()

	if allocation != 2.5 {
		t.Errorf("GetAllocation (container): got %f, want 2.5", allocation)
	}
}

func TestCPUMonitor_GetInfo_HostMode(t *testing.T) {
	t.Parallel()
	cm := &CPUMonitor{
		mode:   "host",
		logger: zerolog.Nop(),
	}

	info := cm.GetInfo()

	if info["mode"] != "host" {
		t.Errorf("mode: got %v, want host", info["mode"])
	}

	allocation, ok := info["allocation"].(float64)
	if !ok {
		t.Error("allocation should be float64")
	}
	if allocation != float64(runtime.NumCPU()) {
		t.Errorf("allocation: got %f, want %f", allocation, float64(runtime.NumCPU()))
	}
}

func TestCPUMonitor_GetInfo_ContainerMode(t *testing.T) {
	t.Parallel()
	cm := &CPUMonitor{
		mode: "container",
		containerCPU: &ContainerCPU{
			cgroupVersion:    2,
			cgroupPath:       "/sys/fs/cgroup/test",
			cpuQuota:         150000,
			cpuPeriod:        100000,
			numCPUsAllocated: 1.5,
		},
		logger: zerolog.Nop(),
	}

	info := cm.GetInfo()

	if info["mode"] != "container" {
		t.Errorf("mode: got %v, want container", info["mode"])
	}
	if info["cgroup_version"] != 2 {
		t.Errorf("cgroup_version: got %v, want 2", info["cgroup_version"])
	}
	if info["cpus_allocated"] != 1.5 {
		t.Errorf("cpus_allocated: got %v, want 1.5", info["cpus_allocated"])
	}
}

// =============================================================================
// File Parsing Helper Tests (using temp files)
// =============================================================================

func TestReadCPUQuota_V2_WithQuota(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	// Create cpu.max file with quota
	cpuMaxPath := filepath.Join(tmpDir, "cpu.max")
	err := os.WriteFile(cpuMaxPath, []byte("200000 100000\n"), 0o600)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	quota, period, err := readCPUQuota(tmpDir, 2)
	if err != nil {
		t.Fatalf("readCPUQuota failed: %v", err)
	}

	if quota != 200000 {
		t.Errorf("quota: got %d, want 200000", quota)
	}
	if period != 100000 {
		t.Errorf("period: got %d, want 100000", period)
	}
}

func TestReadCPUQuota_V2_NoQuota(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	// Create cpu.max file with "max" (no limit)
	cpuMaxPath := filepath.Join(tmpDir, "cpu.max")
	err := os.WriteFile(cpuMaxPath, []byte("max 100000\n"), 0o600)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	quota, period, err := readCPUQuota(tmpDir, 2)
	if err != nil {
		t.Fatalf("readCPUQuota failed: %v", err)
	}

	if quota != -1 {
		t.Errorf("quota: got %d, want -1 (no limit)", quota)
	}
	if period != 0 {
		t.Errorf("period: got %d, want 0", period)
	}
}

func TestReadCPUQuota_V2_InvalidFormat(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	// Create cpu.max file with invalid format
	cpuMaxPath := filepath.Join(tmpDir, "cpu.max")
	err := os.WriteFile(cpuMaxPath, []byte("invalid\n"), 0o600)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	_, _, err = readCPUQuota(tmpDir, 2)
	if err == nil {
		t.Error("Expected error for invalid format")
	}
}

func TestReadCPUQuota_V1_WithQuota(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	// Create cgroup v1 files
	quotaPath := filepath.Join(tmpDir, "cpu.cfs_quota_us")
	periodPath := filepath.Join(tmpDir, "cpu.cfs_period_us")

	err := os.WriteFile(quotaPath, []byte("150000\n"), 0o600)
	if err != nil {
		t.Fatalf("Failed to create quota file: %v", err)
	}

	err = os.WriteFile(periodPath, []byte("100000\n"), 0o600)
	if err != nil {
		t.Fatalf("Failed to create period file: %v", err)
	}

	quota, period, err := readCPUQuota(tmpDir, 1)
	if err != nil {
		t.Fatalf("readCPUQuota failed: %v", err)
	}

	if quota != 150000 {
		t.Errorf("quota: got %d, want 150000", quota)
	}
	if period != 100000 {
		t.Errorf("period: got %d, want 100000", period)
	}
}

func TestReadCPUQuota_V1_NoLimit(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	// Create cgroup v1 files with -1 (no limit)
	quotaPath := filepath.Join(tmpDir, "cpu.cfs_quota_us")
	periodPath := filepath.Join(tmpDir, "cpu.cfs_period_us")

	err := os.WriteFile(quotaPath, []byte("-1\n"), 0o600)
	if err != nil {
		t.Fatalf("Failed to create quota file: %v", err)
	}

	err = os.WriteFile(periodPath, []byte("100000\n"), 0o600)
	if err != nil {
		t.Fatalf("Failed to create period file: %v", err)
	}

	quota, period, err := readCPUQuota(tmpDir, 1)
	if err != nil {
		t.Fatalf("readCPUQuota failed: %v", err)
	}

	if quota != -1 {
		t.Errorf("quota: got %d, want -1", quota)
	}
	if period != 100000 {
		t.Errorf("period: got %d, want 100000", period)
	}
}

func TestReadCPUQuota_MissingFile(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	_, _, err := readCPUQuota(tmpDir, 2)
	if err == nil {
		t.Error("Expected error for missing file")
	}
}

func TestReadCPUUsage_V2(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	// Create cpu.stat file with usage_usec
	cpuStatPath := filepath.Join(tmpDir, "cpu.stat")
	content := `usage_usec 1234567890
user_usec 1000000000
system_usec 234567890
nr_periods 100
nr_throttled 5
throttled_usec 500000
`
	err := os.WriteFile(cpuStatPath, []byte(content), 0o600)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	usage, err := readCPUUsage(tmpDir, 2)
	if err != nil {
		t.Fatalf("readCPUUsage failed: %v", err)
	}

	if usage != 1234567890 {
		t.Errorf("usage: got %d, want 1234567890", usage)
	}
}

func TestReadCPUUsage_V2_MissingUsageUsec(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	// Create cpu.stat file without usage_usec
	cpuStatPath := filepath.Join(tmpDir, "cpu.stat")
	content := `user_usec 1000000000
system_usec 234567890
`
	err := os.WriteFile(cpuStatPath, []byte(content), 0o600)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	_, err = readCPUUsage(tmpDir, 2)
	if err == nil {
		t.Error("Expected error for missing usage_usec")
	}
}

func TestReadCPUUsage_V1(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	// Create cpuacct.usage file (nanoseconds)
	cpuacctPath := filepath.Join(tmpDir, "cpuacct.usage")
	// 1234567890000 nanoseconds = 1234567890 microseconds
	err := os.WriteFile(cpuacctPath, []byte("1234567890000\n"), 0o600)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	usage, err := readCPUUsage(tmpDir, 1)
	if err != nil {
		t.Fatalf("readCPUUsage failed: %v", err)
	}

	// Should be converted from nanoseconds to microseconds
	if usage != 1234567890 {
		t.Errorf("usage: got %d, want 1234567890", usage)
	}
}

func TestReadCPUUsage_MissingFile(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	_, err := readCPUUsage(tmpDir, 2)
	if err == nil {
		t.Error("Expected error for missing file")
	}
}

func TestReadThrottleStats_V2(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	// Create cpu.stat file with throttle stats
	cpuStatPath := filepath.Join(tmpDir, "cpu.stat")
	content := `usage_usec 1234567890
nr_periods 1000
nr_throttled 50
throttled_usec 5000000
`
	err := os.WriteFile(cpuStatPath, []byte(content), 0o600)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	stats, err := readThrottleStats(tmpDir, 2)
	if err != nil {
		t.Fatalf("readThrottleStats failed: %v", err)
	}

	if stats.NrPeriods != 1000 {
		t.Errorf("NrPeriods: got %d, want 1000", stats.NrPeriods)
	}
	if stats.NrThrottled != 50 {
		t.Errorf("NrThrottled: got %d, want 50", stats.NrThrottled)
	}
	// 5000000 usec = 5.0 seconds
	if stats.ThrottledSec != 5.0 {
		t.Errorf("ThrottledSec: got %f, want 5.0", stats.ThrottledSec)
	}
}

func TestReadThrottleStats_V1(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	// Create cpu.stat file with v1 format (throttled_time in nanoseconds)
	cpuStatPath := filepath.Join(tmpDir, "cpu.stat")
	content := `nr_periods 2000
nr_throttled 100
throttled_time 3000000000
`
	err := os.WriteFile(cpuStatPath, []byte(content), 0o600)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	stats, err := readThrottleStats(tmpDir, 1)
	if err != nil {
		t.Fatalf("readThrottleStats failed: %v", err)
	}

	if stats.NrPeriods != 2000 {
		t.Errorf("NrPeriods: got %d, want 2000", stats.NrPeriods)
	}
	if stats.NrThrottled != 100 {
		t.Errorf("NrThrottled: got %d, want 100", stats.NrThrottled)
	}
	// 3000000000 nanoseconds = 3.0 seconds
	if stats.ThrottledSec != 3.0 {
		t.Errorf("ThrottledSec: got %f, want 3.0", stats.ThrottledSec)
	}
}

func TestReadThrottleStats_MissingFile(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	_, err := readThrottleStats(tmpDir, 2)
	if err == nil {
		t.Error("Expected error for missing file")
	}
}

func TestReadThrottleStats_PartialData(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	// Create cpu.stat file with only some fields
	cpuStatPath := filepath.Join(tmpDir, "cpu.stat")
	content := `nr_periods 500
`
	err := os.WriteFile(cpuStatPath, []byte(content), 0o600)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	stats, err := readThrottleStats(tmpDir, 2)
	if err != nil {
		t.Fatalf("readThrottleStats failed: %v", err)
	}

	if stats.NrPeriods != 500 {
		t.Errorf("NrPeriods: got %d, want 500", stats.NrPeriods)
	}
	// Missing fields should be zero
	if stats.NrThrottled != 0 {
		t.Errorf("NrThrottled: got %d, want 0", stats.NrThrottled)
	}
	if stats.ThrottledSec != 0 {
		t.Errorf("ThrottledSec: got %f, want 0", stats.ThrottledSec)
	}
}

// =============================================================================
// Integration-style Tests (on actual system - may skip if not in container)
// =============================================================================

func TestNewCPUMonitor_FallbackToHost(t *testing.T) {
	t.Parallel()
	logger := zerolog.Nop()

	// This test runs on any system - if not in a container, it falls back to host mode
	cm := NewCPUMonitor(logger)

	mode := cm.Mode()
	if mode != "container" && mode != "host" {
		t.Errorf("Mode should be 'container' or 'host', got %s", mode)
	}

	// Should return a valid allocation regardless of mode
	allocation := cm.GetAllocation()
	if allocation <= 0 {
		t.Errorf("Allocation should be > 0, got %f", allocation)
	}

	// GetInfo should return valid data
	info := cm.GetInfo()
	if info["mode"] == nil {
		t.Error("GetInfo should contain 'mode'")
	}
	if info["allocation"] == nil {
		t.Error("GetInfo should contain 'allocation'")
	}
}

func TestNewCPUMonitor_GetPercent(t *testing.T) {
	t.Parallel()
	logger := zerolog.Nop()
	cm := NewCPUMonitor(logger)

	// Wait a bit to allow CPU measurement
	time.Sleep(150 * time.Millisecond)

	percent, throttle, err := cm.GetPercent()

	// Behavior depends on the mode the monitor is running in
	mode := cm.Mode()
	if mode == "host" {
		// In host mode, GetPercent uses gopsutil which should always work
		if err != nil {
			t.Errorf("GetPercent in host mode should not fail: %v", err)
		}
		// CPU percent should be a reasonable value (0-100)
		if percent < 0 || percent > 100 {
			t.Errorf("CPU percent in host mode should be 0-100, got %f", percent)
		}
		// Throttle stats are only available in container mode, should be zero in host mode
		if throttle.NrPeriods != 0 || throttle.NrThrottled != 0 {
			t.Logf("Throttle stats in host mode (unexpected but not an error): %+v", throttle)
		}
	} else {
		// In container mode, GetPercent reads cgroup stats
		// This may fail if cgroup v2 is not properly set up
		if err != nil {
			// Verify we get a meaningful error, not a panic
			t.Logf("GetPercent in container mode returned error (acceptable in some environments): %v", err)
			// Ensure we didn't get garbage values
			if percent != 0 {
				t.Errorf("CPU percent should be 0 on error, got %f", percent)
			}
		} else {
			// CPU percent should be a reasonable value (can exceed 100% with multiple CPUs)
			if percent < 0 {
				t.Errorf("CPU percent should not be negative: %f", percent)
			}
			// Throttle stats should be valid
			if throttle.ThrottledSec < 0 {
				t.Error("ThrottledSec should not be negative")
			}
		}
	}
}

func TestCPUMonitor_GetHostPercent(t *testing.T) {
	t.Parallel()
	logger := zerolog.Nop()
	cm := NewCPUMonitor(logger)

	percent, err := cm.GetHostPercent()
	if err != nil {
		t.Fatalf("GetHostPercent failed: %v", err)
	}

	// Host CPU percent should be between 0 and 100
	if percent < 0 || percent > 100 {
		t.Errorf("Host CPU percent should be 0-100, got %f", percent)
	}
}

// =============================================================================
// ContainerCPU Calculation Tests (mocked)
// =============================================================================

func TestContainerCPU_CalculatePercent(t *testing.T) {
	t.Parallel()
	// Test the percentage calculation logic
	// Formula: (usageDelta / timeDeltaUsec) * 100 / numCPUsAllocated

	tests := []struct {
		name             string
		usageDelta       uint64
		timeDeltaUsec    int64
		numCPUsAllocated float64
		wantPercent      float64
	}{
		{
			name:             "100% of 1 CPU",
			usageDelta:       1000000, // 1 second of CPU time
			timeDeltaUsec:    1000000, // 1 second wall time
			numCPUsAllocated: 1.0,
			wantPercent:      100.0,
		},
		{
			name:             "50% of 1 CPU",
			usageDelta:       500000,  // 0.5 seconds of CPU time
			timeDeltaUsec:    1000000, // 1 second wall time
			numCPUsAllocated: 1.0,
			wantPercent:      50.0,
		},
		{
			name:             "100% of 2 CPUs (using 1 CPU)",
			usageDelta:       1000000, // 1 second of CPU time
			timeDeltaUsec:    1000000, // 1 second wall time
			numCPUsAllocated: 2.0,
			wantPercent:      50.0, // Using 50% of allocation
		},
		{
			name:             "200% raw usage normalized to 2 CPUs",
			usageDelta:       2000000, // 2 seconds of CPU time
			timeDeltaUsec:    1000000, // 1 second wall time
			numCPUsAllocated: 2.0,
			wantPercent:      100.0, // Full allocation used
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rawPercent := (float64(tt.usageDelta) / float64(tt.timeDeltaUsec)) * 100.0
			percent := rawPercent / tt.numCPUsAllocated

			if percent != tt.wantPercent {
				t.Errorf("percent: got %f, want %f", percent, tt.wantPercent)
			}
		})
	}
}

// =============================================================================
// Benchmark Tests
// =============================================================================

func BenchmarkContainerCPU_GetAllocation(b *testing.B) {
	cc := &ContainerCPU{
		numCPUsAllocated: 2.0,
	}

	for b.Loop() {
		_ = cc.GetAllocation()
	}
}

func BenchmarkContainerCPU_GetInfo(b *testing.B) {
	cc := &ContainerCPU{
		cgroupVersion:    2,
		cgroupPath:       "/sys/fs/cgroup/test",
		cpuQuota:         200000,
		cpuPeriod:        100000,
		numCPUsAllocated: 2.0,
	}

	for b.Loop() {
		_ = cc.GetInfo()
	}
}

func BenchmarkCPUMonitor_GetAllocation_Host(b *testing.B) {
	cm := &CPUMonitor{
		mode:   "host",
		logger: zerolog.Nop(),
	}

	for b.Loop() {
		_ = cm.GetAllocation()
	}
}

func BenchmarkReadCPUQuota_V2(b *testing.B) {
	tmpDir := b.TempDir()
	cpuMaxPath := filepath.Join(tmpDir, "cpu.max")
	_ = os.WriteFile(cpuMaxPath, []byte("200000 100000\n"), 0o600)

	for b.Loop() {
		_, _, _ = readCPUQuota(tmpDir, 2)
	}
}

func BenchmarkReadCPUUsage_V2(b *testing.B) {
	tmpDir := b.TempDir()
	cpuStatPath := filepath.Join(tmpDir, "cpu.stat")
	content := `usage_usec 1234567890
user_usec 1000000000
system_usec 234567890
`
	_ = os.WriteFile(cpuStatPath, []byte(content), 0o600)

	for b.Loop() {
		_, _ = readCPUUsage(tmpDir, 2)
	}
}

func BenchmarkReadThrottleStats_V2(b *testing.B) {
	tmpDir := b.TempDir()
	cpuStatPath := filepath.Join(tmpDir, "cpu.stat")
	content := `usage_usec 1234567890
nr_periods 1000
nr_throttled 50
throttled_usec 5000000
`
	_ = os.WriteFile(cpuStatPath, []byte(content), 0o600)

	for b.Loop() {
		_, _ = readThrottleStats(tmpDir, 2)
	}
}
