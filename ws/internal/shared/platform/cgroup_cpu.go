package platform

import (
	"bufio"
	"errors"
	"fmt"
	"maps"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/shirou/gopsutil/v3/cpu"
)

// ContainerCPU provides accurate CPU usage relative to container limits
// by reading directly from cgroup statistics files.
type ContainerCPU struct {
	mu               sync.RWMutex
	lastCPUUsec      uint64
	lastSampleTime   time.Time
	cgroupVersion    int     // 1 or 2
	cgroupPath       string  // Detected cgroup path
	cpuQuota         int64   // CPU quota (e.g., 100000 for 1.0 CPU)
	cpuPeriod        int64   // CPU period (usually 100000)
	numCPUsAllocated float64 // Quota/Period = number of CPUs allocated
	lastThrottle     ThrottleStats
}

// ThrottleStats contains CPU throttling statistics from cgroup
type ThrottleStats struct {
	NrPeriods    uint64  // Total enforcement periods
	NrThrottled  uint64  // Times container was throttled
	ThrottledSec float64 // Total time throttled (seconds)
}

// NewContainerCPU detects cgroup configuration and initializes CPU monitor
func NewContainerCPU() (*ContainerCPU, error) {
	cc := &ContainerCPU{
		lastSampleTime: time.Now(),
	}

	// Detect cgroup version and path
	cgroupPath, version, err := detectCgroupPath()
	if err != nil {
		return nil, fmt.Errorf("failed to detect cgroup: %w", err)
	}

	cc.cgroupPath = cgroupPath
	cc.cgroupVersion = version

	// Read CPU quota/period to understand allocation
	quota, period, err := readCPUQuota(cgroupPath, version)
	if err != nil {
		return nil, fmt.Errorf("failed to read CPU quota: %w", err)
	}

	cc.cpuQuota = quota
	cc.cpuPeriod = period

	if quota > 0 && period > 0 {
		cc.numCPUsAllocated = float64(quota) / float64(period)
	} else {
		// No limit set - use number of CPU cores
		cc.numCPUsAllocated = float64(runtime.NumCPU())
	}

	// Initialize first sample
	usage, err := readCPUUsage(cgroupPath, version)
	if err != nil {
		return nil, fmt.Errorf("failed to read initial CPU usage: %w", err)
	}
	cc.lastCPUUsec = usage

	// Initialize throttle stats
	throttle, err := readThrottleStats(cgroupPath, version)
	if err == nil {
		cc.lastThrottle = throttle
	}

	return cc, nil
}

// GetPercent returns CPU usage as percentage of allocated CPUs
// Returns: percentage (0-100+ if throttling), throttling stats
func (cc *ContainerCPU) GetPercent() (percent float64, throttled ThrottleStats, err error) {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	now := time.Now()
	timeDelta := now.Sub(cc.lastSampleTime)

	// Read current CPU usage in microseconds
	currentUsec, err := readCPUUsage(cc.cgroupPath, cc.cgroupVersion)
	if err != nil {
		return 0, ThrottleStats{}, fmt.Errorf("read cpu usage: %w", err)
	}

	// Calculate CPU usage delta.
	// Guard against counter reset (container restart, cgroup migration) where
	// currentUsec < lastCPUUsec would wrap around as uint64, producing a
	// wildly incorrect percentage. On reset, treat as zero usage for this sample.
	var usageDelta uint64
	if currentUsec >= cc.lastCPUUsec {
		usageDelta = currentUsec - cc.lastCPUUsec
	}

	// Calculate percentage
	// usageDelta is in microseconds of CPU time consumed
	// timeDelta is wall-clock time elapsed
	// percent = (cpu_time / wall_time) * 100
	timeDeltaUsec := timeDelta.Microseconds()
	if timeDeltaUsec == 0 {
		return 0, ThrottleStats{}, errors.New("time delta too small")
	}

	// Raw CPU usage percentage (can be > 100% on multi-core)
	rawPercent := (float64(usageDelta) / float64(timeDeltaUsec)) * 100.0

	// Normalize to allocated CPUs
	// If container has 1.0 CPU and uses 100% → return 100%
	// If container has 4.0 CPUs and uses 1.0 CPU → return 25%
	percent = (rawPercent / cc.numCPUsAllocated)

	// Read current throttling stats
	currentThrottle, err := readThrottleStats(cc.cgroupPath, cc.cgroupVersion)
	if err == nil {
		// Return delta since last call
		throttled = ThrottleStats{
			NrPeriods:    currentThrottle.NrPeriods - cc.lastThrottle.NrPeriods,
			NrThrottled:  currentThrottle.NrThrottled - cc.lastThrottle.NrThrottled,
			ThrottledSec: currentThrottle.ThrottledSec - cc.lastThrottle.ThrottledSec,
		}
		cc.lastThrottle = currentThrottle
	}

	// Update state for next call
	cc.lastCPUUsec = currentUsec
	cc.lastSampleTime = now

	return percent, throttled, nil
}

// detectCgroupPath finds the cgroup path for current process
func detectCgroupPath() (path string, version int, err error) {
	// Read /proc/self/cgroup to find our cgroup path
	file, err := os.Open("/proc/self/cgroup")
	if err != nil {
		return "", 0, fmt.Errorf("open /proc/self/cgroup: %w", err)
	}
	defer func() { _ = file.Close() }()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		// Format: hierarchy-ID:controller-list:cgroup-path
		parts := strings.Split(line, ":")
		if len(parts) != 3 {
			continue
		}

		hierarchyID := parts[0]
		cgroupPath := parts[2]

		// cgroup v2: hierarchy-ID is 0 and controller-list is empty
		if hierarchyID == "0" && parts[1] == "" {
			// cgroup v2
			fullPath := "/sys/fs/cgroup" + cgroupPath
			return fullPath, 2, nil
		}

		// cgroup v1: look for cpu controller
		if strings.Contains(parts[1], "cpu") {
			fullPath := "/sys/fs/cgroup/cpu" + cgroupPath
			return fullPath, 1, nil
		}
	}

	return "", 0, errors.New("could not detect cgroup path")
}

// readCPUQuota reads the CPU quota and period
func readCPUQuota(cgroupPath string, version int) (quota, period int64, err error) {
	if version == 2 {
		// cgroup v2: /sys/fs/cgroup/.../cpu.max contains "quota period"
		data, err := os.ReadFile(cgroupPath + "/cpu.max") //nolint:gosec // Path constructed from trusted cgroup detection
		if err != nil {
			return 0, 0, fmt.Errorf("read cgroup v2 cpu.max: %w", err)
		}

		fields := strings.Fields(string(data))
		if len(fields) != 2 {
			return 0, 0, fmt.Errorf("unexpected cpu.max format: %s", string(data))
		}

		if fields[0] == "max" {
			// No quota set
			return -1, 0, nil
		}

		quota, err = strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("parse cgroup v2 cpu quota: %w", err)
		}

		period, err = strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("parse cgroup v2 cpu period: %w", err)
		}

		return quota, period, nil
	}

	// cgroup v1: separate files
	quotaData, err := os.ReadFile(cgroupPath + "/cpu.cfs_quota_us") //nolint:gosec // Path constructed from trusted cgroup detection
	if err != nil {
		return 0, 0, fmt.Errorf("read cgroup v1 cfs_quota_us: %w", err)
	}

	periodData, err := os.ReadFile(cgroupPath + "/cpu.cfs_period_us") //nolint:gosec // Path constructed from trusted cgroup detection
	if err != nil {
		return 0, 0, fmt.Errorf("read cgroup v1 cfs_period_us: %w", err)
	}

	quota, err = strconv.ParseInt(strings.TrimSpace(string(quotaData)), 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parse cgroup v1 cpu quota: %w", err)
	}

	period, err = strconv.ParseInt(strings.TrimSpace(string(periodData)), 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parse cgroup v1 cpu period: %w", err)
	}

	return quota, period, nil
}

// readCPUUsage reads cumulative CPU usage in microseconds
func readCPUUsage(cgroupPath string, version int) (uint64, error) {
	if version == 2 {
		// cgroup v2: cpu.stat contains "usage_usec NNNNNN"
		file, err := os.Open(cgroupPath + "/cpu.stat") //nolint:gosec // Path constructed from trusted cgroup detection
		if err != nil {
			return 0, fmt.Errorf("open cgroup v2 cpu.stat: %w", err)
		}
		defer func() { _ = file.Close() }()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "usage_usec ") {
				fields := strings.Fields(line)
				if len(fields) == 2 {
					usec, err := strconv.ParseUint(fields[1], 10, 64)
					if err != nil {
						return 0, fmt.Errorf("parse usage_usec value: %w", err)
					}
					return usec, nil
				}
			}
		}
		return 0, errors.New("usage_usec not found in cpu.stat")
	}

	// cgroup v1: cpuacct.usage contains nanoseconds
	data, err := os.ReadFile(cgroupPath + "/cpuacct.usage") //nolint:gosec // Path constructed from trusted cgroup detection
	if err != nil {
		return 0, fmt.Errorf("read cgroup v1 cpuacct.usage: %w", err)
	}

	nsec, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse cpuacct.usage value: %w", err)
	}

	// Convert nanoseconds to microseconds
	return nsec / 1000, nil
}

// readThrottleStats reads CPU throttling statistics
func readThrottleStats(cgroupPath string, version int) (ThrottleStats, error) {
	var stats ThrottleStats

	if version == 2 {
		file, err := os.Open(cgroupPath + "/cpu.stat") //nolint:gosec // Path constructed from trusted cgroup detection
		if err != nil {
			return stats, fmt.Errorf("open cgroup v2 cpu.stat for throttle stats: %w", err)
		}
		defer func() { _ = file.Close() }()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			fields := strings.Fields(line)
			if len(fields) != 2 {
				continue
			}

			value, _ := strconv.ParseUint(fields[1], 10, 64)

			switch fields[0] {
			case "nr_periods":
				stats.NrPeriods = value
			case "nr_throttled":
				stats.NrThrottled = value
			case "throttled_usec":
				stats.ThrottledSec = float64(value) / 1000000.0
			}
		}

	} else {
		// cgroup v1: cpu.stat has different format
		file, err := os.Open(cgroupPath + "/cpu.stat") //nolint:gosec // Path constructed from trusted cgroup detection
		if err != nil {
			return stats, fmt.Errorf("open cgroup v1 cpu.stat for throttle stats: %w", err)
		}
		defer func() { _ = file.Close() }()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			fields := strings.Fields(line)
			if len(fields) != 2 {
				continue
			}

			value, _ := strconv.ParseUint(fields[1], 10, 64)

			switch fields[0] {
			case "nr_periods":
				stats.NrPeriods = value
			case "nr_throttled":
				stats.NrThrottled = value
			case "throttled_time":
				// In nanoseconds
				stats.ThrottledSec = float64(value) / 1000000000.0
			}
		}
	}

	return stats, nil
}

// GetAllocation returns the number of CPUs allocated to this container
func (cc *ContainerCPU) GetAllocation() float64 {
	cc.mu.RLock()
	defer cc.mu.RUnlock()
	return cc.numCPUsAllocated
}

// GetInfo returns cgroup configuration info for debugging
func (cc *ContainerCPU) GetInfo() map[string]any {
	cc.mu.RLock()
	defer cc.mu.RUnlock()

	return map[string]any{
		"cgroup_version": cc.cgroupVersion,
		"cgroup_path":    cc.cgroupPath,
		"cpu_quota":      cc.cpuQuota,
		"cpu_period":     cc.cpuPeriod,
		"cpus_allocated": cc.numCPUsAllocated,
	}
}

// CPUMonitor provides unified CPU measurement with automatic fallback
type CPUMonitor struct {
	mode         string        // "container" or "host"
	containerCPU *ContainerCPU // nil if mode=host
	logger       zerolog.Logger
}

// NewCPUMonitor creates a CPU monitor with automatic container detection
// Falls back to host CPU measurement if container detection fails
func NewCPUMonitor(logger zerolog.Logger) *CPUMonitor {
	// Try container-aware measurement
	containerCPU, err := NewContainerCPU()
	if err == nil {
		logger.Info().
			Int("cgroup_version", containerCPU.cgroupVersion).
			Float64("cpus_allocated", containerCPU.GetAllocation()).
			Str("cgroup_path", containerCPU.cgroupPath).
			Msg("Using container-aware CPU measurement")

		return &CPUMonitor{
			mode:         "container",
			containerCPU: containerCPU,
			logger:       logger,
		}
	}

	// Fallback to host CPU measurement
	logger.Warn().
		Err(err).
		Msg("Failed to initialize container CPU measurement, falling back to host CPU")

	return &CPUMonitor{
		mode:   "host",
		logger: logger,
	}
}

// GetPercent returns CPU usage percentage
// In container mode: percentage relative to container allocation (0-100%)
// In host mode: percentage of total host CPUs (0-100% * num_cpus)
func (cm *CPUMonitor) GetPercent() (float64, ThrottleStats, error) {
	if cm.mode == "container" {
		return cm.containerCPU.GetPercent()
	}

	// Fallback to gopsutil for non-containerized
	cpuPercent, err := cpu.Percent(100*time.Millisecond, false)
	if err != nil {
		return 0, ThrottleStats{}, fmt.Errorf("get host cpu percent: %w", err)
	}
	if len(cpuPercent) == 0 {
		return 0, ThrottleStats{}, errors.New("no CPU data")
	}
	return cpuPercent[0], ThrottleStats{}, nil
}

// GetHostPercent returns host-wide CPU percentage (for reference metrics)
func (cm *CPUMonitor) GetHostPercent() (float64, error) {
	cpuPercent, err := cpu.Percent(100*time.Millisecond, false)
	if err != nil {
		return 0, fmt.Errorf("get host cpu percent: %w", err)
	}
	if len(cpuPercent) == 0 {
		return 0, errors.New("no CPU data")
	}
	return cpuPercent[0], nil
}

// GetAllocation returns the number of CPUs allocated
// In container mode: quota/period (e.g., 1.0)
// In host mode: total number of host CPUs
func (cm *CPUMonitor) GetAllocation() float64 {
	if cm.mode == "container" {
		return cm.containerCPU.GetAllocation()
	}
	return float64(runtime.NumCPU())
}

// Mode returns the current CPU monitoring mode
func (cm *CPUMonitor) Mode() string {
	return cm.mode
}

// GetInfo returns configuration information
func (cm *CPUMonitor) GetInfo() map[string]any {
	info := map[string]any{
		"mode":       cm.mode,
		"allocation": cm.GetAllocation(),
	}

	if cm.mode == "container" {
		maps.Copy(info, cm.containerCPU.GetInfo())
	}

	return info
}
