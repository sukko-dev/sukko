// Package version provides build-time version information.
// Variables are set via ldflags during build:
//
//	-X 'github.com/sukko-dev/sukko/internal/shared/version.Version=v1.0.0'
//	-X 'github.com/sukko-dev/sukko/internal/shared/version.CommitHash=abc123'
//	-X 'github.com/sukko-dev/sukko/internal/shared/version.BuildTime=2024-01-15T10:30:00Z'
package version

// Build-time variables (set via ldflags).
var (
	// Version is the semantic version (e.g., "v1.0.0").
	Version = "dev"
	// CommitHash is the git commit SHA (short or full).
	CommitHash = "unknown"
	// BuildTime is the build timestamp in RFC3339 format.
	BuildTime = "unknown"
)

// Info holds version information for JSON serialization.
// Edition info is served by the separate GET /edition endpoint.
type Info struct {
	Version    string `json:"version"`
	CommitHash string `json:"commit_hash"`
	BuildTime  string `json:"build_time"`
	Service    string `json:"service"`
}

// Get returns the current version info for the given service name.
func Get(service string) Info {
	return Info{
		Version:    Version,
		CommitHash: CommitHash,
		BuildTime:  BuildTime,
		Service:    service,
	}
}

// String returns a human-readable version string.
func String() string {
	return Version + " (" + CommitHash + ")"
}
