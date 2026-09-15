// Package profiling provides pprof endpoint registration and Pyroscope
// continuous profiling initialization. Both are disabled by default
// with zero overhead when not enabled.
package profiling

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"runtime"

	"github.com/grafana/pyroscope-go"
	"github.com/rs/zerolog"
)

// PyroscopeConfig holds configuration for Pyroscope continuous profiling.
type PyroscopeConfig struct {
	Enabled     bool
	Addr        string
	ServiceName string
	Environment string
}

// HandleFuncRegistrar registers HTTP handler functions by pattern.
// *http.ServeMux.HandleFunc matches directly. chi.Mux.HandleFunc uses
// http.HandlerFunc (named type) so requires a thin wrapper at the call site.
type HandleFuncRegistrar func(pattern string, handler func(http.ResponseWriter, *http.Request))

// InitPprof registers /debug/pprof/ handlers using the given registrar when enabled.
// When disabled, this is a no-op.
//
// Usage with *http.ServeMux:
//
//	profiling.InitPprof(mux.HandleFunc, enabled, logger)
//
// Usage with chi.Mux (wrapper needed — chi uses http.HandlerFunc named type):
//
//	profiling.InitPprof(func(p string, h func(http.ResponseWriter, *http.Request)) {
//	    r.HandleFunc(p, h)
//	}, enabled, logger)
func InitPprof(register HandleFuncRegistrar, enabled bool, logger zerolog.Logger) {
	if !enabled {
		return
	}

	register("/debug/pprof/", pprof.Index)
	register("/debug/pprof/cmdline", pprof.Cmdline)
	register("/debug/pprof/profile", pprof.Profile)
	register("/debug/pprof/symbol", pprof.Symbol)
	register("/debug/pprof/trace", pprof.Trace)

	logger.Info().Msg("pprof endpoints enabled at /debug/pprof/")
}

// InitPyroscope starts the Pyroscope push agent for continuous profiling.
// Returns a stop function that must be called during shutdown.
// When disabled, returns a no-op stop function with zero overhead.
func InitPyroscope(cfg PyroscopeConfig, logger zerolog.Logger) (stop func(), err error) {
	if !cfg.Enabled {
		logger.Info().Msg("Pyroscope disabled")
		return func() {}, nil
	}

	runtime.SetMutexProfileFraction(5)
	runtime.SetBlockProfileRate(5)

	profiler, err := pyroscope.Start(pyroscope.Config{
		ApplicationName: cfg.ServiceName,
		ServerAddress:   cfg.Addr,
		Tags:            map[string]string{"environment": cfg.Environment},
		ProfileTypes: []pyroscope.ProfileType{
			pyroscope.ProfileCPU,
			pyroscope.ProfileAllocObjects,
			pyroscope.ProfileAllocSpace,
			pyroscope.ProfileInuseObjects,
			pyroscope.ProfileInuseSpace,
			pyroscope.ProfileGoroutines,
			pyroscope.ProfileMutexCount,
			pyroscope.ProfileMutexDuration,
			pyroscope.ProfileBlockCount,
			pyroscope.ProfileBlockDuration,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("start pyroscope agent: %w", err)
	}

	logger.Info().
		Str("addr", cfg.Addr).
		Str("service", cfg.ServiceName).
		Msg("Pyroscope continuous profiling enabled")

	return func() {
		if stopErr := profiler.Stop(); stopErr != nil {
			logger.Error().Err(stopErr).Msg("Failed to stop Pyroscope profiler")
		}
	}, nil
}
