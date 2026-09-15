package logging

import (
	"runtime/debug"

	"github.com/rs/zerolog"
)

// RecoverPanic is a helper for goroutine panic recovery that logs but doesn't exit.
//
// CRITICAL: Use this in ALL goroutine defer blocks to catch panics that would otherwise
// crash the entire process. This is for DEBUGGING - logs panic details but keeps server running.
//
// Example:
//
//	go func() {
//	    defer logging.RecoverPanic(logger, "writePump", map[string]any{"client_id": id})
//	    // ... goroutine work ...
//	}()
func RecoverPanic(logger zerolog.Logger, goroutineName string, fields map[string]any) {
	if r := recover(); r != nil {
		stack := string(debug.Stack())

		// Use Error instead of Fatal so we log but don't exit
		// This lets us see WHICH goroutine panicked and WHY
		event := logger.Error().
			Str("goroutine", goroutineName).
			Interface("panic_value", r).
			Str("stack_trace", stack).
			Str("recovery_mode", "captured_panic_continuing_execution")

		// Add all context fields
		for k, v := range fields {
			event = event.Interface(k, v)
		}

		event.Msg("GOROUTINE PANIC RECOVERED - This would have crashed the server!")
	}
}
