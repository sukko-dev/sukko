// Package server provides core WebSocket server functionality.
// This file contains adapter implementations for the interfaces defined in interfaces.go.
// Adapters bridge external libraries (zerolog, monitoring, limits) to internal interfaces
// enabling dependency injection and testability.
package server

import (
	"time"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/alerting"
)

// =============================================================================
// Zerolog Adapter (implements Logger interface)
// =============================================================================

// ZerologAdapter wraps zerolog.Logger to implement the Logger interface.
type ZerologAdapter struct {
	logger zerolog.Logger
}

// NewZerologAdapter creates a Logger from a zerolog.Logger.
func NewZerologAdapter(logger zerolog.Logger) *ZerologAdapter {
	return &ZerologAdapter{logger: logger}
}

// Debug returns a debug-level log event.
func (z *ZerologAdapter) Debug() LogEvent { return &ZerologEventAdapter{event: z.logger.Debug()} }

// Info returns an info-level log event.
func (z *ZerologAdapter) Info() LogEvent { return &ZerologEventAdapter{event: z.logger.Info()} }

// Warn returns a warn-level log event.
func (z *ZerologAdapter) Warn() LogEvent { return &ZerologEventAdapter{event: z.logger.Warn()} }

// Error returns an error-level log event.
func (z *ZerologAdapter) Error() LogEvent { return &ZerologEventAdapter{event: z.logger.Error()} }

// Printf logs a formatted message.
func (z *ZerologAdapter) Printf(format string, v ...any) {
	z.logger.Printf(format, v...)
}

// ZerologEventAdapter wraps zerolog.Event to implement LogEvent.
type ZerologEventAdapter struct {
	event *zerolog.Event
}

// Err adds an error field to the log event.
func (e *ZerologEventAdapter) Err(err error) LogEvent {
	e.event = e.event.Err(err)
	return e
}

// Int64 adds an int64 field to the log event.
func (e *ZerologEventAdapter) Int64(key string, val int64) LogEvent {
	e.event = e.event.Int64(key, val)
	return e
}

// Int adds an int field to the log event.
func (e *ZerologEventAdapter) Int(key string, val int) LogEvent {
	e.event = e.event.Int(key, val)
	return e
}

// Str adds a string field to the log event.
func (e *ZerologEventAdapter) Str(key, val string) LogEvent {
	e.event = e.event.Str(key, val)
	return e
}

// Interface adds an interface field to the log event.
func (e *ZerologEventAdapter) Interface(key string, val any) LogEvent {
	e.event = e.event.Interface(key, val)
	return e
}

// Msg sends the log event with the given message.
func (e *ZerologEventAdapter) Msg(msg string) {
	e.event.Msg(msg)
}

// =============================================================================
// Alert Logger (implements AlertLogger interface)
// =============================================================================

// alertLogger implements AlertLogger using zerolog + alerting.Alerter.
type alertLogger struct {
	logger  zerolog.Logger
	alerter alerting.Alerter
}

// newAlertLogger creates an AlertLogger from a zerolog.Logger and alerting.Alerter.
func newAlertLogger(logger zerolog.Logger, alerter alerting.Alerter) *alertLogger {
	return &alertLogger{
		logger:  logger.With().Str("component", "alert").Logger(),
		alerter: alerter,
	}
}

// Info logs an info-level alert event.
func (a *alertLogger) Info(event, message string, metadata map[string]any) {
	a.logger.Info().Str("alert_event", event).Fields(metadata).Msg(message)
}

// Warning logs a warning-level alert event and fires an alert.
func (a *alertLogger) Warning(event, message string, metadata map[string]any) {
	a.logger.Warn().Str("alert_event", event).Fields(metadata).Msg(message)
	if a.alerter != nil {
		a.alerter.Alert(alerting.WARNING, message, metadata)
	}
}

// Error logs an error-level alert event and fires an alert.
func (a *alertLogger) Error(event, message string, metadata map[string]any) {
	a.logger.Error().Str("alert_event", event).Fields(metadata).Msg(message)
	if a.alerter != nil {
		a.alerter.Alert(alerting.ERROR, message, metadata)
	}
}

// Critical logs a critical-level alert event and fires an alert.
func (a *alertLogger) Critical(event, message string, metadata map[string]any) {
	a.logger.Error().Str("alert_event", event).Str("severity", "CRITICAL").Fields(metadata).Msg(message)
	if a.alerter != nil {
		a.alerter.Alert(alerting.CRITICAL, message, metadata)
	}
}

// =============================================================================
// Rate Limiter Adapter (implements RateLimiter interface)
// =============================================================================

// RateLimiterImpl is the interface that limits.RateLimiter implements.
type RateLimiterImpl interface {
	CheckLimit(clientID int64) bool
}

// RateLimiterAdapter wraps limits.RateLimiter to implement RateLimiter interface.
type RateLimiterAdapter struct {
	limiter RateLimiterImpl
}

// NewRateLimiterAdapter creates a RateLimiter from limits.RateLimiter.
func NewRateLimiterAdapter(limiter RateLimiterImpl) *RateLimiterAdapter {
	return &RateLimiterAdapter{limiter: limiter}
}

// CheckLimit checks if the client has exceeded its rate limit.
func (r *RateLimiterAdapter) CheckLimit(clientID int64) bool {
	return r.limiter.CheckLimit(clientID)
}

// =============================================================================
// Real Clock Implementation (implements Clock interface)
// =============================================================================

// RealClock implements Clock using the standard time package.
type RealClock struct{}

// Now returns the current time.
func (c *RealClock) Now() time.Time {
	return time.Now()
}

// NewTicker creates a new ticker with the given duration.
func (c *RealClock) NewTicker(d time.Duration) Ticker {
	return &RealTicker{ticker: time.NewTicker(d)}
}

// After waits for the duration to elapse and then sends the current time.
func (c *RealClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

// RealTicker wraps time.Ticker to implement the Ticker interface.
type RealTicker struct {
	ticker *time.Ticker
}

// C returns the channel on which ticks are delivered.
func (t *RealTicker) C() <-chan time.Time {
	return t.ticker.C
}

// Stop stops the ticker.
func (t *RealTicker) Stop() {
	t.ticker.Stop()
}
