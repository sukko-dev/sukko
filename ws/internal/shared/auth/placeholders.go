// Package auth provides JWT authentication for WebSocket connections.
package auth

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// PlaceholderFunc extracts a value from claims for placeholder resolution.
type PlaceholderFunc func(c *Claims) string

// PlaceholderResolver handles dynamic placeholder substitution in patterns.
// Placeholders use the format {placeholder_name} and are resolved using JWT claims.
//
// Example:
//
//	pattern: "{tenant_id}.{user_id}.balances"
//	claims:  { tenant_id: "acme", sub: "user123" }
//	result:  "acme.user123.balances"
type PlaceholderResolver struct {
	builtins map[string]PlaceholderFunc
	custom   map[string]PlaceholderFunc
}

// DefaultPlaceholders provides standard built-in placeholders.
// These are generic and not product-specific.
var DefaultPlaceholders = map[string]PlaceholderFunc{
	"user_id":   func(c *Claims) string { return c.Subject },
	"tenant_id": func(c *Claims) string { return c.TenantID },
	"tenant":    func(c *Claims) string { return c.TenantID },
	"sub":       func(c *Claims) string { return c.Subject },
	"app_id":    func(c *Claims) string { return c.Subject },
	"principal": func(c *Claims) string { return c.Subject },
}

// PlaceholderNames returns the keys of DefaultPlaceholders.
// Used by types.ChannelRules.ValidateWithPlaceholders() to validate
// placeholder tokens without importing auth from types (leaf package).
func PlaceholderNames() []string {
	names := make([]string, 0, len(DefaultPlaceholders))
	for name := range DefaultPlaceholders {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// NewPlaceholderResolver creates a resolver with default built-in placeholders.
func NewPlaceholderResolver() *PlaceholderResolver {
	return &PlaceholderResolver{
		builtins: DefaultPlaceholders,
		custom:   make(map[string]PlaceholderFunc),
	}
}

// RegisterCustom adds a custom placeholder resolver.
// Custom placeholders take precedence over built-ins with the same name.
func (r *PlaceholderResolver) RegisterCustom(name string, fn PlaceholderFunc) {
	r.custom[name] = fn
}

// RegisterAttribute registers a placeholder that reads from claims.Attributes.
// This is a convenience method for attribute-based placeholders.
//
// Example:
//
//	resolver.RegisterAttribute("tier")
//	// Now {tier} resolves to claims.Attributes["tier"]
func (r *PlaceholderResolver) RegisterAttribute(attrName string) {
	r.custom[attrName] = func(c *Claims) string {
		return c.GetAttribute(attrName)
	}
}

// placeholderRegex matches {placeholder_name} patterns.
var placeholderRegex = regexp.MustCompile(`\{([a-zA-Z_][a-zA-Z0-9_]*)\}`)

// Resolve substitutes all placeholders in the pattern with values from claims.
// Unknown placeholders are left unchanged.
//
// Example:
//
//	Resolve("{tenant_id}.{user_id}.balances", claims)
//	// Returns: "acme.user123.balances"
func (r *PlaceholderResolver) Resolve(pattern string, claims *Claims) string {
	if claims == nil {
		return pattern
	}

	return placeholderRegex.ReplaceAllStringFunc(pattern, func(match string) string {
		// Extract placeholder name (strip braces)
		name := match[1 : len(match)-1]

		// Check custom first (takes precedence)
		if fn, ok := r.custom[name]; ok {
			if value := fn(claims); value != "" {
				return value
			}
		}

		// Check builtins
		if fn, ok := r.builtins[name]; ok {
			if value := fn(claims); value != "" {
				return value
			}
		}

		// Check claims attributes as fallback
		if value := claims.GetAttribute(name); value != "" {
			return value
		}

		// Unknown placeholder - leave unchanged
		return match
	})
}

// HasPlaceholders checks if the pattern contains any placeholders.
func HasPlaceholders(pattern string) bool {
	return placeholderRegex.MatchString(pattern)
}

// ExtractPlaceholderNames returns all placeholder names found in the pattern.
func ExtractPlaceholderNames(pattern string) []string {
	matches := placeholderRegex.FindAllStringSubmatch(pattern, -1)
	names := make([]string, 0, len(matches))
	seen := make(map[string]bool)

	for _, match := range matches {
		if len(match) > 1 && !seen[match[1]] {
			names = append(names, match[1])
			seen[match[1]] = true
		}
	}

	return names
}

// MatchResult contains the result of pattern matching against a channel.
type MatchResult struct {
	Matched    bool
	Captures   map[string]string // Extracted placeholder values
	Literal    string            // The literal portion of the pattern
	Normalized string            // Channel with placeholders replaced
	Error      error             // Non-nil if pattern compilation failed
}

// MatchPattern checks if a channel matches a pattern with placeholders.
// Returns captured values for each placeholder.
//
// Example:
//
//	pattern: "{tenant_id}.{symbol}.trade"
//	channel: "acme.BTC.trade"
//	result:  { Matched: true, Captures: {"tenant_id": "acme", "symbol": "BTC"} }
func MatchPattern(pattern, channel string) *MatchResult {
	result := &MatchResult{
		Captures: make(map[string]string),
	}

	// Escape dots first (before placeholder/wildcard replacement)
	regexPattern := strings.ReplaceAll(pattern, ".", `\.`)

	// Replace placeholders with named capture groups
	regexPattern = placeholderRegex.ReplaceAllStringFunc(regexPattern, func(match string) string {
		name := match[1 : len(match)-1]
		return `(?P<` + name + `>[^.]+)`
	})

	// Allow wildcards
	regexPattern = strings.ReplaceAll(regexPattern, "*", `[^.]+`)

	// Anchor the pattern
	regexPattern = "^" + regexPattern + "$"

	re, err := regexp.Compile(regexPattern)
	if err != nil {
		result.Error = fmt.Errorf("compile channel pattern %q: %w", pattern, err)
		return result
	}

	matches := re.FindStringSubmatch(channel)
	if matches == nil {
		return result
	}

	result.Matched = true
	result.Normalized = channel

	// Extract named captures
	for i, name := range re.SubexpNames() {
		if i > 0 && name != "" && i < len(matches) {
			result.Captures[name] = matches[i]
		}
	}

	return result
}

// BuildPattern constructs a pattern string from parts.
// Convenience function for building channel patterns.
//
// Example:
//
//	BuildPattern("{tenant_id}", "{user_id}", "balances")
//	// Returns: "{tenant_id}.{user_id}.balances"
func BuildPattern(parts ...string) string {
	return strings.Join(parts, ".")
}
