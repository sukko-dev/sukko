// Package auth provides JWT authentication for WebSocket connections.
package auth

import (
	"cmp"
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"
)

// Effect represents the effect of a rule (allow or deny).
type Effect string

// Effect constants for authorization rules.
const (
	EffectAllow Effect = "allow"
	EffectDeny  Effect = "deny"
)

// Action represents an action that can be authorized.
type Action string

// Action constants for authorization.
const (
	ActionRead    Action = "read"
	ActionWrite   Action = "write"
	RuleSubscribe Action = "subscribe"
	RulePublish   Action = "publish"
	RulePresence  Action = "presence"
)

// ConditionType represents the type of condition.
type ConditionType string

// ConditionType constants for rule conditions.
const (
	ConditionTypeClaim     ConditionType = "claim"
	ConditionTypeAttribute ConditionType = "attribute"
	ConditionTypeChannel   ConditionType = "channel"
	ConditionTypeTime      ConditionType = "time"
)

// Operator represents a comparison operator.
type Operator string

// Operator constants for condition comparisons.
const (
	OpEquals     Operator = "eq"
	OpNotEquals  Operator = "neq"
	OpContains   Operator = "contains"
	OpMatches    Operator = "matches"
	OpExists     Operator = "exists"
	OpIn         Operator = "in"
	OpStartsWith Operator = "starts_with"
	OpEndsWith   Operator = "ends_with"
)

// PermissionRule represents a single authorization rule.
type PermissionRule struct {
	// ID is a unique identifier for the rule.
	ID string `yaml:"id" json:"id"`

	// Description explains what the rule does.
	Description string `yaml:"description" json:"description"`

	// Priority determines evaluation order (higher = first).
	Priority int `yaml:"priority" json:"priority"`

	// Match defines the channel pattern to match.
	Match RuleMatch `yaml:"match" json:"match"`

	// Actions are the actions this rule applies to.
	Actions []Action `yaml:"actions" json:"actions"`

	// Conditions are additional requirements (AND logic).
	Conditions []Condition `yaml:"conditions" json:"conditions"`

	// Effect is the outcome if the rule matches (allow or deny).
	Effect Effect `yaml:"effect" json:"effect"`

	// compiled regex for channel pattern (internal)
	compiledPattern *regexp.Regexp
}

// RuleMatch defines how to match channels.
type RuleMatch struct {
	// ChannelPattern is the pattern to match (supports placeholders).
	ChannelPattern string `yaml:"channel_pattern" json:"channel_pattern"`

	// ChannelPrefix matches channels starting with this prefix.
	ChannelPrefix string `yaml:"channel_prefix" json:"channel_prefix"`

	// ChannelExact matches this exact channel.
	ChannelExact string `yaml:"channel_exact" json:"channel_exact"`
}

// Condition represents a requirement that must be met.
type Condition struct {
	// Type is the condition type (claim, attribute, channel, time).
	Type ConditionType `yaml:"type" json:"type"`

	// Field is the field to check (e.g., "roles", "attrs.tier").
	Field string `yaml:"field" json:"field"`

	// Op is the comparison operator.
	Op Operator `yaml:"op" json:"op"`

	// Value is the value to compare against.
	Value any `yaml:"value" json:"value"`

	// Negate inverts the condition result.
	Negate bool `yaml:"negate" json:"negate"`
}

// PolicyEngine evaluates authorization rules.
type PolicyEngine struct {
	mu             sync.RWMutex
	rules          []*PermissionRule
	tenantRules    map[string][]*PermissionRule
	placeholders   *PlaceholderResolver
	tenantIsolator *TenantIsolator
	defaultEffect  Effect
	auditLogger    AuditLogger
	regexCache     sync.Map // map[string]*regexp.Regexp
}

// PolicyEngineConfig configures the policy engine.
type PolicyEngineConfig struct {
	// DefaultEffect when no rules match (default: deny).
	DefaultEffect Effect `yaml:"default_effect" json:"default_effect"`

	// Rules are the permission rules to evaluate.
	Rules []*PermissionRule `yaml:"rules" json:"rules"`

	// TenantRules are per-tenant rule overrides.
	TenantRules map[string][]*PermissionRule `yaml:"tenant_rules" json:"tenant_rules"`
}

// DefaultPolicyEngineConfig returns sensible defaults.
func DefaultPolicyEngineConfig() PolicyEngineConfig {
	return PolicyEngineConfig{
		DefaultEffect: EffectDeny,
		Rules:         []*PermissionRule{},
		TenantRules:   make(map[string][]*PermissionRule),
	}
}

// PolicyEngineOption configures the PolicyEngine.
type PolicyEngineOption func(*PolicyEngine)

// WithPlaceholderResolver sets a custom placeholder resolver.
func WithPlaceholderResolver(resolver *PlaceholderResolver) PolicyEngineOption {
	return func(e *PolicyEngine) {
		e.placeholders = resolver
	}
}

// WithPolicyTenantIsolator sets a custom tenant isolator.
func WithPolicyTenantIsolator(isolator *TenantIsolator) PolicyEngineOption {
	return func(e *PolicyEngine) {
		e.tenantIsolator = isolator
	}
}

// WithPolicyAuditLogger sets a custom audit logger.
func WithPolicyAuditLogger(logger AuditLogger) PolicyEngineOption {
	return func(e *PolicyEngine) {
		e.auditLogger = logger
	}
}

// NewPolicyEngine creates a new policy engine.
func NewPolicyEngine(config PolicyEngineConfig, opts ...PolicyEngineOption) (*PolicyEngine, error) {
	if config.DefaultEffect == "" {
		config.DefaultEffect = EffectDeny
	}

	e := &PolicyEngine{
		rules:         config.Rules,
		tenantRules:   config.TenantRules,
		defaultEffect: config.DefaultEffect,
		placeholders:  NewPlaceholderResolver(),
		auditLogger:   &noopAuditLogger{},
	}

	// Apply options
	for _, opt := range opts {
		opt(e)
	}

	// Sort rules by priority (highest first)
	e.sortRules()

	// Compile patterns
	for _, rule := range e.rules {
		if err := e.compileRule(rule); err != nil {
			return nil, fmt.Errorf("compile rule %q: %w", rule.ID, err)
		}
	}
	for tenantID, rules := range e.tenantRules {
		for _, rule := range rules {
			if err := e.compileRule(rule); err != nil {
				return nil, fmt.Errorf("compile tenant %q rule %q: %w", tenantID, rule.ID, err)
			}
		}
	}

	return e, nil
}

// sortRules sorts rules by priority (highest first).
func (e *PolicyEngine) sortRules() {
	slices.SortFunc(e.rules, func(a, b *PermissionRule) int {
		return cmp.Compare(b.Priority, a.Priority)
	})

	for _, rules := range e.tenantRules {
		slices.SortFunc(rules, func(a, b *PermissionRule) int {
			return cmp.Compare(b.Priority, a.Priority)
		})
	}
}

// compileRule compiles the channel pattern regex.
func (e *PolicyEngine) compileRule(rule *PermissionRule) error {
	if rule.Match.ChannelPattern == "" {
		return nil
	}

	// Convert pattern to regex
	// {placeholder} -> (?P<placeholder>[^.]+)
	// * -> [^.]+
	pattern := rule.Match.ChannelPattern

	// Escape dots
	pattern = strings.ReplaceAll(pattern, ".", `\.`)

	// Convert placeholders to regex captures
	pattern = placeholderRegex.ReplaceAllStringFunc(pattern, func(match string) string {
		name := match[1 : len(match)-1]
		return `(?P<` + name + `>[^.]+)`
	})

	// Convert wildcards
	pattern = strings.ReplaceAll(pattern, "*", `[^.]+`)

	// Anchor
	pattern = "^" + pattern + "$"

	re, err := regexp.Compile(pattern)
	if err != nil {
		return fmt.Errorf("compile channel pattern %q: %w", rule.Match.ChannelPattern, err)
	}
	rule.compiledPattern = re
	return nil
}

// AuthzRequest represents an authorization request.
type AuthzRequest struct {
	// Claims from the JWT.
	Claims *Claims

	// Action being performed.
	Action Action

	// Channel being accessed.
	Channel string

	// Context for cancellation and values.
	Context context.Context
}

// AuthzResult contains the authorization decision.
type AuthzResult struct {
	// Allowed indicates whether the request is authorized.
	Allowed bool

	// MatchedRule is the rule that matched (if any).
	MatchedRule *PermissionRule

	// Reason explains the decision.
	Reason string

	// Captures contains placeholder values extracted from channel.
	Captures map[string]string

	// Duration is how long evaluation took.
	Duration time.Duration
}

// Authorize evaluates rules to determine if a request is allowed.
func (e *PolicyEngine) Authorize(req *AuthzRequest) *AuthzResult {
	start := time.Now()

	result := &AuthzResult{
		Captures: make(map[string]string),
	}

	if req.Context == nil {
		req.Context = context.Background()
	}

	// Check tenant isolation first if isolator is configured
	if e.tenantIsolator != nil && req.Claims != nil && req.Claims.TenantID != "" {
		accessResult := e.tenantIsolator.CheckChannelAccess(
			req.Context,
			req.Claims,
			req.Channel,
			AccessAction(req.Action),
		)
		if !accessResult.Allowed {
			result.Allowed = false
			result.Reason = "tenant isolation: " + accessResult.Reason
			result.Duration = time.Since(start)
			return result
		}
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	// Resolve channel with placeholders for matching
	resolvedChannel := req.Channel
	if req.Claims != nil {
		resolvedChannel = e.placeholders.Resolve(req.Channel, req.Claims)
	}

	// Get applicable rules (tenant-specific + global)
	rules := e.getApplicableRules(req.Claims)

	// Evaluate rules in priority order
	for _, rule := range rules {
		matched, captures := e.matchRule(rule, req, resolvedChannel)
		if !matched {
			continue
		}
		result.Allowed = rule.Effect == EffectAllow
		result.MatchedRule = rule
		result.Captures = captures
		result.Reason = "matched rule: " + rule.ID
		result.Duration = time.Since(start)
		return result
	}

	// No rule matched - use default effect
	result.Allowed = e.defaultEffect == EffectAllow
	result.Reason = fmt.Sprintf("no matching rule, default: %s", e.defaultEffect)
	result.Duration = time.Since(start)
	return result
}

// getApplicableRules returns rules for the tenant plus global rules.
func (e *PolicyEngine) getApplicableRules(claims *Claims) []*PermissionRule {
	if claims == nil || claims.TenantID == "" {
		return e.rules
	}

	// Tenant-specific rules take precedence
	tenantRules, ok := e.tenantRules[claims.TenantID]
	if !ok {
		return e.rules
	}

	// Combine tenant rules with global rules
	combined := make([]*PermissionRule, 0, len(tenantRules)+len(e.rules))
	combined = append(combined, tenantRules...)
	combined = append(combined, e.rules...)

	// Re-sort by priority
	slices.SortFunc(combined, func(a, b *PermissionRule) int {
		return cmp.Compare(b.Priority, a.Priority)
	})

	return combined
}

// matchRule checks if a rule matches the request.
func (e *PolicyEngine) matchRule(rule *PermissionRule, req *AuthzRequest, channel string) (matched bool, captures map[string]string) {
	captures = make(map[string]string)

	// Check action
	if !e.actionMatches(rule.Actions, req.Action) {
		return false, nil
	}

	// Check channel match
	if !e.channelMatches(rule, channel, captures, req.Claims) {
		return false, nil
	}

	// Check conditions
	if !e.conditionsMatch(rule.Conditions, req.Claims, captures) {
		return false, nil
	}

	return true, captures
}

// actionMatches checks if the action is in the rule's actions.
func (e *PolicyEngine) actionMatches(ruleActions []Action, action Action) bool {
	if len(ruleActions) == 0 {
		return true // Empty means all actions
	}

	return slices.Contains(ruleActions, action)
}

// channelMatches checks if the channel matches the rule.
func (e *PolicyEngine) channelMatches(rule *PermissionRule, channel string, captures map[string]string, claims *Claims) bool {
	// Exact match
	if rule.Match.ChannelExact != "" {
		resolved := rule.Match.ChannelExact
		if claims != nil {
			resolved = e.placeholders.Resolve(resolved, claims)
		}
		return channel == resolved
	}

	// Prefix match
	if rule.Match.ChannelPrefix != "" {
		resolved := rule.Match.ChannelPrefix
		if claims != nil {
			resolved = e.placeholders.Resolve(resolved, claims)
		}
		return strings.HasPrefix(channel, resolved)
	}

	// Pattern match
	if rule.compiledPattern != nil {
		matches := rule.compiledPattern.FindStringSubmatch(channel)
		if matches == nil {
			return false
		}

		// Extract captures
		for i, name := range rule.compiledPattern.SubexpNames() {
			if i > 0 && name != "" && i < len(matches) {
				captures[name] = matches[i]
			}
		}
		return true
	}

	// No match criteria = match all
	return true
}

// conditionsMatch evaluates all conditions (AND logic).
func (e *PolicyEngine) conditionsMatch(conditions []Condition, claims *Claims, captures map[string]string) bool {
	for _, cond := range conditions {
		matched := e.evaluateCondition(cond, claims, captures)
		if cond.Negate {
			matched = !matched
		}
		if !matched {
			return false
		}
	}
	return true
}

// evaluateCondition evaluates a single condition.
func (e *PolicyEngine) evaluateCondition(cond Condition, claims *Claims, captures map[string]string) bool {
	if claims == nil {
		return false
	}

	switch cond.Type {
	case ConditionTypeClaim:
		return e.evaluateClaimCondition(cond, claims)
	case ConditionTypeAttribute:
		return e.evaluateAttributeCondition(cond, claims)
	case ConditionTypeChannel:
		return e.evaluateChannelCondition(cond, captures)
	case ConditionTypeTime:
		// TODO: Time-based conditions not yet implemented.
		// When implemented, should support: time_of_day, day_of_week, date_range.
		// Returns false (fail-secure) for any time condition until implemented.
		return false
	default:
		return false
	}
}

// evaluateClaimCondition evaluates a claim condition.
func (e *PolicyEngine) evaluateClaimCondition(cond Condition, claims *Claims) bool {
	var fieldValue any

	switch cond.Field {
	case "roles":
		fieldValue = claims.Roles
	case "groups":
		fieldValue = claims.Groups
	case "scopes":
		fieldValue = claims.Scopes
	case "tenant_id":
		fieldValue = claims.TenantID
	case "sub", "subject":
		fieldValue = claims.Subject
	default:
		// Check attributes
		if after, ok := strings.CutPrefix(cond.Field, "attrs."); ok {
			attrName := after
			fieldValue = claims.GetAttribute(attrName)
		} else {
			return false
		}
	}

	return e.compareValues(fieldValue, cond.Op, cond.Value)
}

// evaluateAttributeCondition evaluates an attribute condition.
func (e *PolicyEngine) evaluateAttributeCondition(cond Condition, claims *Claims) bool {
	attrValue := claims.GetAttribute(cond.Field)
	return e.compareValues(attrValue, cond.Op, cond.Value)
}

// evaluateChannelCondition evaluates a channel condition.
func (e *PolicyEngine) evaluateChannelCondition(cond Condition, captures map[string]string) bool {
	captureValue, ok := captures[cond.Field]
	if !ok {
		return false
	}
	return e.compareValues(captureValue, cond.Op, cond.Value)
}

// compareValues compares values using the specified operator.
func (e *PolicyEngine) compareValues(fieldValue any, op Operator, condValue any) bool {
	switch op {
	case OpExists:
		return fieldValue != nil && fieldValue != ""

	case OpEquals:
		return fmt.Sprintf("%v", fieldValue) == fmt.Sprintf("%v", condValue)

	case OpNotEquals:
		return fmt.Sprintf("%v", fieldValue) != fmt.Sprintf("%v", condValue)

	case OpContains:
		// Check if slice contains value
		switch fv := fieldValue.(type) {
		case []string:
			condStr := fmt.Sprintf("%v", condValue)
			return slices.Contains(fv, condStr)
		case string:
			return strings.Contains(fv, fmt.Sprintf("%v", condValue))
		default:
			return false
		}

	case OpIn:
		// Check if field value is in condition values
		fieldStr := fmt.Sprintf("%v", fieldValue)
		switch cv := condValue.(type) {
		case []string:
			return slices.Contains(cv, fieldStr)
		case []any:
			for _, v := range cv {
				if fmt.Sprintf("%v", v) == fieldStr {
					return true
				}
			}
			return false
		default:
			return false
		}

	case OpMatches:
		fieldStr := fmt.Sprintf("%v", fieldValue)
		condStr := fmt.Sprintf("%v", condValue)
		var re *regexp.Regexp
		if cached, ok := e.regexCache.Load(condStr); ok {
			re, _ = cached.(*regexp.Regexp)
		} else {
			compiled, err := regexp.Compile(condStr)
			if err != nil {
				return false
			}
			re = compiled
			e.regexCache.Store(condStr, re)
		}
		return re.MatchString(fieldStr)

	case OpStartsWith:
		fieldStr := fmt.Sprintf("%v", fieldValue)
		condStr := fmt.Sprintf("%v", condValue)
		return strings.HasPrefix(fieldStr, condStr)

	case OpEndsWith:
		fieldStr := fmt.Sprintf("%v", fieldValue)
		condStr := fmt.Sprintf("%v", condValue)
		return strings.HasSuffix(fieldStr, condStr)

	default:
		return false
	}
}

// AddRule adds a rule to the engine.
func (e *PolicyEngine) AddRule(rule *PermissionRule) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.compileRule(rule); err != nil {
		return fmt.Errorf("add rule %q: %w", rule.ID, err)
	}
	e.rules = append(e.rules, rule)
	e.sortRules()
	return nil
}

// RemoveRule removes a rule by ID.
func (e *PolicyEngine) RemoveRule(id string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	for i, rule := range e.rules {
		if rule.ID == id {
			e.rules = append(e.rules[:i], e.rules[i+1:]...)
			return true
		}
	}
	return false
}

// GetRule returns a rule by ID.
func (e *PolicyEngine) GetRule(id string) *PermissionRule {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, rule := range e.rules {
		if rule.ID == id {
			return rule
		}
	}
	return nil
}

// ListRules returns all rules.
func (e *PolicyEngine) ListRules() []*PermissionRule {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]*PermissionRule, len(e.rules))
	copy(result, e.rules)
	return result
}

// CanSubscribe is a convenience method for subscribe authorization.
func (e *PolicyEngine) CanSubscribe(ctx context.Context, claims *Claims, channel string) bool {
	return e.Authorize(&AuthzRequest{
		Claims:  claims,
		Action:  RuleSubscribe,
		Channel: channel,
		Context: ctx,
	}).Allowed
}

// CanPublish is a convenience method for publish authorization.
func (e *PolicyEngine) CanPublish(ctx context.Context, claims *Claims, channel string) bool {
	return e.Authorize(&AuthzRequest{
		Claims:  claims,
		Action:  RulePublish,
		Channel: channel,
		Context: ctx,
	}).Allowed
}
