package runner

import (
	"context"
	"fmt"
)

// seedDefaultChannelRules authorizes all channels (subscribe + publish) for
// the suite's tenant. Channel authorization is provisioning-only: a tenant
// without rules is denied everything, so suites that exercise transport
// behavior (not authorization semantics) seed permissive rules at setup.
// Suites that test authorization set their own precise rules instead.
//
// The catch-all pattern is "*" — the gateway's rule matcher (MatchWildcard)
// treats a lone "*" as match-anything; "**" is routing-rule syntax and would
// NOT match here.
//
// A nil ProvClient (static-token mode, no provisioning) is a no-op: the
// target deployment's rules are whatever the operator provisioned.
func seedDefaultChannelRules(ctx context.Context, run *TestRun) error {
	if run.authResult == nil || run.authResult.ProvClient == nil {
		return nil
	}
	err := run.authResult.ProvClient.SetChannelRules(ctx, run.authResult.TenantID, map[string]any{
		"public":         []string{"*"},
		"default":        []string{"*"},
		"publish_public": []string{"*"},
	})
	if err != nil {
		return fmt.Errorf("set default channel rules: %w", err)
	}
	return nil
}
